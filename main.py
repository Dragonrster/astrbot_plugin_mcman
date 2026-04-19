import asyncio
import json
import struct
import os
import re
import time
from collections import defaultdict, deque

from astrbot.api.event import filter, AstrMessageEvent

try:
    from astrbot.api.event import MessageChain
except ImportError:
    from astrbot.core.message.message_event_result import MessageChain
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.api.star import StarTools
from astrbot.api import AstrBotConfig  # 配置管理


class AsyncRcon:  # 异步RCON类
    def __init__(self, host: str, port: int, password: str):
        self.host = host
        self.port = port
        self.password = password
        self.reader = None
        self.writer = None

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        await self._send_packet(0, 3, self.password)  # 登录
        req_id, _, _ = await self._recv_packet()
        if req_id == -1:
            raise PermissionError("RCON 登录失败，请检查密码配置。")

    async def send_cmd(self, command: str, *, extra_recv_deadline_sec: float = 1.45) -> str:
        await self._send_packet(1, 2, command)
        req_id, _, body = await self._recv_packet()
        chunks = [body] if body else []

        # 部分服务端会把输出拆成多包；Spark health 等可能晚于首包才返回正文。
        deadline = time.monotonic() + max(0.05, extra_recv_deadline_sec)
        idle = 0.12
        while time.monotonic() < deadline:
            try:
                left = deadline - time.monotonic()
                if left <= 0:
                    break
                next_req_id, _, next_body = await asyncio.wait_for(
                    self._recv_packet(), timeout=min(idle, left)
                )
            except asyncio.TimeoutError:
                if "".join(chunks).strip():
                    break
                continue

            if next_req_id != req_id:
                continue
            if next_body:
                chunks.append(next_body)

        return "".join(chunks)

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    async def _send_packet(self, req_id: int, ptype: int, payload: str):
        data = struct.pack("<ii", req_id, ptype) + payload.encode() + b"\x00\x00"
        length = struct.pack("<i", len(data))
        self.writer.write(length + data)
        await self.writer.drain()

    async def _recv_packet(self):
        length_bytes = await self.reader.readexactly(4)
        length = struct.unpack("<i", length_bytes)[0]
        data = await self.reader.readexactly(length)
        req_id, ptype = struct.unpack("<ii", data[:8])
        body = data[8:].rstrip(b"\x00").decode(errors="ignore")
        return req_id, ptype, body


def strip_mc_color(text: str) -> str:
    return re.sub(r"§.", "", text)


# 服务端 latest.log 里常见玩家聊天形态：<Steve> hi 或 [Not Secure] <Steve> hi
_MC_LOG_CHAT_RE = re.compile(
    r"(?:\[Not Secure\]\s*)?<(?P<player>[^>]+)>\s*(?P<body>.*?)\s*$",
    re.IGNORECASE,
)


# /mcrun 默认禁止的首个命令词（不含命名空间前缀，比较时会去掉 xxx:）
_MCRUN_DEFAULT_BLOCKED_FIRST = frozenset({"stop", "restart", "op", "deop", "reload"})


async def rcon_command(
    host: str,
    port: int,
    password: str,
    command: str,
    *,
    extra_recv_deadline_sec: float = 0.45,
) -> str:  # 执行rcon命令
    """统一执行任意 RCON 命令"""
    rcon = AsyncRcon(host, port, password)
    await rcon.connect()
    try:
        return await rcon.send_cmd(
            command, extra_recv_deadline_sec=extra_recv_deadline_sec
        )
    finally:
        await rcon.close()


@register(
    "astrbot_plugin_mcman", "卡带酱", "一个基于RCON协议的MC服务器管理器插件", "1.1.0"
)
class MyPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.whitelist_command = self.config.get("whitelist_command", "whitelist")
        self.admin_qqs = set(self.config.get("admin_qqs", []))
        self.rcon_host = self.config.get("rcon_host")
        self.rcon_port = self.config.get("rcon_port")
        self.rcon_password = self.config.get("rcon_password")
        # 申请白名单功能
        self.enable_apply_whitelist = self.config.get("enable_apply_whitelist", False)
        self.plugin_data_dir = StarTools.get_data_dir("astrbot_plugin_mcman")
        self.apply_file = os.path.join(self.plugin_data_dir, "apply_whitelist.json")
        self.apply_data = self._load_apply_data()
        self.mcrun_blocked_first = set(_MCRUN_DEFAULT_BLOCKED_FIRST)
        extra = self.config.get("mcrun_blocked_extra", [])
        if isinstance(extra, list):
            self.mcrun_blocked_first.update(
                str(x).strip().lower() for x in extra if str(x).strip()
            )
        # 游戏内「.mcsay xxx」→ QQ：依赖可读 latest.log + 主动发消息（RCON 收不到聊天）
        self.mc_chat_log_to_qq_enabled = bool(
            self.config.get("mc_chat_log_to_qq_enabled", False)
        )
        self.mc_chat_log_path = str(self.config.get("mc_chat_log_path", "") or "").strip()
        self.mc_chat_trigger_prefix = str(
            self.config.get("mc_chat_trigger_prefix", ".mcsay ") or ".mcsay "
        )
        self.mc_chat_qq_platform = str(
            self.config.get("mc_chat_qq_platform", "aiocqhttp") or "aiocqhttp"
        ).strip()
        self.mc_chat_qq_group_id = str(
            self.config.get("mc_chat_qq_group_id", "") or ""
        ).strip()
        self.mc_chat_unified_msg_origin = str(
            self.config.get("mc_chat_unified_msg_origin", "") or ""
        ).strip()
        self.mc_chat_log_tail_from_end = bool(
            self.config.get("mc_chat_log_tail_from_end", True)
        )
        self.mc_chat_target_file = os.path.join(
            str(self.plugin_data_dir), "mc_chat_qq_target.json"
        )
        self._load_mc_chat_target_from_file()
        self._mc_chat_task = None
        self._mc_chat_stop = None
        self._mc_chat_file_pos = [0]
        self._mc_chat_first_boot = [True]
        self._mc_chat_rate: defaultdict[str, deque[float]] = defaultdict(deque)

    def _load_mc_chat_target_from_file(self):
        if not os.path.isfile(self.mc_chat_target_file):
            return
        try:
            with open(self.mc_chat_target_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            u = data.get("unified_msg_origin")
            if isinstance(u, str) and u.strip():
                self.mc_chat_unified_msg_origin = u.strip()
        except Exception as e:
            logger.warning(f"读取 mc_chat_qq_target.json 失败: {e}")

    def _save_mc_chat_target_to_file(self, umo: str):
        os.makedirs(str(self.plugin_data_dir), exist_ok=True)
        with open(self.mc_chat_target_file, "w", encoding="utf-8") as f:
            json.dump({"unified_msg_origin": umo}, f, ensure_ascii=False, indent=2)

    def _load_apply_data(self):
        if os.path.exists(self.apply_file):
            with open(self.apply_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save_apply_data(self):
        with open(self.apply_file, "w", encoding="utf-8") as f:
            json.dump(self.apply_data, f, ensure_ascii=False, indent=2)

    async def initialize(self):
        logger.info("mcman plugin by kdj")
        await self._start_mc_chat_watcher_if_configured()

    async def _stop_mc_chat_watcher_if_running(self):
        if self._mc_chat_stop is not None:
            self._mc_chat_stop.set()
        t = self._mc_chat_task
        self._mc_chat_task = None
        if t and not t.done():
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

    async def _start_mc_chat_watcher_if_configured(self):
        await self._stop_mc_chat_watcher_if_running()
        self._mc_chat_stop = asyncio.Event()
        self._mc_chat_file_pos = [0]
        self._mc_chat_first_boot = [True]
        if not self.mc_chat_log_to_qq_enabled:
            return
        if not self.mc_chat_log_path:
            logger.warning("mc_chat_log_to_qq_enabled 已开启但未配置 mc_chat_log_path，跳过日志监听")
            return
        if not (self.mc_chat_unified_msg_origin or self.mc_chat_qq_group_id):
            logger.warning(
                "mc_chat_log_to_qq_enabled 已开启但未配置 QQ 目标（mc_chat_unified_msg_origin 或 mc_chat_qq_group_id），跳过"
            )
            return
        self._mc_chat_task = asyncio.create_task(
            self._mc_chat_log_tail_loop(), name="mcman_mc_chat_tail"
        )

    def _mc_chat_read_new_lines_sync(self) -> list[str]:
        path = self.mc_chat_log_path
        if not path or not os.path.isfile(path):
            return []
        lines_out: list[str] = []
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            size = os.path.getsize(path)
            if size < self._mc_chat_file_pos[0]:
                self._mc_chat_file_pos[0] = 0
            if (
                self._mc_chat_file_pos[0] == 0
                and self._mc_chat_first_boot[0]
                and self.mc_chat_log_tail_from_end
            ):
                f.seek(0, os.SEEK_END)
                self._mc_chat_file_pos[0] = f.tell()
                self._mc_chat_first_boot[0] = False
                return lines_out
            f.seek(self._mc_chat_file_pos[0])
            chunk = f.read()
            self._mc_chat_file_pos[0] = f.tell()
        return chunk.splitlines()

    def _parse_mc_chat_log_line(self, line: str) -> tuple[str, str] | None:
        line = re.sub(r"\x1b\[[0-9;]*m", "", line).strip()
        if "<" not in line:
            return None
        m = _MC_LOG_CHAT_RE.search(line)
        if not m:
            return None
        player = m.group("player").strip()
        body = m.group("body").strip()
        prefix = self.mc_chat_trigger_prefix
        if not body.startswith(prefix):
            return None
        payload = body[len(prefix) :].strip()
        if not payload:
            return None
        return player, payload

    def _mc_chat_rate_ok(self, player: str) -> bool:
        now = time.monotonic()
        dq = self._mc_chat_rate[player]
        while dq and now - dq[0] > 60.0:
            dq.popleft()
        if len(dq) >= 12:
            return False
        dq.append(now)
        return True

    async def _mc_chat_send_to_qq(self, player: str, text: str):
        body = f"[MC] <{player}> {text}"
        chain = MessageChain().message(body)
        try:
            if self.mc_chat_unified_msg_origin:
                await self.context.send_message(self.mc_chat_unified_msg_origin, chain)
            elif self.mc_chat_qq_group_id:
                await StarTools.send_message_by_id(
                    "GroupMessage",
                    self.mc_chat_qq_group_id,
                    chain,
                    self.mc_chat_qq_platform,
                )
        except Exception as e:
            logger.error(f"MC 游戏聊天转发到 QQ 失败: {e}")

    async def _mc_chat_log_tail_loop(self):
        try:
            while self._mc_chat_stop and not self._mc_chat_stop.is_set():
                await asyncio.sleep(0.85)
                try:
                    lines = await asyncio.to_thread(self._mc_chat_read_new_lines_sync)
                except Exception as e:
                    logger.error(f"读取 MC 日志失败: {e}")
                    continue
                for line in lines:
                    parsed = self._parse_mc_chat_log_line(line)
                    if not parsed:
                        continue
                    player, payload = parsed
                    if not self._mc_chat_rate_ok(player):
                        continue
                    await self._mc_chat_send_to_qq(player, payload)
        except asyncio.CancelledError:
            raise

    def is_admin(self, qqid: str) -> bool:
        return qqid in self.admin_qqs

    def _event_message_str(self, event: AstrMessageEvent) -> str:
        raw = getattr(event, "message_str", None)
        if (raw is None or not str(raw).strip()) and hasattr(event, "get_message_str"):
            try:
                raw = event.get_message_str()
            except Exception:
                raw = ""
        return (str(raw) if raw is not None else "").strip()

    def _tail_after_command_names(self, event: AstrMessageEvent, *names: str) -> str:
        """从整段纯文本里截取「命令名 + 空格」之后的全部内容，避免多词只吃到第一个参数。"""
        raw = self._event_message_str(event)
        if not names:
            return ""
        alt = "|".join(re.escape(n) for n in names)
        m = re.match(rf"^/?(?:{alt})\s+(?P<payload>.+)$", raw, re.IGNORECASE | re.DOTALL)
        if m:
            return m.group("payload").strip()
        return ""

    def _parse_mcrun_payload(self, event: AstrMessageEvent) -> str:
        return self._tail_after_command_names(event, "mcrun", "mcexec")

    @staticmethod
    def _mcrun_first_token_lower(console_cmd: str) -> str:
        parts = console_cmd.strip().split(maxsplit=1)
        if not parts:
            return ""
        t = parts[0].lower()
        if ":" in t:
            t = t.split(":")[-1]
        return t

    def _mcrun_blocked_reason(self, console_cmd: str) -> str | None:
        if not console_cmd.strip():
            return "请在 /mcrun 后输入要发送到服务端的命令。"
        first = self._mcrun_first_token_lower(console_cmd)
        if first in self.mcrun_blocked_first:
            return (
                f"出于安全考虑，已禁止以 `{first}` 开头的控制台命令。"
                "可在插件配置 `mcrun_blocked_extra` 中追加更多首词；"
                "默认拦截：stop、restart、op、deop、reload。"
            )
        return None

    async def _build_empty_response_hint(self, command: str) -> str:
        """空响应时，针对关键命令给出可验证的反馈。"""
        low_cmd = command.lower().strip()
        parts = low_cmd.split()
        if not parts:
            return "(空响应) 命令可能已执行成功，但服务器未返回文本。"

        if parts[0] == "ban" and len(parts) >= 2:
            target = parts[1]
            banlist_resp = await rcon_command(
                self.rcon_host,
                self.rcon_port,
                self.rcon_password,
                "banlist players",
            )
            banlist_text = strip_mc_color(banlist_resp).lower()
            if target in banlist_text:
                return f"(空响应) 已通过 banlist 校验：玩家 `{target}` 当前在黑名单中。"
            return (
                f"(空响应) 未在 banlist 中检索到 `{target}`，"
                "请确认玩家名大小写或查看服务端日志。"
            )

        if parts[0] == "pardon" and len(parts) >= 2:
            target = parts[1]
            banlist_resp = await rcon_command(
                self.rcon_host,
                self.rcon_port,
                self.rcon_password,
                "banlist players",
            )
            banlist_text = strip_mc_color(banlist_resp).lower()
            if target not in banlist_text:
                return f"(空响应) 已通过 banlist 校验：玩家 `{target}` 不在黑名单中。"
            return (
                f"(空响应) banlist 里仍能看到 `{target}`，"
                "请确认是否有同名记录或稍后重试。"
            )

        if low_cmd.startswith("spark "):
            return (
                "(空响应) Spark 的 `health` 等命令常把报告发到服务端控制台或游戏内管理员聊天，"
                "RCON 不一定能收到正文。请直接看服务器控制台；轻量数据可试 `/mctps` 或游戏内执行 `spark health`。"
            )

        return (
            "(空响应) 命令可能已执行成功，但服务器未返回文本。"
            "可在服务端执行 `gamerule sendCommandFeedback true` 观察反馈。"
        )

    async def execute_and_reply(
        self,
        event: AstrMessageEvent,
        command: str,
        desc: str,
        *,
        rcon_extra_recv_deadline_sec: float | None = None,
    ):
        """通用执行 + 回复逻辑"""
        user_name = event.get_sender_name()
        sender_qq = str(event.get_sender_id())
        named = f"{user_name}({sender_qq})"

        try:
            kw = {}
            if rcon_extra_recv_deadline_sec is not None:
                kw["extra_recv_deadline_sec"] = rcon_extra_recv_deadline_sec
            resp = await rcon_command(
                self.rcon_host, self.rcon_port, self.rcon_password, command, **kw
            )
            cresp = strip_mc_color(resp).strip()
            if not cresp:
                cresp = await self._build_empty_response_hint(command)
            logger.info(f"RCON 执行结果: {resp}")
            yield event.plain_result(
                f"已尝试执行 `{command}` ({desc})\n\n服务器返回：\n{cresp}"
            )
        except Exception as e:
            logger.error(f"RCON 执行失败: {e}")
            yield event.plain_result(f"操作失败：{e}")

    @filter.command("mcmanhelp", desc="MC 管理插件帮助", alias={"mchelp", "mch"})
    async def mcmanhelp(self, event: AstrMessageEvent):
        want = "开启" if self.enable_apply_whitelist else "未开启"
        text = "\n".join(
            [
                "MC 管理插件帮助",
                "带「管」的指令需在插件配置 admin_qqs 中。",
                "",
                "白名单 / 封禁",
                "  [管] /mcwl <add|remove|list|...> [MC名]  白名单（mcwhitelist）",
                "  [管] /mcban <MC名> [原因]  封禁",
                "  [管] /mcunban <MC名>  解封（mcpardon）",
                "  /mcbanlist  封禁列表（mcbl）",
                "  [管] /mckick <MC名> [原因]  踢人（mck）",
                "  [管] /mctempban <MC名> <时长> [原因]  临时封禁（mctb）",
                "",
                "在线与信息",
                "  /mclist  在线玩家（mcl）",
                "  /mcplugins  插件列表",
                "  [管] /mctps  TPS",
                "  [管] /mcsparkhealth  Spark 健康（mcspark、mcsh）",
                "  [管] /mcping [@a]  ping，默认 @a（mcp）",
                "",
                "Paper / 实体",
                "  [管] /mcentitylist [选择器] [世界]  默认 * world（mcel）",
                "",
                "聊天",
                "  /mcsay <整段内容>  minecraft:tellraw，支持空格（mcs）",
                "  [管] /mcbroadcast <整段内容>  广播，支持空格（mcb、mcbc）",
                "",
                "其他",
                "  [管] /mckill <MC名>",
                "  [管] /mcauthunregister <MC名>  AuthMe 注销（mcauthunreg）",
                "  [管] /mcrun <任意控制台命令>  RCON 透传（mcexec；有安全拦截）",
                "  [管] /mcbindchat  绑定当前 QQ 会话为「游戏内→QQ」转发目标（需开日志监听）",
                f"  /wantwl <MC名>  申请白名单（当前{want}）",
                "",
                "游戏内→QQ：开启 mc_chat_log_to_qq_enabled 并配置 mc_chat_log_path；",
                "服里聊天形如 `<玩家> .mcsay 你好` 会按日志解析后推到 QQ（非 RCON）。",
                "",
                "本帮助：/mcmanhelp（mchelp、mch）",
            ]
        )
        yield event.plain_result(text)

    @filter.command("mcwl", desc="MC 白名单管理", alias={"mcwhitelist"})
    async def mcwl(self, event: AstrMessageEvent, o: str, mcname: str = ""):
        if not self.is_admin(str(event.get_sender_id())):
            yield event.plain_result("抱歉，你没有权限执行此操作。")
            return
        command = f"{self.whitelist_command} {o} {mcname}".strip()
        async for msg in self.execute_and_reply(event, command, "白名单管理"):
            yield msg

    @filter.command("mcban", desc="MC 黑名单添加")
    async def mcban(self, event: AstrMessageEvent, mcname: str = "", reason: str = ""):
        if not self.is_admin(str(event.get_sender_id())):
            yield event.plain_result("抱歉，你没有权限执行此操作。")
            return
        if not mcname:
            yield event.plain_result("请输入要封禁的玩家名，例如：/mcban Steve [原因]")
            return
        command = f"ban {mcname} {reason}".strip()
        async for msg in self.execute_and_reply(event, command, "黑名单添加"):
            yield msg

    @filter.command("mcunban", desc="MC 黑名单移除", alias={"mcpardon"})
    async def mcunban(self, event: AstrMessageEvent, mcname: str = ""):
        if not self.is_admin(str(event.get_sender_id())):
            yield event.plain_result("抱歉，你没有权限执行此操作。")
            return
        command = f"unban {mcname}".strip()
        async for msg in self.execute_and_reply(event, command, "黑名单移除"):
            yield msg

    @filter.command("mcbanlist", desc="MC 黑名单查看", alias={"mcbl"})
    async def mcbl(self, event: AstrMessageEvent):
        async for msg in self.execute_and_reply(
            event, "banlist players", "查看黑名单"
        ):
            yield msg

    @filter.command("mclist", desc="MC 查看在线玩家", alias={"mcl"})
    async def mclist(self, event: AstrMessageEvent):
        async for msg in self.execute_and_reply(event, "list", "查看在线玩家"):
            yield msg

    @filter.command("mckick", desc="MC 踢出指定玩家", alias={"mck"})
    async def mckick(self, event: AstrMessageEvent, mcname: str = "", reason: str = ""):
        if not self.is_admin(str(event.get_sender_id())):
            yield event.plain_result("抱歉，你没有权限执行此操作。")
            return
        command = f"kick {mcname} {reason}".strip()
        async for msg in self.execute_and_reply(event, command, "踢出玩家"):
            yield msg

    @filter.command("mctempban", desc="MC 临时黑名单", alias={"mctb"})
    async def mctempban(
        self,
        event: AstrMessageEvent,
        mcname: str = "",
        time: str = "",
        reason: str = "",
    ):
        if not self.is_admin(str(event.get_sender_id())):
            yield event.plain_result("抱歉，你没有权限执行此操作。")
            return
        command = f"tempban {mcname} {time} {reason}".strip()
        async for msg in self.execute_and_reply(event, command, "临时封禁"):
            yield msg

    @filter.command("mcsay", desc="MC 说话", alias={"mcs"})
    async def mcsay(self, event: AstrMessageEvent):
        user_name = event.get_sender_name()
        sender_qq = str(event.get_sender_id())
        named = f"{user_name}({sender_qq})"

        text = self._tail_after_command_names(event, "mcsay", "mcs")
        if not text:
            yield event.plain_result("请输入信息，例如：/mcsay 你好 世界")
            return

        message = [
            {"text": f"(QQ消息) ", "color": "aqua"},
            {"text": f"<{named}>", "color": "green", "underlined": True},
            {"text": " 说: ", "color": "white"},
            {"text": text, "color": "yellow"},
        ]
        command = f"minecraft:tellraw @a {json.dumps(message, ensure_ascii=False)}"
        async for msg in self.execute_and_reply(event, command, "玩家发言"):
            yield msg

    @filter.command("mcbroadcast", desc="MC 广播消息", alias={"mcb", "mcbc"})
    async def mcbroadcast(self, event: AstrMessageEvent):
        user_name = event.get_sender_name()
        sender_qq = str(event.get_sender_id())
        named = f"{user_name}({sender_qq})"
        if not self.is_admin(str(event.get_sender_id())):
            yield event.plain_result("抱歉，你没有权限执行此操作。")
            return
        text = self._tail_after_command_names(event, "mcbroadcast", "mcb", "mcbc")
        if not text:
            yield event.plain_result("请输入广播内容，例如：/mcbroadcast 维护通知 今晚")
            return

        message = [
            {"text": f"<管理员广播消息>", "color": "green", "underlined": True},
            {"text": " ", "color": "white", "underlined": False},
            {"text": text, "color": "yellow", "underlined": False},
        ]
        command = f"minecraft:tellraw @a {json.dumps(message, ensure_ascii=False)}"
        async for msg in self.execute_and_reply(event, command, "广播消息"):
            yield msg

    @filter.command("wantwl", desc="申请MC白名单")
    async def wantwl(self, event: AstrMessageEvent, mcname: str = ""):
        if not self.enable_apply_whitelist:
            yield event.plain_result("抱歉，白名单申请功能未开启。")
            return
        if not mcname:
            yield event.plain_result("请输入要绑定的MC用户名。")
            return

        qqid = str(event.get_sender_id())
        if qqid in self.apply_data:
            yield event.plain_result(
                f"你已经绑定过MC账号 `{self.apply_data[qqid]}`，不能重复申请。"
            )
            return

        # 调用RCON执行
        command = f"{self.whitelist_command} add {mcname}"
        try:
            resp = await rcon_command(
                self.rcon_host, self.rcon_port, self.rcon_password, command
            )
            self.apply_data[qqid] = mcname
            self._save_apply_data()
            yield event.plain_result(
                f"成功为你绑定MC账号 `{mcname}` 并加入白名单！\n服务器返回：{strip_mc_color(resp)}"
            )
        except Exception as e:
            yield event.plain_result(f"申请失败：{e}")

    @filter.command("mckill", desc="MC kill人")
    async def mckill(self, event: AstrMessageEvent, mcname: str = ""):
        if not self.is_admin(str(event.get_sender_id())):
            yield event.plain_result("抱歉，你没有权限执行此操作。")
            return
        command = f"kill {mcname}".strip()
        async for msg in self.execute_and_reply(event, command, "kill"):
            yield msg

    @filter.command("mcplugins", desc="MC 插件列表")
    async def mcplugins(
        self,
        event: AstrMessageEvent,
    ):
        command = f"plugins".strip()
        async for msg in self.execute_and_reply(event, command, "插件列表"):
            yield msg

    @filter.command("mcentitylist", desc="Paper 实体列表", alias={"mcel"})
    async def mcentitylist(
        self,
        event: AstrMessageEvent,
        selector: str = "*",
        world: str = "world",
    ):
        if not self.is_admin(str(event.get_sender_id())):
            yield event.plain_result("抱歉，你没有权限执行此操作。")
            return
        command = f"paper entity list {selector} {world}".strip()
        async for msg in self.execute_and_reply(event, command, "Paper 实体列表"):
            yield msg

    @filter.command("mcping", desc="MC ping", alias={"mcp"})
    async def mcping(self, event: AstrMessageEvent, target: str = "@a"):
        if not self.is_admin(str(event.get_sender_id())):
            yield event.plain_result("抱歉，你没有权限执行此操作。")
            return
        command = f"ping {target}".strip()
        async for msg in self.execute_and_reply(event, command, "ping"):
            yield msg

    @filter.command("mcsparkhealth", desc="Spark 健康报告", alias={"mcspark", "mcsh"})
    async def mcsparkhealth(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())):
            yield event.plain_result("抱歉，你没有权限执行此操作。")
            return
        async for msg in self.execute_and_reply(
            event,
            "spark health",
            "Spark 健康",
            rcon_extra_recv_deadline_sec=8.0,
        ):
            yield msg

    @filter.command("mctps", desc="MC 查看 TPS")
    async def mctps(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())):
            yield event.plain_result("抱歉，你没有权限执行此操作。")
            return
        async for msg in self.execute_and_reply(event, "tps", "TPS"):
            yield msg

    @filter.command("mcauthunregister", desc="AuthMe 注销玩家", alias={"mcauthunreg"})
    async def mcauthunregister(self, event: AstrMessageEvent, mcname: str = ""):
        if not self.is_admin(str(event.get_sender_id())):
            yield event.plain_result("抱歉，你没有权限执行此操作。")
            return
        if not mcname:
            yield event.plain_result(
                "请输入要注销的 MC 用户名，例如：/mcauthunregister Steve"
            )
            return
        command = f"authme unregister {mcname}".strip()
        async for msg in self.execute_and_reply(event, command, "AuthMe 注销"):
            yield msg

    @filter.command("mcrun", desc="RCON 透传控制台命令", alias={"mcexec"})
    async def mcrun(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())):
            yield event.plain_result("抱歉，你没有权限执行此操作。")
            return
        payload = self._parse_mcrun_payload(event)
        if not payload:
            yield event.plain_result(
                "用法：/mcrun <任意控制台命令>\n"
                "示例：/mcrun save-all\n"
                "说明：从整段消息里截取命令，支持空格与参数；"
                "首个词在拦截列表中则拒绝（默认：stop、restart、op、deop、reload）。"
            )
            return
        blocked = self._mcrun_blocked_reason(payload)
        if blocked:
            yield event.plain_result(blocked)
            return
        extra_deadline = (
            8.0 if payload.lower().lstrip().startswith("spark ") else None
        )
        async for msg in self.execute_and_reply(
            event,
            payload,
            "RCON 透传",
            rcon_extra_recv_deadline_sec=extra_deadline,
        ):
            yield msg

    @filter.command("mcbindchat", desc="绑定当前会话为 MC 游戏聊天转发目标（管理员）")
    async def mcbindchat(self, event: AstrMessageEvent):
        if not self.is_admin(str(event.get_sender_id())):
            yield event.plain_result("抱歉，你没有权限执行此操作。")
            return
        umo = getattr(event, "unified_msg_origin", "") or ""
        if not str(umo).strip():
            yield event.plain_result(
                "无法获取当前会话 unified_msg_origin，请改用配置项 mc_chat_unified_msg_origin 或 mc_chat_qq_group_id。"
            )
            return
        self.mc_chat_unified_msg_origin = str(umo).strip()
        self._save_mc_chat_target_to_file(self.mc_chat_unified_msg_origin)
        await self._start_mc_chat_watcher_if_configured()
        yield event.plain_result(
            "已写入转发目标（mc_chat_qq_target.json）。\n"
            "请确认：1) 已开启 mc_chat_log_to_qq_enabled；2) mc_chat_log_path 指向该服 latest.log（与 AstrBot 能读到的路径一致）。\n"
            "游戏内发：`<触发前缀>内容>`，默认触发前缀为 `.mcsay `（见 mc_chat_trigger_prefix）。"
        )

    async def terminate(self):
        logger.info("mcman plugin stopped")
        await self._stop_mc_chat_watcher_if_running()
