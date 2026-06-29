"""
QQ platform adapter via OneBot v11 protocol.

Uses WebSocket for both receiving events and sending API calls
(NapCatQQ, go-cqhttp, Lagrange.OneBot, LLOneBot, or any compatible implementation).

Architecture:
    QQ Client  ←→  OneBot Implementation (NapCat/go-cqhttp/etc.)
                         ↓ WebSocket (events + API)
                    QQ Adapter (WS client)

Requires:
    pip install websockets
    A running OneBot-compatible QQ implementation with WebSocket enabled

Configuration in config.yaml:
    platforms:
      qq:
        enabled: true
        extra:
          # 正向模式（Hermes 连 LLBot）
          ws_host: "127.0.0.1"
          ws_port: 3001
          ws_path: "/"
          # 反向模式（LLBot 连 Hermes，推荐，get_file 无 30s 超时）
          reverse_mode: false
          reverse_host: "0.0.0.0"
          reverse_port: 6700
          access_token: ""
          show_qq_id: false         # 在 user_name 里附带 QQ 号，如 用户名(123456)
          group_whitelist: ""        # 群白名单（逗号分隔），仅响应这些群
          group_blacklist: ""        # 群黑名单（逗号分隔），忽略这些群（白名单优先）

Environment variables:
    QQ_ACCESS_TOKEN     - OneBot access_token
    QQ_WS_URL           - Full WebSocket URL (overrides ws_host/port/path)
    QQ_REVERSE_MODE     - Set to "true" for reverse WS mode
    QQ_ONEBOT_GROUP_WHITELIST  - Group whitelist (comma-separated group IDs)
    QQ_ONEBOT_GROUP_BLACKLIST  - Group blacklist (comma-separated group IDs)

    QQ_HOME_CHANNEL     - Default chat_id for sending (user_id or group_id)
"""

import asyncio
import json
import logging
import mimetypes
import os
import random
import re
import time
import tempfile
import urllib.request
import urllib.error
import urllib.parse
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Set

import websockets
from websockets.asyncio.client import ClientConnection

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_image_from_bytes,
)
from gateway.platforms.helpers import MessageDeduplicator

logger = logging.getLogger(__name__)

# Module-level WS client reference for external access (e.g. send_message tool)
_global_ws_client: Optional["_OneBotWSClient"] = None

def get_ws_client() -> Optional["_OneBotWSClient"]:
    """Get the global OneBot WS client instance, if connected."""
    return _global_ws_client


def check_qq_requirements() -> bool:
    """Check if OneBot QQ runtime dependencies are available."""
    try:
        import websockets
        return True
    except ImportError:
        return False

# 全局常量
MAX_MESSAGE_LENGTH = 4500
LIKE_EMOJI_IDS = [66, 76, 122, 124, 144, 147, 175, 180, 201, 282, 297]

# ── 简单的 LRU 缓存（用于防内存泄漏） ────────────────────────────────────────

class _SimpleLRU:
    """A lightweight LRU cache for managing temporary routing/delivery state."""
    def __init__(self, max_size: int = 1000):
        self.cache: OrderedDict[str, Any] = OrderedDict()
        self.max_size = max_size

    def get(self, key: str, default: Any = None) -> Any:
        if key in self.cache:
            self.cache.move_to_end(key) # 移动到末尾 (最近使用)
            return self.cache[key]
        return default

    def set(self, key: str, value: Any) -> None:
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.max_size:
            # 弹出最早插入的项 (last=False 即 FIFO 的第一个)
            self.cache.popitem(last=False)

    def pop(self, key: str, default: Any = None) -> Any:
        return self.cache.pop(key, default)

    def __contains__(self, key: str) -> bool:
        """支持直接使用 'key in cache' 的优雅语法"""
        return key in self.cache


# ── OneBot v11 message segment types ────────────────────────────────────────

def _text_segment(text: str) -> dict:
    return {"type": "text", "data": {"text": text}}

def _image_segment(uri: str) -> dict:
    if uri.startswith(("http://", "https://")):
        return {"type": "image", "data": {"file": uri}}
    # Strip existing file:// prefix if present
    if uri.startswith("file://"):
        uri = uri[7:]
    # Use file://localhost/path format — NapCat strips 'file://' (7 chars)
    # so this yields 'localhost/path' instead of the broken '//path' from
    # file:///path.  'localhost/' is harmless and the file resolves correctly.
    if not uri.startswith("/"):
        uri = "/" + uri
    return {"type": "image", "data": {"file": f"file://localhost{uri}"}}

def _reply_segment(message_id: str) -> dict:
    return {"type": "reply", "data": {"id": message_id}}

def _at_segment(qq_id: str) -> dict:
    return {"type": "at", "data": {"qq": qq_id}}

def _record_segment(uri: str) -> dict:
    if uri.startswith(("http://", "https://")):
        return {"type": "record", "data": {"file": uri}}
    if uri.startswith("file://"):
        uri = uri[7:]
    if not uri.startswith("/"):
        uri = "/" + uri
    return {"type": "record", "data": {"file": f"file://localhost{uri}"}}

def _file_segment(uri: str) -> dict:
    return {"type": "file", "data": {"file": uri}}

def _build_onebot_message(
    text: str,
    reply_to: Optional[str] = None,
    media_files: Optional[List[dict]] = None,
) -> List[dict]:
    """Build a OneBot v11 message array (outgoing)."""
    segments: List[dict] = []
    if reply_to:
        segments.append(_reply_segment(reply_to))
    if text.strip():
        segments.append(_text_segment(text))
    for mf in (media_files or []):
        path = mf.get("path", "")
        if not path:
            continue
        ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""
        if ext in ("png", "jpg", "jpeg", "gif", "webp"):
            segments.append(_image_segment(path))
        elif ext in ("ogg", "mp3", "wav", "amr", "silk"):
            segments.append(_record_segment(path))
        else:
            segments.append(_file_segment(path))
    return segments


# ── Incoming message parsing ────────────────────────────────────────────────

def _extract_text_from_segments(segments: List[dict]) -> str:
    parts = []
    for seg in segments:
        if seg.get("type") == "text":
            parts.append(seg.get("data", {}).get("text", ""))
    return "".join(parts)

def _build_onebot_text(raw_message: str, segments: List[dict]) -> str:
    text = _extract_text_from_segments(segments)
    if text.strip():
        return text.strip()
    if raw_message:
        cleaned = re.sub(r"\[CQ:[^\]]+\]", "", raw_message).strip()
        if cleaned:
            return cleaned
    # 识别媒体/文件类型，给个有意义的标签而不是 [非文本消息]
    for seg in segments:
        seg_type = seg.get("type", "")
        if seg_type == "image":
            return "[图片]"
        if seg_type == "file":
            fname = seg.get("data", {}).get("file", "")
            return f"[文件: {fname}]" if fname else "[文件]"
        if seg_type == "record":
            return "[语音]"
        if seg_type == "video":
            return "[视频]"
    return "[非文本消息]"

def _extract_at_qq(segments: List[dict], bot_self_id: str) -> Optional[str]:
    for seg in segments:
        if seg.get("type") == "at":
            qq = seg.get("data", {}).get("qq", "")
            if str(qq) == bot_self_id:
                return str(qq)
    return None


# ── Helper class ────────────────────────────────────────────────────────────

class _OneBotWSClient:
    """Send OneBot API calls over the same WebSocket connection."""
    
    def __init__(self):
        self._ws: Optional[ClientConnection] = None
        self._pending: Dict[str, asyncio.Future] = {}
        self._echo_counter: int = 0
        self._reverse_mode: bool = False
    
    def set_ws(self, ws: Optional[ClientConnection]):
        self._ws = ws
        if ws is None:
            # 当连接断开时，快速失败掉所有挂起的 API 请求，防止协程一直挂起到超时
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError("WebSocket disconnected"))
            self._pending.clear()
    
    async def call(self, action: str, params: dict) -> dict:
        if not self._ws:
            return {"status": "failed", "msg": "WebSocket not connected"}
        
        self._echo_counter += 1
        echo = f"hermes_{self._echo_counter}_{int(time.time() * 1000)}"
        
        payload = json.dumps({
            "action": action,
            "params": params,
            "echo": echo,
        })
        
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[echo] = future
        
        try:
            await self._ws.send(payload)
            async with asyncio.timeout(60.0):
                return await future
        except TimeoutError:
            # In reverse WS mode, don't close the connection — let LLBot manage it.
            # Only close in forward WS mode to trigger reconnection.
            if self._ws and not self._reverse_mode:
                try:
                    await self._ws.close()
                except Exception:
                    pass
            return {"status": "failed", "msg": "API call timeout"}
        except websockets.exceptions.ConnectionClosed as e:
            # 捕获 websockets 专属的断连异常，防止向外抛出导致崩溃
            return {"status": "failed", "msg": f"WS connection closed: {e}"}
        except ConnectionError as e:
            return {"status": "failed", "msg": str(e)}
        finally:
            self._pending.pop(echo, None)
    
    def handle_response(self, data: dict):
        """Called when a response frame is received from WebSocket."""
        echo = data.get("echo")
        if echo and echo in self._pending:
            future = self._pending.pop(echo)
            if not future.done():
                future.set_result(data)

    async def send_private_msg(self, user_id: str, message: list) -> dict:
        return await self.call("send_private_msg", {"user_id": int(user_id), "message": message})

    async def send_group_msg(self, group_id: str, message: list) -> dict:
        return await self.call("send_group_msg", {"group_id": int(group_id), "message": message})

    async def send_msg(self, message_type: str, target_id: str, message: list) -> dict:
        return await self.call("send_msg", {"message_type": message_type, "target_id": int(target_id), "message": message})

    async def get_group_info(self, group_id: str) -> dict:
        return await self.call("get_group_info", {"group_id": int(group_id)})

    async def get_file(self, file_id: str) -> dict:
        return await self.call("get_file", {"file_id": file_id})

    async def get_msg(self, message_id: str) -> dict:
        return await self.call("get_msg", {"message_id": int(message_id)})

    async def upload_group_file(self, group_id: str, file_path: str, file_name: str = "") -> dict:
        return await self.call("upload_group_file", {
            "group_id": int(group_id),
            "file": file_path,
            "name": file_name or os.path.basename(file_path),
        })

    async def upload_private_file(self, user_id: str, file_path: str, file_name: str = "") -> dict:
        return await self.call("upload_private_file", {
            "user_id": int(user_id),
            "file": file_path,
            "name": file_name or os.path.basename(file_path),
        })

    async def send_forward_msg(self, messages: list) -> dict:
        """Send a merged/forward message (合并转发)."""
        return await self.call("send_forward_msg", {"messages": messages})

    async def send_group_forward_msg(self, group_id: str, messages: list) -> dict:
        return await self.call("send_group_forward_msg", {"group_id": int(group_id), "messages": messages})

    async def send_private_forward_msg(self, user_id: str, messages: list) -> dict:
        return await self.call("send_private_forward_msg", {"user_id": int(user_id), "messages": messages})

    async def set_msg_emoji_like(self, message_id: str, emoji_id: int = 12) -> dict:
        """Send emoji reaction to a message. emoji_id 12 = 👍."""
        return await self.call("set_msg_emoji_like", {"message_id": int(message_id), "emoji_id": emoji_id, "set": True})

    async def friend_poke(self, user_id: str) -> dict:
        """Poke a friend (私聊搓一搓)."""
        return await self.call("friend_poke", {"user_id": int(user_id)})


class _OneBotWSServer:
    """WebSocket server for reverse WS mode — LLBot connects to us."""

    def __init__(self, ws_client: _OneBotWSClient, event_handler):
        self._ws_client = ws_client
        self._event_handler = event_handler
        self._server = None
        self._client_ws: Optional[ClientConnection] = None
        self._task: Optional[asyncio.Task] = None

    async def start(self, host: str, port: int, access_token: str = ""):
        self._access_token = access_token

        async def _handler(ws):
            # Auth check
            if self._access_token:
                token = ws.request_headers.get("Authorization", "").removeprefix("Bearer ")
                if not token:
                    try:
                        from urllib.parse import parse_qs, urlparse
                        qs = parse_qs(urlparse(ws.request.path).query)
                        token = (qs.get("access_token", [""])[0])
                    except Exception as e:
                        logger.debug("[qq] Reverse WS: Failed to parse token from query string: %s", e)
                if token != self._access_token:
                    await ws.close(1008, "auth failed")
                    return

            logger.info("[qq] Reverse WS client connected: %s", ws.remote_address)
            self._client_ws = ws
            self._ws_client.set_ws(ws)
            global _global_ws_client
            _global_ws_client = self._ws_client

            try:
                async for message in ws:
                    try:
                        data = json.loads(message)
                        post_type = data.get("post_type", "")
                        meta_type = data.get("meta_event_type", "")
                        logger.debug("[qq] Reverse WS recv: post_type=%s meta_type=%s action=%s",
                                     post_type, meta_type, data.get("action", ""))
                        if "echo" in data:
                            self._ws_client.handle_response(data)
                        elif post_type == "meta":
                            pass  # lifecycle/heartbeat — ignore
                        else:
                            await self._event_handler(data)
                    except json.JSONDecodeError:
                        logger.debug("[qq] Reverse WS: non-JSON message: %s", message[:200])
                    except Exception as e:
                        logger.error("[qq] Reverse WS event error: %s", e, exc_info=True)
            except websockets.ConnectionClosed as e:
                logger.warning("[qq] Reverse WS connection closed: code=%s reason=%s", e.code, e.reason)
            except Exception as e:
                logger.error("[qq] Reverse WS handler error: %s", e, exc_info=True)
            finally:
                logger.info("[qq] Reverse WS client disconnected")
                # 只在当前连接是自己的时候才清空，防止旧连接断开时把新连接覆盖掉
                if self._client_ws is ws:
                    self._client_ws = None
                    self._ws_client.set_ws(None)

        self._server = await websockets.serve(_handler, host, port)
        logger.info("[qq] Reverse WS server listening on ws://%s:%s", host, port)

    async def stop(self):
        if self._client_ws:
            try:
                await self._client_ws.close()
            except Exception:
                pass
        if self._server:
            self._server.close()
            await self._server.wait_closed()


# ── Main adapter ────────────────────────────────────────────────────────────


class QQAdapter(BasePlatformAdapter):
    """QQ platform adapter via OneBot v11 protocol."""

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("qqonebot"))
        extra = config.extra or {}

        # WebSocket config
        self._ws_host: str = extra.get("ws_host", "127.0.0.1")
        self._ws_port: int = int(extra.get("ws_port", 3001))
        self._ws_path: str = extra.get("ws_path", "/onebot/v11/ws")
        ws_url_override = os.getenv("QQ_WS_URL", "")
        if ws_url_override:
            self._ws_url = ws_url_override
        else:
            self._ws_url = f"ws://{self._ws_host}:{self._ws_port}{self._ws_path}"

        # Auth
        self._access_token: str = extra.get("access_token", "") or os.getenv("QQ_ACCESS_TOKEN", "")
        self._bot_self_id: str = extra.get("bot_self_id", "") or os.getenv("QQ_BOT_SELF_ID", "")
        self._http_api_url: str = extra.get("http_api_url", "") or os.getenv("QQ_HTTP_API_URL", "")


        # Internal state
        self._ws: Optional[ClientConnection] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_client = _OneBotWSClient()
        self._ws_server: Optional[_OneBotWSServer] = None
        self._reverse_mode: bool = extra.get("reverse_mode", False) or os.getenv("QQ_REVERSE_MODE", "").lower() == "true"
        self._ws_client._reverse_mode = self._reverse_mode
        self._reverse_host: str = extra.get("reverse_host", "0.0.0.0")
        self._reverse_port: int = int(extra.get("reverse_port", 6700))
        self._dedup = MessageDeduplicator(max_size=2000)
        self._delivery_info = _SimpleLRU(max_size=2000)
        self._background_tasks: Set[asyncio.Task] = set()
        self._reconnect_delay: float = 1.0
        self._max_reconnect_delay: float = 60.0

        # Caches
        self._group_name_cache = _SimpleLRU(max_size=500)
        self._nickname_cache = _SimpleLRU(max_size=5000)

        # Show QQ ID in user_name
        self._show_qq_id: bool = extra.get("show_qq_id", False)

        # Keyword trigger patterns for group chats (like Telegram mention_patterns)
        self._mention_patterns: List[re.Pattern] = self._compile_mention_patterns(extra)

        # User allowlist — if set, only these QQ IDs trigger reactions & processing
        raw_allow = extra.get("allowed_qq_ids", "") or os.getenv("QQ_ONEBOT_ALLOWED_USERS", "")
        self._allow_from: frozenset[str] = frozenset(
            p.strip() for p in str(raw_allow).split(",") if p.strip()
        )

        # Group whitelist — if set, only respond in these groups
        raw_group_wl = extra.get("group_whitelist", "") or os.getenv("QQ_ONEBOT_GROUP_WHITELIST", "")
        self._group_whitelist: frozenset[str] = frozenset(
            p.strip() for p in str(raw_group_wl).split(",") if p.strip()
        )

        # Group blacklist — if set, ignore these groups (whitelist takes priority)
        raw_group_bl = extra.get("group_blacklist", "") or os.getenv("QQ_ONEBOT_GROUP_BLACKLIST", "")
        self._group_blacklist: frozenset[str] = frozenset(
            p.strip() for p in str(raw_group_bl).split(",") if p.strip()
        )

        # Emoji reaction toggle (group emoji_like + private poke)
        self._enable_reaction: bool = extra.get("enable_reaction", True)

    def _compile_mention_patterns(self, extra: dict) -> List[re.Pattern]:
        """Compile regex wake-word patterns for group triggers."""
        patterns = extra.get("mention_patterns")
        if patterns is None:
            raw = os.getenv("QQ_MENTION_PATTERNS", "").strip()
            if raw:
                patterns = [p.strip() for p in raw.split(",") if p.strip()]
            else:
                return []
        if isinstance(patterns, str):
            patterns = [patterns]
        if not isinstance(patterns, list):
            logger.warning("[qq] mention_patterns must be a list or string; got %s", type(patterns).__name__)
            return []
        compiled = []
        for p in patterns:
            if not isinstance(p, str) or not p:
                continue
            try:
                compiled.append(re.compile(p, re.IGNORECASE))
            except re.error as e:
                logger.warning("[qq] Invalid mention_pattern %r: %s", p, e)
        return compiled

    def _text_matches_keywords(self, text: str) -> bool:
        """Check if text matches any configured keyword pattern."""
        if not self._mention_patterns or not text:
            return False
        for pattern in self._mention_patterns:
            if pattern.search(text):
                return True
        return False

    def _is_user_allowed(self, user_id: str) -> bool:
        """Check whether a QQ user is in the allowlist. Empty list = allow all."""
        if not self._allow_from:
            return True
        return user_id in self._allow_from

    def _is_group_allowed(self, group_id: str) -> bool:
        """Check whether a QQ group passes whitelist/blacklist filter.

        Priority: whitelist > blacklist > allow all.
        - If whitelist is set, only whitelisted groups are allowed.
        - If only blacklist is set, all groups except blacklisted ones are allowed.
        - If neither is set, all groups are allowed.
        """
        if self._group_whitelist:
            return group_id in self._group_whitelist
        if self._group_blacklist:
            return group_id not in self._group_blacklist
        return True

    # ── Connection lifecycle ────────────────────────────────────────────

    async def connect(self, is_reconnect: bool = False) -> bool:
        if self._reverse_mode:
            # 反向 WS 模式：起 server 等 LLBot 连上来
            self._ws_server = _OneBotWSServer(self._ws_client, self._handle_ws_event)
            await self._ws_server.start(
                self._reverse_host, self._reverse_port, self._access_token
            )
            self._mark_connected()
            logger.info("[qq] Reverse WS mode: waiting for LLBot on port %s", self._reverse_port)
        else:
            # 正向 WS 模式：连 LLBot
            self._ws_task = asyncio.create_task(self._ws_loop())
            self._mark_connected()
            logger.info("[qq] Connecting to OneBot WebSocket at %s", self._ws_url)
        if self._http_api_url:
            logger.info("[qq] HTTP API enabled at %s", self._http_api_url)
        return True

    async def _http_call(self, action: str, params: dict) -> dict:
        """Call OneBot API via HTTP — independent of WS, won't block anything."""
        url = f"{self._http_api_url}/{action}"
        payload = json.dumps(params).encode()
        headers = {"Content-Type": "application/json"}
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"
        req = urllib.request.Request(url, data=payload, headers=headers)
        try:
            resp = await asyncio.to_thread(urllib.request.urlopen, req, timeout=30)
            return json.loads(resp.read().decode())
        except Exception as e:
            return {"status": "failed", "msg": str(e)}

    async def disconnect(self):
        if self._ws_server:
            await self._ws_server.stop()
            self._ws_server = None
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
            self._ws_task = None
        if self._ws:
            await self._ws.close()
            self._ws = None
        self._ws_client.set_ws(None)
        
        # 优雅地取消所有后台任务并等待其妥善清理
        for task in list(self._background_tasks):
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

    # ── WebSocket loop ──────────────────────────────────────────────────

    async def _ws_loop(self):
        """Main WebSocket connection loop with auto-reconnect."""
        while True:
            try:
                headers = {}
                if self._access_token:
                    headers["Authorization"] = f"Bearer {self._access_token}"
                
                async with websockets.connect(
                    self._ws_url,
                    additional_headers=headers,
                    ping_interval=30,
                    ping_timeout=10,
                ) as ws:
                    self._ws = ws
                    self._ws_client.set_ws(ws)
                    global _global_ws_client
                    _global_ws_client = self._ws_client
                    self._reconnect_delay = 1.0
                    logger.info("[qq] WebSocket connected to %s", self._ws_url)
                    
                    async for message in ws:
                        try:
                            data = json.loads(message)
                            # Check if it's a response frame (has echo)
                            if "echo" in data:
                                self._ws_client.handle_response(data)
                            else:
                                await self._handle_ws_event(data)
                        except json.JSONDecodeError:
                            logger.debug("[qq] Invalid JSON from WebSocket")
                        except Exception as e:
                            logger.error("[qq] Error handling WS event: %s", e)
                            
            except asyncio.CancelledError:
                logger.info("[qq] WebSocket loop cancelled")
                break
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning("[qq] WebSocket connection closed: %s", e)
            except ConnectionRefusedError:
                logger.warning("[qq] WebSocket connection refused at %s", self._ws_url)
            except Exception as e:
                logger.error("[qq] WebSocket error: %s", e)
            
            self._ws = None
            self._ws_client.set_ws(None)
            
            # Reconnect with exponential backoff
            logger.info("[qq] Reconnecting in %.1fs...", self._reconnect_delay)
            try:
                await asyncio.sleep(self._reconnect_delay)
            except asyncio.CancelledError:
                break
            self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)

    # ── Event handling ──────────────────────────────────────────────────

    async def _handle_ws_event(self, data: dict):
        """Handle incoming WebSocket event from OneBot."""
        post_type = data.get("post_type", "")

        # Update bot self_id if present
        bot_self_id = str(data.get("self_id", ""))
        if bot_self_id:
            self._bot_self_id = bot_self_id

        if post_type == "message":
            # 后台处理，不阻塞 WS 接收循环
            task = asyncio.create_task(self._handle_message_event(data))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

    async def _handle_message_event(self, data: dict):
        message_type = data.get("message_type", "")
        user_id = str(data.get("user_id", ""))
        raw_message = data.get("raw_message", "")
        message_id = str(data.get("message_id", ""))
        group_id = str(data.get("group_id", "")) if message_type == "group" else ""
        sender = data.get("sender", {})
        nickname = sender.get("nickname", "") or user_id

        # Robust segment parsing
        message_segments = data.get("message", [])
        if not isinstance(message_segments, list):
            message_segments = []

        # Deduplication
        dedup_key = f"qq:{message_id}" if message_id else f"qq:{user_id}:{raw_message[:100]}"
        if self._dedup.is_duplicate(dedup_key):
            return

        # Self-messages filter
        if user_id == self._bot_self_id:
            return

        # User allowlist filter (skip all processing for non-allowed users)
        if not self._is_user_allowed(user_id):
            return

        # Group whitelist/blacklist filter
        if message_type == "group" and group_id and not self._is_group_allowed(group_id):
            return

        # @Mention detection in groups (early filter)
        if message_type == "group":
            has_at = _extract_at_qq(message_segments, self._bot_self_id)
            text_before_at = _build_onebot_text(raw_message, message_segments)
            has_keyword = self._text_matches_keywords(text_before_at)
            if not has_at and not has_keyword:
                return


        if nickname:
            self._nickname_cache.set(user_id, nickname)

        # Optionally append QQ ID to nickname for LLM visibility
        display_name = f"{nickname}({user_id})" if self._show_qq_id and nickname != user_id else nickname

        # Resolve Chat Info
        if message_type == "group":
            chat_id = f"qq_group_{group_id}"
            chat_name = self._group_name_cache.get(group_id, f"QQ群{group_id}")
            if group_id not in self._group_name_cache:
                task = asyncio.create_task(self._resolve_group_name(group_id))
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
        else:
            chat_id = f"qq_{user_id}"
            chat_name = nickname or f"QQ用户{user_id}"

        source = self.build_source(
            chat_id=chat_id, chat_name=chat_name, user_id=user_id, user_name=display_name,
            chat_type="group" if message_type == "group" else "dm",
            thread_id=group_id if message_type == "group" else None,
        )

        # 1. 提取基础文本
        text = _build_onebot_text(raw_message, message_segments)

        # 2. 解析主消息的媒体附件
        media_urls, media_types = await self._parse_media_segments(message_segments)

        # 3. 解析回复的消息体（包含回复消息体的附件下载）
        reply_to_id, reply_to_text, rep_urls, rep_types = await self._parse_reply_message(message_segments)
        if reply_to_id:
            media_urls.extend(rep_urls)
            media_types.extend(rep_types)

        # 4. 如果包含纯文本文件附件，将其内容注入到文本中
        text = self._inject_file_content(text, media_urls, media_types)

        # 5. 生成兜底文本
        if not text.strip() and media_urls:
            if any(t.startswith("image/") for t in media_types): text = "[图片]"
            elif any(t.startswith("audio/") for t in media_types): text = "[语音]"
            else: text = "[文件]"

        if not text.strip():
            return

        # 6. 推断消息类型
        msg_type = MessageType.TEXT
        if media_types:
            if any(t.startswith(("application/", "text/")) for t in media_types): msg_type = MessageType.DOCUMENT
            elif any(t.startswith("audio/") for t in media_types): msg_type = MessageType.AUDIO
            elif any(t.startswith("image/") for t in media_types): msg_type = MessageType.PHOTO

        # 7. 记录发送上下文并分发事件
        self._delivery_info.set(chat_id, {
            "message_type": message_type,
            "target_id": group_id if message_type == "group" else user_id,
            "reply_to": message_id,
            "group_id": group_id,
            "user_id": user_id,
        })

        event = MessageEvent(
            message_type=msg_type, text=text, source=source, raw_message=data,
            message_id=message_id or None, media_urls=media_urls, media_types=media_types,
            reply_to_message_id=reply_to_id, reply_to_text=reply_to_text,
        )

        if message_id and self._enable_reaction:
            try:
                if message_type == "group":
                    # 群聊：表情回应
                    emoji_id = random.choice(LIKE_EMOJI_IDS)
                    task = asyncio.create_task(self._emoji_like_bg(message_id, emoji_id))
                else:
                    # 私聊：搓一搓
                    task = asyncio.create_task(self._friend_poke_bg(user_id))
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
            except Exception as e:
                logger.debug("[qq] Failed to trigger background reaction task: %s", e)

        await self.handle_message(event)

    async def _parse_media_segments(self, message_segments: List[dict]) -> tuple[List[str], List[str]]:
        """提取消息段中的所有媒体附件，并下载到本地"""
        media_urls = []
        media_types = []
        
        for seg in message_segments:
            seg_type = seg.get("type")
            data_block = seg.get("data", {})
            url = data_block.get("url", "") or data_block.get("file", "")

            local_path = None
            default_mime = "application/octet-stream"

            if seg_type == "image" and url:
                local_path = await self._download_media(url, "image")
                default_mime = "image/jpeg"
            elif seg_type == "record" and url:
                local_path = await self._download_media(url, "audio")
                default_mime = "audio/ogg"
            elif seg_type == "file":
                logger.warning("[qq] FILE: path=%s url=%s file_id=%s",
                    data_block.get("path",""), data_block.get("url",""), data_block.get("file_id",""))
                local_path = data_block.get("path", "")
                if local_path and os.path.isfile(local_path):
                    pass  # 直接用
                elif url and url.startswith("file://"):
                    local_path = urllib.parse.urlparse(url).path
                elif url and url.startswith(("http://", "https://")):
                    local_path = await self._download_media(url, "file")
                # NEED ATTENTION
                else:
                    # 兜底：用 file_id 或 file 字段调 get_file
                    local_path = None
                    for candidate in [data_block.get("file_id", ""), data_block.get("file", "")]:
                        if not candidate:
                            continue
                        try:
                            if self._http_api_url:
                                file_info = await self._http_call("get_file", {"file_id": candidate})
                            else:
                                file_info = await self._ws_client.get_file(candidate)
                            if file_info.get("status") == "ok":
                                fp = file_info.get("data", {}).get("file", "")
                                if fp and os.path.isfile(fp):
                                    local_path = fp
                                    break
                        except Exception as e:
                            logger.debug("[qq] get_file failed candidate=%s: %s", candidate, e)
                if local_path and (not os.path.isabs(local_path) or not os.path.isfile(local_path)):
                    local_path = None

            if local_path:
                media_urls.append(local_path)
                mime = mimetypes.guess_type(local_path)[0] or default_mime
                media_types.append(mime)
                
        return media_urls, media_types

    async def _parse_reply_message(self, message_segments: List[dict]) -> tuple[Optional[str], Optional[str], List[str], List[str]]:
        """解析回复的消息体，返回 (回复ID, 回复文本, 回复包含的媒体路径, 回复包含的媒体类型)"""
        reply_seg = next((s for s in message_segments if s.get("type") == "reply"), None)
        replied_msg_id = str(reply_seg.get("data", {}).get("id", "")) if reply_seg else ""
        
        if not replied_msg_id:
            return None, None, [], []

        reply_to_text = None
        media_urls = []
        media_types = []

        try:
            replied = await self._ws_client.get_msg(replied_msg_id)
            if replied.get("status") == "ok":
                orig = replied.get("data", {})
                orig_segments = orig.get("message", [])
                
                if isinstance(orig_segments, list):
                    reply_to_text = _build_onebot_text(orig.get("raw_message", ""), orig_segments)
                    # 复用刚才分离出去的 _parse_media_segments，大幅消减代码冗余！
                    media_urls, media_types = await self._parse_media_segments(orig_segments)
        except Exception as e:
            logger.debug("[qq] Failed to fetch replied message %s: %s", replied_msg_id, e)

        return replied_msg_id, reply_to_text, media_urls, media_types

    def _inject_file_content(self, text: str, media_urls: List[str], media_types: List[str]) -> str:
        """读取文本类附件内容并拼接到正文中"""
        _TEXT_EXTS = {".txt", ".md", ".csv", ".json", ".xml", ".yaml", ".yml",
                      ".log", ".py", ".js", ".ts", ".html", ".css", ".ini", ".cfg"}
        _MAX_TEXT_INJECT = 100 * 1024
        
        injected_text = text
        for i, (murl, mt) in enumerate(zip(media_urls, media_types)):
            ext = os.path.splitext(murl)[1].lower()
            if ext in _TEXT_EXTS or mt.startswith("text/"):
                try:
                    fsize = os.path.getsize(murl)
                    if fsize > _MAX_TEXT_INJECT:
                        logger.debug("[qq] Skipping text injection for %s (%d bytes > %d)", murl, fsize, _MAX_TEXT_INJECT)
                        continue
                    with open(murl, "r", errors="replace") as _f:
                        file_content = _f.read()
                    display_name = os.path.basename(murl)
                    injection = f"[Content of {display_name}]:\n{file_content}"
                    injected_text = f"{injection}\n\n{injected_text}" if injected_text.strip() else injection
                    logger.debug("[qq] Injected text content from: %s", murl)
                except Exception as e:
                    logger.debug("[qq] Failed to read document text %s: %s", murl, e)
        return injected_text

    async def _emoji_like_bg(self, message_id: str, emoji_id: int):
        """Send emoji reaction in background without blocking WS event loop."""
        try:
            result = await self._ws_client.set_msg_emoji_like(message_id, emoji_id=emoji_id)
            if result.get("status") != "ok":
                logger.warning("[qq] emoji_like failed for msg %s: %s", message_id, result)
        except Exception as e:
            logger.warning("[qq] emoji_like exception for msg %s: %s", message_id, e)

    async def _friend_poke_bg(self, user_id: str):
        """Poke a friend in background via WS."""
        try:
            result = await self._ws_client.friend_poke(user_id)
            if result.get("status") != "ok":
                logger.warning("[qq] friend_poke failed for user %s: %s", user_id, result)
        except Exception as e:
            logger.warning("[qq] friend_poke exception for user %s: %s", user_id, e)

    async def _download_media(self, url: str, media_type: str = "image") -> Optional[str]:
        """Download media from URL safely, without blocking event loop."""
        
        # Determine extension from URL using urlparse to avoid query parameter issues
        ext = ".jpg"
        url_path = urllib.parse.urlparse(url).path.lower()
        if url_path.endswith(".png"): ext = ".png"
        elif url_path.endswith(".gif"): ext = ".gif"
        elif url_path.endswith(".webp"): ext = ".webp"
        elif url_path.endswith(".ogg"): ext = ".ogg"
        elif url_path.endswith(".mp3"): ext = ".mp3"
        elif url_path.endswith(".wav"): ext = ".wav"
        elif media_type == "audio": ext = ".ogg"
        elif media_type == "file": ext = ".bin"

        MAX_BYTES = 10 * 1024 * 1024 if media_type == "image" else 50 * 1024 * 1024
        
        # 根据媒体类型动态分配超时时间
        timeout_seconds = 15 if media_type == "image" else 60

        def _sync_download_and_save() -> Optional[str]:
            """Synchronous block for network and file I/O."""
            try:
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                    data = resp.read(MAX_BYTES + 1)
                    if len(data) > MAX_BYTES:
                        logger.warning("[qq] Media exceeds limit, aborting download.")
                        return None
                        
                # 磁盘写入操作也放在这里统一同步完成，防止主线程阻塞
                if media_type == "image":
                    return cache_image_from_bytes(data, ext=ext)
                else:
                    fd, path = tempfile.mkstemp(suffix=ext)
                    with os.fdopen(fd, "wb") as f:
                        f.write(data)
                    return path
            except Exception as e:
                logger.warning("[qq] I/O Error during download/save: %s", e)
                return None

        # Python 3.9+ 引入的 asyncio.to_thread 更简洁地代替了 run_in_executor
        try:
            return await asyncio.to_thread(_sync_download_and_save)
        except Exception as e:
            logger.warning("[qq] Failed to download media from %s: %s", url, e)
            return None

    # ── Delivery Helper ─────────────────────────────────────────────────
    
    def _get_delivery_target(self, chat_id: str) -> tuple[str, str]:
        """解析并返回通用的 (message_type, target_id) 用于发送回退"""
        delivery = self._delivery_info.get(chat_id, {})
        msg_type = delivery.get("message_type", "")
        target_id = delivery.get("target_id", "")

        if not target_id:
            if chat_id.startswith("qq_group_"):
                target_id = chat_id.removeprefix("qq_group_")
                msg_type = "group"
            elif chat_id.startswith("qq_"):
                target_id = chat_id.removeprefix("qq_")
                msg_type = "private"
            elif chat_id.lstrip("-").isdigit():
                # Bare numeric chat_id — check delivery_info with common prefixes
                target_id = chat_id
                for prefix in (f"qq_group_{chat_id}", f"qq_{chat_id}"):
                    info = self._delivery_info.get(prefix, {})
                    if info.get("target_id"):
                        msg_type = info["message_type"]
                        target_id = info["target_id"]
                        break
                if not msg_type:
                    # Default to private for bare numeric IDs (direct tool calls)
                    msg_type = "private"

        return msg_type or "private", str(target_id)

    # ── Forward message helpers ─────────────────────────────────────────

    @staticmethod
    def _split_text(text: str, max_len: int = 1500) -> List[str]:
        """Split long text into chunks at paragraph/sentence boundaries."""
        if len(text) <= max_len:
            return [text]
        
        chunks: List[str] = []
        remaining = text
        
        while len(remaining) > max_len:
            # Try to split at paragraph boundary first
            split_pos = remaining.rfind("\n\n", 0, max_len)
            
            # Then try single newline
            if split_pos < max_len * 0.3:
                split_pos = remaining.rfind("\n", 0, max_len)
            
            # Then try CJK sentence boundary
            if split_pos < max_len * 0.3:
                for pattern in ("。", "！", "？", "；", ".", "!", "?"):
                    pos = remaining.rfind(pattern, 0, max_len)
                    if pos > 0:
                        split_pos = pos + 1
                        break
            
            # Hard cut at space or any position
            if split_pos < max_len * 0.3:
                split_pos = remaining.rfind(" ", 0, max_len)
                if split_pos < max_len * 0.3:
                    split_pos = max_len
            
            chunks.append(remaining[:split_pos].strip())
            remaining = remaining[split_pos:].strip()
        
        if remaining:
            chunks.append(remaining)
        
        return [c for c in chunks if c]

    async def _send_forward(self, chat_id: str, content: str, reply_to: Optional[str] = None) -> Optional[SendResult]:
        """Send long content as a merged forward message. Returns None if forward API fails."""
        delivery = self._delivery_info.get(chat_id, {})
        message_type = delivery.get("message_type", "")
        target_id = delivery.get("target_id", "")
        
        if not target_id:
            message_type, target_id = self._get_delivery_target(chat_id)
        
        # Forward message: single node with full content (already collapsed, no need to split)
        nodes = []
        if reply_to:
            nodes.append({"type": "node", "data": {"id": int(reply_to)}})
        
        bot_id = self._bot_self_id or "0"
        nodes.append({
            "type": "node",
            "data": {
                "uin": int(bot_id),
                "name": "芙芙",
                "content": [{"type": "text", "data": {"text": content}}],
            },
        })
        
        try:
            if message_type == "group":
                result = await self._ws_client.send_group_forward_msg(target_id, nodes)
            else:
                result = await self._ws_client.send_private_forward_msg(target_id, nodes)
            
            if result.get("status") == "ok" or result.get("retcode", -1) == 0:
                return SendResult(success=True, message_id=str(result.get("data", {}).get("message_id", "")))
            # Forward API failed, fall through to return None
            logger.debug("[qq] Forward send failed (retcode=%s), falling back to split send", result.get("retcode"))
            return None
        except Exception as e:
            logger.debug("[qq] Forward send exception: %s, falling back to split send", e)
            return None

    # ── Send methods ────────────────────────────────────────────────────

    async def send(self, chat_id: str, content: str = "", **kwargs) -> SendResult:
        if not self._ws_client:
            return SendResult(success=False, error="OneBot HTTP client not initialized")

        reply_to = kwargs.get("reply_to")

        # For long content: try forward message first (group), then fall back to split
        if len(content) > MAX_MESSAGE_LENGTH:
            delivery = self._delivery_info.get(chat_id, {})
            message_type = delivery.get("message_type", "")
            if not message_type:
                message_type, _ = self._get_delivery_target(chat_id)

            # Try forward message for group chats
            if message_type == "group":
                forward_result = await self._send_forward(chat_id, content, reply_to)
                if forward_result:
                    return forward_result

            # Fall back: send as split messages
            chunks = self._split_text(content)
            last_result = None
            for chunk in chunks:
                segments = _build_onebot_message(chunk, reply_to=reply_to)
                if not segments:
                    continue
                try:
                    msg_type, target_id = self._get_delivery_target(chat_id)
                    if msg_type == "group":
                        last_result = await self._ws_client.send_group_msg(target_id, segments)
                    elif msg_type == "private":
                        last_result = await self._ws_client.send_private_msg(target_id, segments)
                    else:
                        last_result = await self._ws_client.send_msg("private", target_id, segments)
                except Exception as e:
                    logger.error("[qq] Split send failed: %s", e)
                    return SendResult(success=False, error=str(e))
                # Only the first chunk carries reply_to
                reply_to = None

            if last_result and last_result.get("retcode", 0) == 0:
                return SendResult(success=True, message_id=str(last_result.get("data", {}).get("message_id", "")))
            error_msg = (last_result or {}).get("wording", (last_result or {}).get("msg", "unknown error"))
            return SendResult(success=False, error=f"OneBot API error: {error_msg}")

        # Normal path: message within limits
        segments = _build_onebot_message(content, reply_to=reply_to)

        # 避免给 QQ 发送完全为空的消息数组
        if not segments:
            return SendResult(success=False, error="Message is empty after parsing")

        try:
            message_type, target_id = self._get_delivery_target(chat_id)

            if message_type == "group":
                result = await self._ws_client.send_group_msg(target_id, segments)
            elif message_type == "private":
                result = await self._ws_client.send_private_msg(target_id, segments)
            else:
                result = await self._ws_client.send_msg("private", target_id, segments)

            if result.get("status") == "failed" or result.get("retcode", 0) != 0:
                error_msg = result.get("wording") or result.get("message") or result.get("msg") or "unknown error"
                return SendResult(success=False, error=f"OneBot API error: {error_msg}")

            return SendResult(success=True, message_id=str(result.get("data", {}).get("message_id", "")))

        except Exception as e:
            logger.error("[qq] Send failed: %s", e)
            return SendResult(success=False, error=str(e))

    async def send_image(self, chat_id: str, image_url: str, caption: str = "") -> SendResult:
        if not self._ws_client:
            return SendResult(success=False, error="OneBot HTTP client not initialized")
        
        segments = []
        if caption and caption.strip():
            segments.append(_text_segment(caption))
        segments.append(_image_segment(image_url))

        try:
            message_type, target_id = self._get_delivery_target(chat_id)

            if message_type == "group":
                result = await self._ws_client.send_group_msg(target_id, segments)
            else:
                result = await self._ws_client.send_private_msg(target_id, segments)
            return SendResult(success=result.get("retcode", 0) == 0)
        except Exception as e:
            return SendResult(success=False, error=str(e))

    async def send_image_file(self, chat_id: str, image_path: str, caption: str = "", **kwargs) -> SendResult:
        """Send a local image file natively via QQ.

        Uses HTTP API when available (LLBot reverse WS never returns echo
        for image messages, causing 60s timeouts).  Falls back to WS otherwise.
        """
        # Send caption text first if present
        if caption and caption.strip():
            try:
                await self.send(chat_id, caption)
            except Exception:
                pass

        segments = [_image_segment(image_path)]
        message_type, target_id = self._get_delivery_target(chat_id)

        # Prefer HTTP API for images — reverse WS never returns echo for them
        if self._http_api_url:
            try:
                params = {
                    "group_id" if message_type == "group" else "user_id": int(target_id),
                    "message": segments,
                }
                result = await self._http_call("send_group_msg" if message_type == "group" else "send_private_msg", params)
                retcode = result.get("retcode", -1)
                # retcode 0 = success; retcode 200 = invoke timeout but image was actually sent
                if retcode in (0, 200):
                    if retcode == 200:
                        logger.info("[QQ] Image send got retcode=200 (invoke timeout) — image was likely delivered")
                    return SendResult(success=True)
                error_msg = result.get("message", "") or result.get("wording", "") or result.get("msg", "") or f"retcode={retcode}"
                return SendResult(success=False, error=error_msg)
            except Exception as e:
                logger.warning("[QQ] HTTP image send failed, falling back to WS: %s", e)

        # Fallback: WS path (may timeout for images, but try anyway)
        if not self._ws_client:
            return SendResult(success=False, error="OneBot client not initialized")

        try:
            if message_type == "group":
                result = await self._ws_client.send_group_msg(target_id, segments)
            else:
                result = await self._ws_client.send_private_msg(target_id, segments)
            # Check for API-level failure (no retcode in timeout/error responses)
            if result.get("status") == "failed":
                msg = result.get("msg", "unknown error")
                # WS image sends often timeout even though the image was delivered
                if "timeout" in msg.lower():
                    logger.info("[QQ] WS image send timed out — image was likely delivered anyway")
                    return SendResult(success=True)
                return SendResult(success=False, error=msg)
            retcode = result.get("retcode", 0)
            if retcode == 0:
                return SendResult(success=True)
            error_msg = result.get("msg", "") or result.get("wording", "") or f"retcode={retcode}"
            return SendResult(success=False, error=error_msg)
        except Exception as e:
            return SendResult(success=False, error=str(e))

    async def send_typing(self, chat_id: str, **kwargs):
        pass

    async def get_chat_info(self, chat_id: str) -> dict:
        if chat_id.startswith("qq_group_"):
            group_id = chat_id.removeprefix("qq_group_")
            name = self._group_name_cache.get(group_id, f"QQ群{group_id}")
            return {"name": name, "type": "group", "chat_id": chat_id}
        elif chat_id.startswith("qq_"):
            uid = chat_id.removeprefix("qq_")
            name = self._nickname_cache.get(uid, f"QQ用户{uid}")
            return {"name": name, "type": "private", "chat_id": chat_id}
        return {"name": chat_id, "type": "unknown", "chat_id": chat_id}

    async def send_voice(self, chat_id: str, audio_path: str, caption: Optional[str] = None, reply_to: Optional[str] = None, **kwargs) -> SendResult:
        if not self._ws_client:
            return SendResult(success=False, error="client not ready")
        
        segments = [_record_segment(audio_path)]
        try:
            message_type, target_id = self._get_delivery_target(chat_id)
            
            if message_type == "group":
                await self._ws_client.send_group_msg(target_id, segments)
            else:
                await self._ws_client.send_private_msg(target_id, segments)
            return SendResult(success=True)
        except Exception as e:
            return SendResult(success=False, error=str(e))

    async def send_document(self, chat_id: str, file_path: str = "", caption: str = "", **kwargs) -> SendResult:
        path = file_path or kwargs.get("path", "")
        if not self._ws_client:
            return SendResult(success=False, error="client not ready")
        
        try:
            message_type, target_id = self._get_delivery_target(chat_id)

            if caption and caption.strip():
                if message_type == "group":
                    await self._ws_client.send_group_msg(target_id, [_text_segment(caption)])
                else:
                    await self._ws_client.send_private_msg(target_id, [_text_segment(caption)])
                    
            if message_type == "group":
                result = await self._ws_client.upload_group_file(target_id, path)
            else:
                result = await self._ws_client.upload_private_file(target_id, path)
                
            if result.get("status") == "failed":
                return SendResult(success=False, error=result.get("wording", "upload failed"))
            return SendResult(success=True)
        except Exception as e:
            return SendResult(success=False, error=str(e))

    async def _resolve_group_name(self, group_id: str):
        if not self._ws_client or group_id in self._group_name_cache:
            return
        try:
            result = await self._ws_client.get_group_info(group_id)
            data = result.get("data", {})
            name = data.get("group_name", "")
            if name:
                self._group_name_cache.set(group_id, name)
                logger.debug("[qq] Resolved group %s → %s", group_id, name)
        except Exception as e:
            logger.debug("[qq] Failed to resolve group %s: %s", group_id, e)
