#!/usr/bin/env python3
"""遥测 UDP 代理 + 操作网页。

功能要点：
- 按客户端 IP:端口建立会话，空闲超时（默认 5 秒）后过期；
- 建会话时把该会话固定到当前目标接收器（A/B），会话期间不随切换改变；
- 每个会话使用一条独立的、已 connect 的上游 UDP 套接字（独立本地端口），
  因此回包天然只能属于一个会话：只把该会话对应接收器的回包送回该客户端；
- 会话过期即关闭其上游端口，且端口进入冷却期不再分配，
  所以旧会话迟到的回包不可能落到同 IP:端口重建的新会话里；
- 报文与回包原样透传，不做任何修改；
- 当前目标保存在 SQLite，重启后保留；会话全在内存，重启即清空；
- 切换目标时先落库再生效，保存失败则保留原目标；
- Flask 网页可切换目标、发送试传报文、查看会话与报文日志（实际去向/回包）；
- 冷却期端口池试算：首页发起 30 秒观察，用同一段真实端口占用记录分别按
  现用/拟定冷却计算所需最少端口数与首次峰值时刻；只记录不改在线配置，
  缺开场记录、变更超 4096 次、建连失败或途中重启时本轮只显示作废原因。
"""

import asyncio
import json
import os
import socket
import sqlite3
import threading
import time
import uuid

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

# ---------------- 配置 ----------------
UDP_LISTEN_HOST = os.environ.get("UDP_LISTEN_HOST", "0.0.0.0")
UDP_LISTEN_PORT = int(os.environ.get("UDP_LISTEN_PORT", "9000"))
HTTP_HOST = os.environ.get("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8080"))
DB_PATH = os.environ.get(
    "DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxy.db")
)
SESSION_IDLE_TIMEOUT = float(os.environ.get("SESSION_IDLE_TIMEOUT", "5"))
UPSTREAM_PORT_MIN = int(os.environ.get("UPSTREAM_PORT_MIN", "20000"))
UPSTREAM_PORT_MAX = int(os.environ.get("UPSTREAM_PORT_MAX", "29999"))
# 上游端口关闭后的冷却时间：冷却期内不再分配该端口，
# 保证旧会话迟到的回包不会被复用同端口的新会话收到。应大于接收器最大回包延迟。
UPSTREAM_PORT_COOLDOWN = float(os.environ.get("UPSTREAM_PORT_COOLDOWN", "30"))
LOG_KEEP = int(os.environ.get("LOG_KEEP", "1000"))

# 冷却期端口池试算（只观察、不改在线配置）
OBSERVATION_SECONDS = 30          # 观察窗时长
PROPOSED_COOLDOWN_MAX = 120.0     # 拟定冷却上限（秒）
OBSERVATION_MAX_CHANGES = 4096    # 单轮占用变更次数上限

RECEIVERS = {
    "A": (
        os.environ.get("RECEIVER_A_HOST", "127.0.0.1"),
        int(os.environ.get("RECEIVER_A_PORT", "9001")),
    ),
    "B": (
        os.environ.get("RECEIVER_B_HOST", "127.0.0.1"),
        int(os.environ.get("RECEIVER_B_PORT", "9002")),
    ),
}
DEFAULT_TARGET = os.environ.get("DEFAULT_TARGET", "A")


def fmt_addr(addr):
    return f"{addr[0]}:{addr[1]}"


def preview(data: bytes) -> str:
    """报文内容展示：可打印文本原样显示，否则用 hex。"""
    try:
        text = data.decode("utf-8")
        if all(ch.isprintable() or ch in "\r\n\t" for ch in text):
            return text if len(text) <= 200 else text[:200] + "…"
    except UnicodeDecodeError:
        pass
    hexed = data.hex()
    return "hex:" + (hexed if len(hexed) <= 200 else hexed[:200] + "…")


def fmt_ts(ts: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ts)) + f".{int((ts % 1) * 1000):03d}"


# ---------------- SQLite：目标持久化 + 报文日志 ----------------
class Store:
    """线程安全的 SQLite 存取。目标(target)落库，重启后保留。"""

    def __init__(self, path):
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS message_log (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   ts REAL NOT NULL,
                   client TEXT NOT NULL DEFAULT '',
                   session_id INTEGER NOT NULL DEFAULT 0,
                   direction TEXT NOT NULL,
                   receiver TEXT NOT NULL DEFAULT '',
                   payload TEXT NOT NULL DEFAULT '',
                   note TEXT NOT NULL DEFAULT '')"""
        )
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS observation_rounds (
                   round_id TEXT PRIMARY KEY,
                   data TEXT NOT NULL)"""
        )
        self._conn.commit()
        row = self._conn.execute("SELECT value FROM settings WHERE key='target'").fetchone()
        if row and row[0] in RECEIVERS:
            self._target = row[0]
        else:
            self._target = DEFAULT_TARGET if DEFAULT_TARGET in RECEIVERS else sorted(RECEIVERS)[0]
            self._conn.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES('target',?)", (self._target,)
            )
            self._conn.commit()
        self._inserts = 0

    def get_target(self):
        with self._lock:
            return self._target

    def set_target(self, target):
        """先持久化，成功后才切换内存中的当前目标；失败则抛异常并保留原目标。"""
        if target not in RECEIVERS:
            raise ValueError(f"未知接收器: {target!r}")
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES('target',?)", (target,)
            )
            self._conn.commit()
            self._target = target

    def log(self, direction, client="", session_id=0, receiver="", payload="", note=""):
        try:
            with self._lock:
                self._conn.execute(
                    "INSERT INTO message_log(ts,client,session_id,direction,receiver,payload,note)"
                    " VALUES(?,?,?,?,?,?,?)",
                    (time.time(), client, session_id, direction, receiver, payload, note),
                )
                self._conn.commit()
                self._inserts += 1
                if self._inserts % 200 == 0:
                    self._conn.execute(
                        "DELETE FROM message_log WHERE id < "
                        "(SELECT COALESCE(MIN(id),0) FROM "
                        "(SELECT id FROM message_log ORDER BY id DESC LIMIT ?))",
                        (LOG_KEEP,),
                    )
                    self._conn.commit()
        except sqlite3.Error:
            pass  # 日志失败绝不影响转发

    def recent_messages(self, limit=100):
        with self._lock:
            rows = self._conn.execute(
                "SELECT id,ts,client,session_id,direction,receiver,payload,note "
                "FROM message_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        cols = ("id", "ts", "client", "session_id", "direction", "receiver", "payload", "note")
        return [dict(zip(cols, r)) for r in rows]

    def latest_receiver_for_client(self, client):
        with self._lock:
            row = self._conn.execute(
                "SELECT receiver FROM message_log WHERE client=? AND direction='c2r' "
                "ORDER BY id DESC LIMIT 1",
                (client,),
            ).fetchone()
        return row[0] if row else None

    # ---- 冷却试算观察轮次持久化（内存记录在代理重启后即丢失）----
    def upsert_observation(self, round_id, data):
        try:
            with self._lock:
                self._conn.execute(
                    "INSERT OR REPLACE INTO observation_rounds(round_id,data) VALUES(?,?)",
                    (round_id, json.dumps(data, ensure_ascii=False)),
                )
                self._conn.commit()
        except sqlite3.Error:
            pass  # 持久化失败不影响转发；重启检测以存在的记录为准

    def get_observation(self, round_id):
        with self._lock:
            row = self._conn.execute(
                "SELECT data FROM observation_rounds WHERE round_id=?", (round_id,)
            ).fetchone()
        if not row:
            return None
        try:
            return json.loads(row[0])
        except (ValueError, TypeError):
            return None

    def latest_observation_id(self):
        with self._lock:
            row = self._conn.execute(
                "SELECT round_id FROM observation_rounds ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
        return row[0] if row else None


# ---------------- 上游端口分配（带冷却期） ----------------
class PortAllocator:
    """显式管理上游本地端口：关闭后冷却期内不再分配，
    确保旧会话迟到的回包不会被复用同端口的新会话收到。

    占用事件（供冷却试算观察记录，不改变分配语义）：
    - reserve：端口从预留起即占用；
    - return_failed：预留后绑定失败归还，占用立即结束、不进入冷却；
    - release：正常关闭，占用结束并进入冷却。
    """

    def __init__(self, low, high, cooldown):
        self.low, self.high, self.cooldown = low, high, cooldown
        self._next = low
        self._in_use = {}    # port -> 预留时刻(monotonic)
        self._closed = {}    # port -> 关闭时刻(monotonic)
        self._lock = threading.Lock()
        # 观察期事件回调：无锁原子读写，端口锁内只“读引用后回调”，
        # 回调内部绝不再访问本分配器，避免与观察管理器形成交叉加锁。
        self._recorder = None

    def set_recorder(self, fn):
        self._recorder = fn

    def _emit(self, port, kind):
        """调用方持有 self._lock；仅做无锁读取与外部回调，禁止回调本分配器。"""
        fn = self._recorder
        if fn is not None:
            try:
                fn(port, kind, time.monotonic())
            except Exception:
                pass

    def snapshot(self):
        """当前全部占用的开场快照（monotonic 时刻）：
        已预留（含绑定中）端口 + 仍在冷却的端口，各带其预留/关闭时刻。"""
        with self._lock:
            now = time.monotonic()
            in_use = dict(self._in_use)
            cooling = {p: t for p, t in self._closed.items() if now - t < self.cooldown}
            return {"ts": now, "in_use": in_use, "cooling": dict(cooling)}

    def allocate(self):
        with self._lock:
            now = time.monotonic()
            for _ in range(self.high - self.low + 1):
                port = self._next
                self._next += 1
                if self._next > self.high:
                    self._next = self.low
                if port in self._in_use:
                    continue
                closed_at = self._closed.get(port)
                if closed_at is not None and now - closed_at < self.cooldown:
                    continue
                self._in_use[port] = now
                self._closed.pop(port, None)
                self._emit(port, "reserve")
                return port
        raise RuntimeError("上游 UDP 端口耗尽")

    def release(self, port):
        with self._lock:
            self._in_use.pop(port, None)
            self._closed[port] = time.monotonic()
            self._emit(port, "release")

    def discard(self, port):
        """分配后绑定失败：不进入冷却，直接可用。"""
        with self._lock:
            self._in_use.pop(port, None)
            self._closed.pop(port, None)
            self._emit(port, "return_failed")


# ---------------- 会话与转发 ----------------
class Session:
    __slots__ = (
        "id", "client_addr", "receiver_name", "receiver_addr",
        "upstream_port", "transport", "state", "closed",
        "created", "last_activity", "pending",
    )

    def __init__(self, sid, client_addr):
        self.id = sid
        self.client_addr = client_addr
        self.receiver_name = ""
        self.receiver_addr = None
        self.upstream_port = None
        self.transport = None
        self.state = "opening"  # opening -> open -> closed
        self.closed = False
        self.created = time.monotonic()
        self.last_activity = self.created
        self.pending = []


class ClientProtocol(asyncio.DatagramProtocol):
    """面向遥测终端的 UDP 端。"""

    def __init__(self, state):
        self.state = state

    def datagram_received(self, data, addr):
        self.state.on_client_datagram(data, addr)

    def error_received(self, exc):
        pass


class UpstreamProtocol(asyncio.DatagramProtocol):
    """每个会话一条、已 connect 到其接收器的上游 UDP 端。
    只有该会话对应接收器的回包会到达这里。"""

    def __init__(self, state):
        self.state = state
        self.session = None

    def datagram_received(self, data, addr):
        sess = self.session
        if sess is not None:
            self.state.on_upstream_datagram(sess, data)

    def error_received(self, exc):
        pass  # 如对端 ICMP 不可达，忽略


class ProxyState:
    def __init__(self, store):
        self.store = store
        self.loop = None
        self.client_transport = None
        self.sessions = {}  # client_addr -> Session（仅事件循环线程修改）
        self.lock = threading.RLock()
        self.next_session_id = 1
        self.ports = PortAllocator(UPSTREAM_PORT_MIN, UPSTREAM_PORT_MAX, UPSTREAM_PORT_COOLDOWN)
        self.observations = ObservationManager(store, self.ports)

    async def start(self):
        transport, _ = await self.loop.create_datagram_endpoint(
            lambda: ClientProtocol(self),
            local_addr=(UDP_LISTEN_HOST, UDP_LISTEN_PORT),
        )
        self.client_transport = transport
        self.loop.create_task(self._sweeper())
        print(f"[proxy] 客户端 UDP 监听 {UDP_LISTEN_HOST}:{UDP_LISTEN_PORT}", flush=True)
        print(f"[proxy] 空闲会话超时 {SESSION_IDLE_TIMEOUT}s，当前目标 {self.store.get_target()}",
              flush=True)

    # ---- 客户端方向 ----
    def on_client_datagram(self, data, addr):
        now = time.monotonic()
        with self.lock:
            sess = self.sessions.get(addr)
            if sess is not None and sess.closed:
                sess = None
            if (sess is not None and sess.state == "open"
                    and now - sess.last_activity > SESSION_IDLE_TIMEOUT):
                self._close_session(sess, "idle-timeout")
                sess = None
            if sess is None:
                # 建会话：固定到“当前”目标接收器
                sess = Session(self.next_session_id, addr)
                self.next_session_id += 1
                self.sessions[addr] = sess
                sess.pending.append(data)
                self.loop.create_task(self._open_session(sess))
                return
            sess.last_activity = now
            if sess.state == "opening":
                sess.pending.append(data)
                return
        self._forward(sess, data)

    async def _open_session(self, sess):
        target = self.store.get_target()  # 建会话时固定接收器
        host, port = RECEIVERS[target]
        client = fmt_addr(sess.client_addr)
        try:
            transport = None
            upstream_port = None
            last_err = None
            for _ in range(8):
                upstream_port = self.ports.allocate()
                try:
                    transport, proto = await self.loop.create_datagram_endpoint(
                        lambda: UpstreamProtocol(self),
                        local_addr=("0.0.0.0", upstream_port),
                        remote_addr=(host, port),  # connect：只收该接收器的回包
                    )
                    break
                except OSError as exc:
                    last_err = exc
                    self.ports.discard(upstream_port)
            if transport is None:
                raise last_err or RuntimeError("无可用上游端口")
            with self.lock:
                if sess.closed:
                    transport.close()
                    self.ports.release(upstream_port)
                    return
                sess.receiver_name = target
                sess.receiver_addr = (host, port)
                sess.upstream_port = upstream_port
                sess.transport = transport
                sess.state = "open"
                proto.session = sess
                pending, sess.pending = sess.pending, []
            self.store.log("event", client=client, session_id=sess.id, receiver=target,
                           note=f"会话建立，固定到接收器 {target}（上游端口 {upstream_port}）")
            for data in pending:
                self._forward(sess, data)
        except Exception as exc:
            self.store.log("event", client=client, session_id=sess.id,
                           note=f"会话建立失败: {exc}")
            self.observations.record_open_failure(f"session {sess.id}: {exc}")
            self.close_session(sess, f"open-failed: {exc}")

    def _forward(self, sess, data):
        with self.lock:
            if sess.closed or sess.transport is None:
                return
            sess.last_activity = time.monotonic()
        try:
            sess.transport.sendto(data)  # 已 connect，原样发往该会话的接收器
        except Exception as exc:
            self.store.log("event", client=fmt_addr(sess.client_addr), session_id=sess.id,
                           note=f"转发失败: {exc}")
            return
        self.store.log("c2r", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, payload=preview(data))

    # ---- 接收器方向 ----
    def on_upstream_datagram(self, sess, data):
        now = time.monotonic()
        with self.lock:
            if sess.closed or self.sessions.get(sess.client_addr) is not sess:
                self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                               receiver=sess.receiver_name, payload=preview(data),
                               note="旧会话迟到的回包，已丢弃")
                return
            if now - sess.last_activity > SESSION_IDLE_TIMEOUT:
                self._close_session(sess, "idle-timeout")
                self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                               receiver=sess.receiver_name, payload=preview(data),
                               note="回包到达时会话已过期，已丢弃")
                return
            sess.last_activity = now
        self.client_transport.sendto(data, sess.client_addr)  # 原样送回该客户端
        self.store.log("r2c", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, payload=preview(data))

    # ---- 会话生命周期 ----
    def _close_session(self, sess, reason):
        """调用方需持有 self.lock。"""
        if sess.closed:
            return
        sess.closed = True
        sess.state = "closed"
        if self.sessions.get(sess.client_addr) is sess:
            del self.sessions[sess.client_addr]
        transport, sess.transport = sess.transport, None
        port, sess.upstream_port = sess.upstream_port, None
        if transport is not None:
            transport.close()  # 迟到的回包到此端口将被系统丢弃
        if port is not None:
            self.ports.release(port)  # 进入冷却期，冷却内不再分配
        self.store.log("event", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, note=f"会话关闭（{reason}）")

    def close_session(self, sess, reason):
        with self.lock:
            self._close_session(sess, reason)

    async def _sweeper(self):
        while True:
            await asyncio.sleep(0.25)
            now = time.monotonic()
            with self.lock:
                expired = [s for s in self.sessions.values()
                           if not s.closed and now - s.last_activity > SESSION_IDLE_TIMEOUT]
                for sess in expired:
                    self._close_session(sess, "idle-timeout")

    def snapshot_sessions(self):
        now = time.monotonic()
        with self.lock:
            return [
                {
                    "id": s.id,
                    "client": fmt_addr(s.client_addr),
                    "receiver": s.receiver_name or "?",
                    "upstream_port": s.upstream_port,
                    "state": s.state,
                    "idle": round(now - s.last_activity, 1),
                    "age": round(now - s.created, 1),
                }
                for s in sorted(self.sessions.values(), key=lambda x: x.id)
            ]


# ---------------- 冷却期端口池试算 ----------------
def compute_port_peak(opening, events, t0, cooldown):
    """用同一段实际占用记录，计算给定冷却 cooldown 下所需的最少端口数。

    把每个端口的每一次“占用”变成一个半开区间：
    - 预留(reserve)起占用；
    - 绑定失败归还(return_failed)：占用在归还时刻结束，不产生冷却；
    - 正常关闭(release)：占用在“关闭时刻 + cooldown”结束（冷却也算占用）；
    - 开场时仍预留 / 仍在冷却的端口，其区间从观察开始时刻 t0 起算
      （按各档 cooldown 重算冷却结束点，因此同一段开场记录可用于两档试算）；
    - 观察结束时仍未结束的区间，保留为开口区间，窗口内始终占用。
    同一端口在拟定时长下尚未结束的旧冷却，与该端口的新预留是两个并列区间，
    因此各自独立计入重叠数（即“另算一个端口”）。
    最少端口数 = 任意时刻这些区间的最大重叠数；同时返回首次达到峰值的时刻。

    返回 (peak, peak_offset_ms)，peak_offset_ms 为相对 t0 的毫秒数。
    """
    # 统一模型：cur = base（开口占用数） + 有限区间的扫描计数。
    # - 开场已预留端口：进入 base（窗口起点即在占用）；其后续 release 不立即减
    #   base，而是把 -1 放到“关闭时刻 + cooldown”，冷却结束才真正回收；
    # - 开场在冷却端口：按本档 cooldown 重算结束点，作为 [t0, end) 的有限区间；
    # - 窗口内 reserve：+1；return_failed 时同刻 -1（无冷却）；
    #   release 时把 +1 延后到 closed+cooldown 以 -1 结束（冷却也算占用）；
    # - 拟定档下同端口旧冷却未结束即复用时，旧冷却的结束点仍独立保留，
    #   新预留再 +1，因此“另算一个端口”。
    points = []  # (ts, delta)
    base = 0
    opening_in_use = set(opening.get("in_use", {}))
    base += len(opening_in_use)
    for _port, closed_at in opening.get("cooling", {}).items():
        end = closed_at + cooldown
        if end > t0:  # 按本档 cooldown 仍在冷却才计入
            points.append((t0, 1))
            points.append((end, -1))

    active = {}  # port -> 该次预留的起始时刻（窗口内 reserve）
    for port, kind, ts in events:
        if kind == "reserve":
            active[port] = ts
            points.append((ts, 1))
        elif kind == "return_failed":
            active.pop(port, None)
            points.append((ts, -1))  # 绑定失败归还即结束，无冷却
        elif kind == "release":
            active.pop(port, None)
            if port in opening_in_use:
                # 开场就在占用的端口：其占用已计入 base，不能在关闭时刻减，
                # 否则冷却段开始前会少算一个；只在冷却结束点 -1。
                opening_in_use.discard(port)
            points.append((ts + cooldown, -1))

    points.sort(key=lambda p: (p[0], p[1]))  # 同刻先 -1 后 +1（半开区间）

    cur = base
    peak = base
    peak_ts = t0
    for ts, delta in points:
        cur += delta
        if cur > peak:
            peak = cur
            peak_ts = ts
    return peak, max(0, int(round((peak_ts - t0) * 1000)))


class ObservationManager:
    """30 秒观察窗：只记录真实端口占用，不改在线配置，转发照常。

    作废条件（满足任一则本轮只显示原因）：
    - 缺少开场记录；
    - 本轮占用变更超过 4096 次；
    - 观察期间有会话建连失败；
    - 代理途中重启（观察记录全在内存，重启即丢，落库记录会被识别为孤儿）。
    """

    def __init__(self, store, ports):
        self.store = store
        self.ports = ports
        self._lock = threading.Lock()
        self._round = None
        self._timer = None

    # ---- 启动/结束 ----
    def start(self, proposed_cooldown):
        try:
            proposed = float(proposed_cooldown)
        except (TypeError, ValueError):
            raise ValueError("拟定冷却秒数必须是数字")
        if proposed < UPSTREAM_PORT_COOLDOWN:
            raise ValueError(f"拟定冷却不得短于现值 {UPSTREAM_PORT_COOLDOWN:g} 秒")
        if proposed > PROPOSED_COOLDOWN_MAX:
            raise ValueError(f"拟定冷却最多 {PROPOSED_COOLDOWN_MAX:g} 秒")

        with self._lock:
            if self._round is not None and self._round["status"] == "running":
                raise ValueError("已有一轮观察正在进行，请等待其结束")

            now_mono = time.monotonic()
            opening = self.ports.snapshot()  # 开场记录：已预留 + 仍在冷却
            round_id = uuid.uuid4().hex
            self._round = {
                "round_id": round_id,
                "status": "running",
                "reason": "",
                "started_mono": now_mono,
                "started_wall": time.time(),
                "duration": OBSERVATION_SECONDS,
                "cooldown_now": UPSTREAM_PORT_COOLDOWN,
                "cooldown_proposed": proposed,
                "opening": opening,
                "events": [],
                "change_count": 0,
                "open_failures": [],
                "result": None,
            }
            self._timer = threading.Timer(OBSERVATION_SECONDS, self._on_timeout)
            self._timer.daemon = True
            self._timer.start()
            self.store.upsert_observation(round_id, self._serialize_locked())
            self.ports.set_recorder(self._on_event)  # 原子赋值，无锁
            return self._serialize_locked()

    def _on_timeout(self):
        """定时器线程：只翻转状态，不抢端口锁。"""
        with self._lock:
            rnd = self._round
            if rnd is None or rnd["status"] != "running":
                return
            rnd["status"] = "timed_out"
        # 锁外收尾：摘除 recorder 后做试算；新事件在状态非 running 后会被直接忽略
        self.ports.set_recorder(None)
        self._finalize()

    def finish(self):
        """同步结束当前轮次（主要用于测试）。"""
        with self._lock:
            rnd = self._round
            if rnd is None or rnd["status"] != "running":
                return
            rnd["status"] = "timed_out"
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
        self.ports.set_recorder(None)
        self._finalize()

    def _finalize(self):
        with self._lock:
            rnd = self._round
            if rnd is None or rnd["status"] != "timed_out":
                return

            opening = rnd.get("opening")
            if not opening or "in_use" not in opening or "cooling" not in opening:
                self._invalidate_locked("缺少开场记录，本轮试算作废")
                return
            if rnd["open_failures"]:
                self._invalidate_locked(
                    "观察期间有会话建连失败（" + rnd["open_failures"][0] + "），本轮试算作废"
                )
                return

            t0 = rnd["started_mono"]
            now_peak, now_off = compute_port_peak(
                opening, rnd["events"], t0, rnd["cooldown_now"]
            )
            prop_peak, prop_off = compute_port_peak(
                opening, rnd["events"], t0, rnd["cooldown_proposed"]
            )
            rnd["status"] = "done"
            rnd["result"] = {
                "ports_now": now_peak,
                "peak_offset_now_ms": now_off,
                "peak_wall_now": rnd["started_wall"] + now_off / 1000.0,
                "ports_proposed": prop_peak,
                "peak_offset_proposed_ms": prop_off,
                "peak_wall_proposed": rnd["started_wall"] + prop_off / 1000.0,
                "change_count": rnd["change_count"],
            }
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
            self.store.upsert_observation(rnd["round_id"], self._serialize_locked())

    # ---- 事件回调（PortAllocator 锁内只读引用后调用，这里禁止再访问分配器）----
    def _on_event(self, port, kind, ts):
        invalidate_reason = None
        with self._lock:
            rnd = self._round
            if rnd is None or rnd["status"] != "running":
                return
            rnd["events"].append([port, kind, ts])
            rnd["change_count"] += 1
            if rnd["change_count"] > OBSERVATION_MAX_CHANGES:
                self._invalidate_locked(
                    f"本轮占用变更 {rnd['change_count']} 次，超过 {OBSERVATION_MAX_CHANGES} 次上限"
                )
                invalidate_reason = rnd["reason"]
        # 锁外原子摘除 recorder，作废后不再产生空转事件
        if invalidate_reason is not None:
            self.ports.set_recorder(None)

    def record_open_failure(self, detail):
        with self._lock:
            rnd = self._round
            if rnd is None or rnd["status"] != "running":
                return
            rnd["open_failures"].append(detail)
            self._invalidate_locked(f"观察期间有会话建连失败（{detail}），本轮试算作废")
        self.ports.set_recorder(None)  # 锁外原子摘除

    def _invalidate_locked(self, reason):
        rnd = self._round
        if rnd is None or rnd["status"] not in ("running", "timed_out"):
            return
        rnd["status"] = "invalid"
        rnd["reason"] = reason
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        self.store.upsert_observation(rnd["round_id"], self._serialize_locked())

    # ---- 输出 ----
    def _serialize_locked(self):
        rnd = self._round
        if rnd is None:
            return None
        # timed_out 是定时器触发后的内部收尾瞬态，对外仍显示进行中
        status = "running" if rnd["status"] == "timed_out" else rnd["status"]
        return {
            "round_id": rnd["round_id"],
            "status": status,
            "reason": rnd["reason"],
            "started_wall": rnd["started_wall"],
            "duration": rnd["duration"],
            "cooldown_now": rnd["cooldown_now"],
            "cooldown_proposed": rnd["cooldown_proposed"],
            "change_count": rnd["change_count"],
            "open_failures": list(rnd["open_failures"]),
            "result": rnd["result"],
        }

    def payload(self):
        """给网页/API 的当前观察状态；重启后内存记录丢失，识别落库孤儿轮次。"""
        with self._lock:
            if self._round is not None:
                data = self._serialize_locked()
            else:
                rid = self.store.latest_observation_id()
                if rid is None:
                    return {"cooldown_now": UPSTREAM_PORT_COOLDOWN,
                            "cooldown_max": PROPOSED_COOLDOWN_MAX,
                            "duration": OBSERVATION_SECONDS,
                            "round": None}
                data = self.store.get_observation(rid)
                if data is not None and data.get("status") == "running":
                    # 观察记录只在内存：代理重启后能走到这里说明途中重启过
                    data["status"] = "invalid"
                    data["reason"] = "代理途中重启，观察记录已丢失，本轮试算作废"
        out = {
            "cooldown_now": UPSTREAM_PORT_COOLDOWN,
            "cooldown_max": PROPOSED_COOLDOWN_MAX,
            "duration": OBSERVATION_SECONDS,
            "round": data,
        }
        return out


# ---------------- Flask 网页 ----------------
store = Store(DB_PATH)
state = ProxyState(store)
app = Flask(__name__)
app.secret_key = os.urandom(16)

_last_test = {}
_last_test_lock = threading.Lock()

DIRECTION_LABELS = {
    "c2r": "客户端→接收器",
    "r2c": "接收器→客户端",
    "event": "事件",
    "drop": "丢弃",
}


def _format_message(m):
    m = dict(m)
    m["ts"] = fmt_ts(m["ts"])
    m["direction"] = DIRECTION_LABELS.get(m["direction"], m["direction"])
    return m


@app.get("/")
def index():
    with _last_test_lock:
        lt = dict(_last_test) if _last_test else None
    return render_template(
        "index.html",
        target=store.get_target(),
        receivers={k: f"{h}:{p}" for k, (h, p) in RECEIVERS.items()},
        idle_timeout=SESSION_IDLE_TIMEOUT,
        udp_listen_port=UDP_LISTEN_PORT,
        last_test=lt,
        observation=state.observations.payload(),
    )


@app.get("/api/state")
def api_state():
    with _last_test_lock:
        lt = dict(_last_test) if _last_test else None
    return jsonify({
        "target": store.get_target(),
        "receivers": {k: f"{h}:{p}" for k, (h, p) in RECEIVERS.items()},
        "idle_timeout": SESSION_IDLE_TIMEOUT,
        "sessions": state.snapshot_sessions(),
        "messages": [_format_message(m) for m in store.recent_messages(100)],
        "last_test": lt,
    })


@app.get("/api/observation")
def api_observation():
    return jsonify(state.observations.payload())


@app.post("/observation/start")
def observation_start():
    try:
        state.observations.start(request.form.get("cooldown_proposed", "").strip())
        flash(f"已开始 {OBSERVATION_SECONDS} 秒观察。试算只做记录，在线冷却配置不变，转发照常。",
              "ok")
    except ValueError as exc:
        flash(f"无法开始观察：{exc}", "error")
    return redirect(url_for("index"))


@app.post("/switch")
def switch():
    target = request.form.get("target", "")
    try:
        store.set_target(target)
        store.log("event", note=f"网页切换目标为接收器 {target}")
        flash(f"已切换到接收器 {target}。新会话走 {target}，旧会话继续走原接收器直至过期。", "ok")
    except Exception as exc:
        flash(f"切换保存失败，保留原目标 {store.get_target()}。原因：{exc}", "error")
    return redirect(url_for("index"))


@app.post("/test")
def test():
    global _last_test
    result = run_test(
        payload=request.form.get("payload", "PING"),
        delay_s=request.form.get("delay", "").strip(),
        client_port_s=request.form.get("client_port", "").strip(),
        wait_s=request.form.get("wait", "10").strip(),
    )
    with _last_test_lock:
        _last_test = result
    return redirect(url_for("index"))


def run_test(payload, delay_s, client_port_s, wait_s):
    """从网页后端经代理发一条试传报文并等待回包。"""
    try:
        wait = min(max(float(wait_s), 0.5), 60.0)
    except ValueError:
        wait = 10.0
    wire = payload
    try:
        delay = float(delay_s)
        if delay > 0:
            wire = f"DELAY:{delay}:{payload}"  # 接收器识别的延迟回包指令
    except ValueError:
        pass
    data = wire.encode()
    result = {"time": fmt_ts(time.time()), "payload": wire, "ok": False,
              "reply": "", "error": "", "rtt": None, "client": "", "receiver": None}
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if client_port_s:
            sock.bind(("0.0.0.0", int(client_port_s)))
        sock.settimeout(wait)
        sock.connect(("127.0.0.1", UDP_LISTEN_PORT))
        result["client"] = fmt_addr(sock.getsockname())
        t0 = time.monotonic()
        sock.send(data)
        try:
            reply = sock.recv(65535)
            result["ok"] = True
            result["rtt"] = round(time.monotonic() - t0, 3)
            result["reply"] = preview(reply)
        except socket.timeout:
            result["error"] = f"等待回包超时（{wait}s）"
    except Exception as exc:
        result["error"] = f"发送失败：{exc}"
    finally:
        sock.close()
    time.sleep(0.05)  # 等代理把日志落库
    if result["client"]:
        result["receiver"] = store.latest_receiver_for_client(result["client"])
    return result


# ---------------- 启动：asyncio 线程 + Flask 主线程 ----------------
def _run_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    state.loop = loop
    loop.run_until_complete(state.start())
    loop.run_forever()


def main():
    t = threading.Thread(target=_run_loop, name="udp-proxy-loop", daemon=True)
    t.start()
    print(f"[proxy] 操作网页 http://{HTTP_HOST}:{HTTP_PORT}", flush=True)
    app.run(host=HTTP_HOST, port=HTTP_PORT, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
