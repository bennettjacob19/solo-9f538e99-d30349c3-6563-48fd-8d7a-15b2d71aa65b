#!/usr/bin/env python3
"""上游端口占用观察与冷却期试算。

只做记录和计算，不参与报文转发，也不修改在线配置。

占用语义：
- 端口从预留（allocate）起算占用；
- 绑定失败（discard）当即归还，不进入冷却；
- 正常关闭（release）后进入冷却，冷却期也视为占用；
- 同一端口被复用时，若旧的拟定时长冷却尚未结束，按“另一个端口”计。

观察开始时从端口分配器取一份开场记录（已预留端口、仍在冷却的端口），
之后记录本轮每一次预留/归还/关闭，观察固定时长后按现用/拟定冷却分别
回放，求出各自的最少端口数（并发占用峰值）与首次达到峰值的时刻。
"""

import threading
import time

DEFAULT_DURATION = 30.0
MAX_COOLDOWN = 120.0
MAX_CHANGES = 4096


# ---------------- 纯计算 ----------------
def normalize_baseline(raw, start_mono, current_cooldown, proposed_cooldown):
    """把分配器快照转成相对观察开始的基准占用记录。

    只保留在任一档冷却下仍可能占用的冷却项，避免开场记录无谓膨胀。
    返回 None 表示没有开场记录。
    """
    if raw is None:
        return None
    reserved = []
    for item in raw.get("reserved", []) or []:
        token = item.get("token")
        if token is None:
            continue
        reserved.append({"token": int(token), "port": int(item.get("port", 0))})

    horizon = max(current_cooldown, proposed_cooldown)
    cooldown = []
    for item in raw.get("cooldown", []) or []:
        token = item.get("token")
        closed_at = item.get("closed_at")
        if token is None or closed_at is None:
            continue
        rel = float(closed_at) - start_mono
        if rel + horizon > 0.0:
            cooldown.append({
                "token": int(token),
                "port": int(item.get("port", 0)),
                "closed_at": rel,
            })
    return {"reserved": reserved, "cooldown": cooldown}


def baseline_counts(baseline, current_cooldown, proposed_cooldown):
    """开场记录中各档的占用数，用于页面说明。"""
    baseline = baseline or {}
    cooldown = baseline.get("cooldown", [])
    return {
        "reserved": len(baseline.get("reserved", [])),
        "cooling_current": sum(
            1 for c in cooldown if c["closed_at"] + current_cooldown > 0
        ),
        "cooling_proposed": sum(
            1 for c in cooldown if c["closed_at"] + proposed_cooldown > 0
        ),
    }


def _intervals(baseline, events, cooldown, duration):
    """按给定冷却时长展开占用区间 [start, end)。"""
    baseline = baseline or {}
    open_tokens = {item["token"]: 0.0 for item in baseline.get("reserved", [])}
    reservations = []
    releases = []

    for event in events:
        kind = event.get("kind")
        token = event.get("token")
        if kind == "reserve":
            open_tokens[token] = float(event.get("t", 0.0))
        elif kind in ("discard", "release"):
            start = open_tokens.pop(token, None)
            if start is None:
                continue
            end = float(event.get("t", 0.0))
            if end < start:
                end = start
            reservations.append((start, end))
            if kind == "release":
                releases.append(end)

    # 观察结束时仍未关闭的端口，至少占用到观察结束
    for start in open_tokens.values():
        reservations.append((start, duration))

    intervals = list(reservations)
    for closed_at in releases:
        intervals.append((closed_at, closed_at + cooldown))
    for item in baseline.get("cooldown", []):
        closed_at = float(item["closed_at"])
        if closed_at + cooldown > 0.0:
            intervals.append((0.0, closed_at + cooldown))
    return intervals


def peak_occupancy(intervals, duration):
    """并发占用峰值与首次达到峰值的相对时刻（秒）。

    区间按 [start, end) 处理；同一时刻先处理结束再处理开始，
    因此冷却恰好到期后又复用同一端口不会虚增一个端口。
    """
    points = []
    for start, end in intervals:
        if end <= 0.0 or start >= duration:
            continue
        start = max(float(start), 0.0)
        end = min(float(end), float(duration))
        if end <= start:
            continue
        points.append((start, 1))
        points.append((end, -1))
    points.sort(key=lambda p: (p[0], p[1]))  # 同一时刻 -1 在前

    count = 0
    peak = -1
    peak_at = 0.0
    i = 0
    while i < len(points):
        t = points[i][0]
        while i < len(points) and points[i][0] == t:
            count += points[i][1]
            i += 1
        if count > peak:
            peak = count
            peak_at = t
    if peak < 0:
        peak = 0
        peak_at = 0.0
    return peak, peak_at


def compute_result(baseline, events, current_cooldown, proposed_cooldown, duration):
    """同一段实际占用记录，分别回放现用与拟定冷却。"""
    current_intervals = _intervals(baseline, events, current_cooldown, duration)
    proposed_intervals = _intervals(baseline, events, proposed_cooldown, duration)
    current_ports, current_peak_at = peak_occupancy(current_intervals, duration)
    proposed_ports, proposed_peak_at = peak_occupancy(proposed_intervals, duration)
    return {
        "current": {
            "needed_ports": current_ports,
            "peak_at": current_peak_at,
        },
        "proposed": {
            "needed_ports": proposed_ports,
            "peak_at": proposed_peak_at,
        },
        "event_count": len(events),
    }


# ---------------- 观察记录器 ----------------
class PortObservation:
    """单轮观察。线程安全；记录回调只做内存追加，不影响转发。

    观察中若出现会话建连失败、占用变更超过上限、缺少开场记录，
    本轮立即作废并只保留原因。
    """

    def __init__(self, duration=DEFAULT_DURATION, max_changes=MAX_CHANGES):
        self.duration = max(1.0, float(duration))
        self.max_changes = int(max_changes)
        self._lock = threading.Lock()
        self._status = "idle"  # idle / running / done / invalid
        self._round_id = None
        self._start_mono = 0.0
        self._started_at = 0.0
        self._current_cooldown = 0.0
        self._proposed_cooldown = 0.0
        self._baseline = None
        self._baseline_counts = {"reserved": 0, "cooling_current": 0,
                                 "cooling_proposed": 0}
        self._events = []
        self._result = None
        self._reason = ""
        self._listener = None

    # ---- 生命周期 ----
    def set_finished_listener(self, fn):
        with self._lock:
            self._listener = fn

    def is_running(self):
        with self._lock:
            return self._status == "running"

    def begin(self, raw_baseline, current_cooldown, proposed_cooldown,
              started_at, now_mono, round_id=None):
        """开始一轮观察。raw_baseline 为 None 表示缺少开场记录。"""
        baseline = normalize_baseline(
            raw_baseline, now_mono, current_cooldown, proposed_cooldown
        )
        with self._lock:
            if self._status == "running":
                return False
            self._round_id = round_id
            self._started_at = float(started_at)
            self._start_mono = float(now_mono)
            self._current_cooldown = float(current_cooldown)
            self._proposed_cooldown = float(proposed_cooldown)
            self._baseline = baseline
            self._baseline_counts = baseline_counts(
                baseline, current_cooldown, proposed_cooldown
            )
            self._events = []
            self._result = None
            self._reason = ""
            self._status = "running"
        return True

    # ---- 分配器事件 ----
    def reserve(self, token, port, now_mono):
        self._record("reserve", token, port, now_mono)

    def discard(self, token, port, now_mono):
        self._record("discard", token, port, now_mono)

    def release(self, token, port, now_mono):
        self._record("release", token, port, now_mono)

    def _record(self, kind, token, port, now_mono):
        finished = None
        with self._lock:
            if self._status != "running":
                return
            rel = now_mono - self._start_mono
            if rel < 0.0:
                rel = 0.0
            if rel > self.duration:
                return  # 观察已到期，定时器稍后结算
            if len(self._events) >= self.max_changes:
                finished = self._invalidate_locked(
                    f"本轮占用变更超过 {self.max_changes} 次"
                )
            else:
                self._events.append({
                    "kind": kind,
                    "token": token,
                    "port": port,
                    "t": rel,
                })
        if finished is not None:
            self._emit(finished)

    # ---- 作废 / 结算 ----
    def restore_invalid(self, reason, meta=None):
        """代理重启后恢复上一轮：只保留原因，不参与试算。"""
        meta = meta or {}
        with self._lock:
            self._status = "invalid"
            self._reason = reason
            self._events = []
            self._result = None
            self._round_id = meta.get("round_id")
            self._started_at = float(meta.get("started_at") or time.time())
            self._start_mono = time.monotonic()
            try:
                self._duration = float(meta.get("duration") or self.duration)
            except (TypeError, ValueError):
                self._duration = self.duration
            try:
                self._current_cooldown = float(meta.get("current_cooldown") or 0.0)
            except (TypeError, ValueError):
                self._current_cooldown = 0.0
            try:
                self._proposed_cooldown = float(meta.get("proposed_cooldown") or 0.0)
            except (TypeError, ValueError):
                self._proposed_cooldown = 0.0
            self._baseline = None
            self._baseline_counts = {"reserved": 0, "cooling_current": 0,
                                     "cooling_proposed": 0}

    def invalidate(self, reason):
        finished = None
        with self._lock:
            finished = self._invalidate_locked(reason)
        if finished is not None:
            self._emit(finished)
        return finished

    def _invalidate_locked(self, reason):
        if self._status != "running":
            return None
        self._status = "invalid"
        self._reason = reason
        self._events = []
        return self._view_locked(time.monotonic())

    def finalize_if_due(self, now_mono, round_id=None):
        """到期后结算。返回结算结果；未到期或已结束时返回 None。"""
        finished = None
        with self._lock:
            if self._status != "running":
                return None
            if round_id is not None and round_id != self._round_id:
                return None
            if now_mono - self._start_mono < self.duration:
                return None
            if self._baseline is None:
                finished = self._invalidate_locked("缺少开场记录")
            else:
                self._result = compute_result(
                    self._baseline,
                    self._events,
                    self._current_cooldown,
                    self._proposed_cooldown,
                    self.duration,
                )
                self._status = "done"
                finished = self._view_locked(now_mono)
                self._events = []
        if finished is not None:
            self._emit(finished)
        return finished

    # ---- 视图 ----
    def _view_locked(self, now_mono):
        view = {
            "status": self._status,
            "round_id": self._round_id,
            "duration": self.duration,
            "current_cooldown": self._current_cooldown,
            "proposed_cooldown": self._proposed_cooldown,
            "started_at": self._started_at,
            "event_count": len(self._events),
            "baseline": dict(self._baseline_counts),
            "reason": self._reason,
            "current": None,
            "proposed": None,
        }
        if self._status == "running":
            elapsed = max(0.0, now_mono - self._start_mono)
            view["elapsed"] = round(elapsed, 1)
            view["remaining"] = round(max(0.0, self.duration - elapsed), 1)
        elif self._status == "done" and self._result:
            view["current"] = dict(self._result["current"])
            view["proposed"] = dict(self._result["proposed"])
            view["event_count"] = self._result.get("event_count", 0)
            view["elapsed"] = self.duration
            view["remaining"] = 0.0
        return view

    def view(self, now_mono=None):
        now_mono = time.monotonic() if now_mono is None else now_mono
        with self._lock:
            return self._view_locked(now_mono)

    def _emit(self, view):
        with self._lock:
            fn = self._listener
        if fn is not None:
            try:
                fn(view)
            except Exception:
                pass
