#!/usr/bin/env python3
"""本地端到端验证：不依赖 Docker，直接以子进程启动两个接收器和代理，
逐项验证需求行为。用法：python3 scripts/verify_local.py
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROXY_PY = os.path.join(ROOT, "proxy", "app.py")
RECEIVER_PY = os.path.join(ROOT, "receiver", "receiver.py")

UDP_PORT = 19000
HTTP_PORT = 18080
BASE = f"http://127.0.0.1:{HTTP_PORT}"

_failures = []


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def http_get(path):
    with urllib.request.urlopen(BASE + path, timeout=10) as r:
        return json.load(r)


def http_post(path, data):
    with urllib.request.urlopen(
        BASE + path, data=urllib.parse.urlencode(data).encode(), timeout=40
    ) as r:
        return r.status


def wait_http_up(timeout=20):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            http_get("/api/state")
            return True
        except Exception:
            time.sleep(0.3)
    return False


def udp_client():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("127.0.0.1", UDP_PORT))
    return s


def send_recv(sock, payload: bytes, timeout=5.0):
    sock.settimeout(timeout)
    sock.send(payload)
    return sock.recv(65535)


def log_entries(client=None, direction=None, text=None):
    msgs = http_get("/api/state")["messages"]
    out = []
    for m in msgs:
        if client and m["client"] != client:
            continue
        if direction and m["direction"] != direction:
            continue
        if text and text not in m["payload"] and text not in m["note"]:
            continue
        out.append(m)
    return out


def start_receiver(name, port, logdir):
    env = dict(os.environ, RECEIVER_NAME=name, LISTEN_PORT=str(port), REPLY_DELAY="0")
    log = open(os.path.join(logdir, f"receiver-{name}.log"), "w")
    return subprocess.Popen([sys.executable, RECEIVER_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def start_proxy(db_path, logdir, tag, extra_env=None):
    env = dict(
        os.environ,
        UDP_LISTEN_PORT=str(UDP_PORT), HTTP_PORT=str(HTTP_PORT),
        RECEIVER_A_HOST="127.0.0.1", RECEIVER_A_PORT="19001",
        RECEIVER_B_HOST="127.0.0.1", RECEIVER_B_PORT="19002",
        SESSION_IDLE_TIMEOUT="5", DB_PATH=db_path,
        UPSTREAM_PORT_MIN="21000", UPSTREAM_PORT_MAX="21049",
        OBSERVE_WINDOW="8",
    )
    if extra_env:
        env.update(extra_env)
    log = open(os.path.join(logdir, f"proxy-{tag}.log"), "w")
    return subprocess.Popen([sys.executable, PROXY_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def observation():
    return http_get("/api/observation")


def wait_observation_done(timeout=15):
    t0 = time.time()
    while time.time() - t0 < timeout:
        o = observation()
        if o["status"] != "running":
            return o
        time.sleep(0.5)
    return observation()


def check_observation_unit():
    """不依赖进程的端口占用回放语义。"""
    sys.path.insert(0, os.path.join(ROOT, "proxy"))
    import observation as obs
    check("观察窗口默认 30 秒", obs.DEFAULT_DURATION == 30.0, str(obs.DEFAULT_DURATION))
    check("拟定冷却上限 120 秒", obs.MAX_COOLDOWN == 120.0, str(obs.MAX_COOLDOWN))

    baseline = {
        "reserved": [{"token": 1, "port": 20000}],
        "cooldown": [
            {"token": 2, "port": 20001, "closed_at": -10.0},
            {"token": 3, "port": 20002, "closed_at": -100.0},
        ],
    }
    events = [
        {"kind": "reserve", "token": 4, "port": 20003, "t": 1.0},
        {"kind": "release", "token": 4, "port": 20003, "t": 2.0},
        {"kind": "reserve", "token": 5, "port": 20003, "t": 3.0},
        {"kind": "release", "token": 5, "port": 20003, "t": 4.0},
    ]
    r = obs.compute_result(baseline, events, 30.0, 120.0, 30.0)
    # 现用 30s：开场预留 1 + 开场冷却 1 + t=1 新预留 1 = 3；
    # t=3 时该端口先关后复用，旧冷却仍在，另算一个 → 4
    check("现用冷却回放峰值", r["current"]["needed_ports"] == 4, str(r))
    # 拟定 120s：t=0 时 -100s 关闭的旧冷却仍在，开场即多算一个 → 5
    check("拟定冷却回放峰值", r["proposed"]["needed_ports"] == 5, str(r))
    check("首次达到峰值时刻", r["current"]["peak_at"] == 3.0, str(r))

    # 绑定失败当即归还：从预留到归还短暂占用，不进入冷却
    ev2 = [
        {"kind": "reserve", "token": 10, "port": 20010, "t": 0.5},
        {"kind": "discard", "token": 10, "port": 20010, "t": 0.6},
    ]
    r2 = obs.compute_result(None, ev2, 30.0, 120.0, 10.0)
    check("绑定失败归还后不进入冷却", r2["proposed"]["needed_ports"] == 1
          and r2["proposed"]["peak_at"] == 0.5, str(r2))
    check("无事件时峰值为 0",
          obs.compute_result(None, [], 30.0, 120.0, 10.0)["current"]["needed_ports"] == 0)

    # 同一端口在拟定冷却未结束时复用：旧冷却另算一个端口
    ev3 = [
        {"kind": "reserve", "token": 20, "port": 20020, "t": 0.2},
        {"kind": "release", "token": 20, "port": 20020, "t": 0.3},
        {"kind": "reserve", "token": 21, "port": 20020, "t": 2.0},
        {"kind": "release", "token": 21, "port": 20020, "t": 2.5},
    ]
    r3 = obs.compute_result(None, ev3, 1.0, 120.0, 10.0)
    check("复用端口时旧冷却另算", r3["proposed"]["needed_ports"] == 2, str(r3))
    check("现用冷却已过期则复用不叠加", r3["current"]["needed_ports"] == 1, str(r3))

    # 缺少开场记录
    rec = obs.PortObservation(1.0, 4096)
    rec.begin(None, 30.0, 60.0, time.time(), time.monotonic())
    v = rec.finalize_if_due(time.monotonic() + 1.1)
    check("缺少开场记录时只显示原因",
          v and v["status"] == "invalid" and "开场" in v["reason"], str(v))

    # 占用变更超过上限
    rec = obs.PortObservation(1.0, obs.MAX_CHANGES)
    rec.begin({"reserved": [], "cooldown": []}, 30.0, 60.0,
              time.time(), time.monotonic())
    now = time.monotonic()
    for i in range(obs.MAX_CHANGES + 1):
        rec.reserve(100 + i, 20000 + i % 100, now)
    v = rec.view()
    check("本轮占用变更超过 4096 次时只显示原因",
          v["status"] == "invalid" and "4096" in v["reason"], str(v))

    # 有会话建连失败
    rec = obs.PortObservation(1.0, 4096)
    rec.begin({"reserved": [], "cooldown": []}, 30.0, 60.0,
              time.time(), time.monotonic())
    rec.invalidate("有会话建连失败（模拟）")
    v = rec.view()
    check("建连失败时只显示原因",
          v["status"] == "invalid" and "建连失败" in v["reason"], str(v))


def check_observation_e2e():
    # 拟定冷却不短于现值、不超过 120 秒
    http_post("/observe", {"cooldown": "10"})
    check("拟定冷却短于现值被拒", observation()["status"] != "running")
    http_post("/observe", {"cooldown": "300"})
    check("拟定冷却超过 120 秒被拒", observation()["status"] != "running")

    # 先制造一段历史占用：会话过期关闭后端口进入冷却
    c_old = udp_client()
    send_recv(c_old, b"warmup-old")
    c_old.close()
    print("--- 等待会话过期，让端口进入冷却 ---")
    time.sleep(7)
    # 观察开始时仍有一个已预留（未过期会话）的端口
    c_live = udp_client()
    send_recv(c_live, b"warmup-live")

    before_target = http_get("/api/state")["target"]
    http_post("/observe", {"cooldown": "60"})
    time.sleep(0.3)
    o = observation()
    check("观察已开始", o["status"] == "running", str(o))
    check("观察记录拟定冷却", o["proposed_cooldown"] == 60.0, str(o))
    check("观察记录现用冷却只读", o["current_cooldown"] == 30.0, str(o))
    base = o["baseline"]
    check("开场记录包含已预留端口", base["reserved"] >= 1, str(base))
    check("开场记录包含仍在冷却的占用",
          base["cooling_current"] >= 1 and base["cooling_proposed"] >= 1, str(base))

    # 观察期间照常转发，且不改在线配置
    sock = udp_client()
    check("观察期间转发照常", send_recv(sock, b"during-observe") == b"during-observe")
    http_post("/test", {"payload": "obs-probe", "wait": "3", "delay": "0",
                        "client_port": ""})
    check("观察期间网页试传可用",
          http_get("/api/state")["last_test"]["ok"] is True)
    check("试算不改在线目标", http_get("/api/state")["target"] == before_target)

    o = wait_observation_done()
    check("观察到期正常结束", o["status"] == "done", str(o))
    check("结束时给出两档所需端口数",
          isinstance(o["current"]["needed_ports"], int)
          and isinstance(o["proposed"]["needed_ports"], int), str(o))
    check("所需端口数不少于开场占用",
          o["current"]["needed_ports"] >= base["reserved"]
          and o["proposed"]["needed_ports"] >= base["reserved"], str(o))
    check("给出首次达到峰值的时刻",
          0.0 <= o["current"]["peak_at"] <= o["duration"]
          and 0.0 <= o["proposed"]["peak_at"] <= o["duration"], str(o))
    check("本轮记录了实际占用变更", o["event_count"] >= 1, str(o))
    check("观察结束后仍在观察态之外", o["remaining"] == 0.0, str(o))
    return True



def main():
    tmp = tempfile.mkdtemp(prefix="proxy-verify-")
    db_path = os.path.join(tmp, "proxy.db")
    procs = [start_receiver("A", 19001, tmp), start_receiver("B", 19002, tmp)]
    proxy = start_proxy(db_path, tmp, "run1")
    procs.append(proxy)
    try:
        if not wait_http_up():
            check("代理启动", False, f"日志见 {tmp}")
            return 1
        print(f"--- 环境就绪（日志目录 {tmp}）---")

        # 1. 初始目标为 A，报文原样往返
        st = http_get("/api/state")
        check("初始目标为 A", st["target"] == "A")
        c1 = udp_client()
        client1 = f"127.0.0.1:{c1.getsockname()[1]}"
        m1 = b"m1-hello"
        check("报文原样回传", send_recv(c1, m1) == m1)
        time.sleep(0.2)
        e = log_entries(client1, "客户端→接收器", "m1-hello")
        check("m1 实际去向为接收器 A", e and e[0]["receiver"] == "A", str(e))

        # 2. 二进制原样透传
        blob = bytes(range(256))
        check("二进制报文原样透传", send_recv(c1, blob) == blob)

        # 3. 网页切换目标到 B
        http_post("/switch", {"target": "B"})
        check("切换后当前目标为 B", http_get("/api/state")["target"] == "B")

        # 4. 旧会话（空闲超时内）仍走原接收器 A
        m2 = b"m2-stick"
        check("旧会话报文仍有回包", send_recv(c1, m2) == m2)
        time.sleep(0.2)
        e = log_entries(client1, "客户端→接收器", "m2-stick")
        check("切换后旧会话继续走接收器 A", e and e[0]["receiver"] == "A", str(e))

        # 5. 空闲 5 秒过期后，同 IP:端口 重建会话走新接收器 B
        print("--- 等待 6 秒让会话空闲过期 ---")
        time.sleep(6)
        m3 = b"m3-newsession"
        check("新会话报文有回包", send_recv(c1, m3) == m3)
        time.sleep(0.2)
        e = log_entries(client1, "客户端→接收器", "m3-newsession")
        check("过期重建的新会话走接收器 B", e and e[0]["receiver"] == "B", str(e))

        # 6. 旧会话迟到的回包：同 IP:端口 重建会话也收不到
        c2 = udp_client()
        client2 = f"127.0.0.1:{c2.getsockname()[1]}"
        t0 = time.time()
        c2.send(b"DELAY:7:late1")          # 接收器 7 秒后才回包；会话 5 秒即过期
        time.sleep(6 - (time.time() - t0))  # 等到会话过期之后
        check("过期后新会话报文有回包", send_recv(c2, b"ping2", timeout=3) == b"ping2")
        # 旧回包在 t0+7 到达，等到 t0+9 确认没有迟到回包送进来
        got_late = False
        c2.settimeout(0.2)
        while time.time() - t0 < 9:
            try:
                if b"late1" in c2.recv(65535):
                    got_late = True
            except socket.timeout:
                pass
        check("旧会话迟到的回包未送达重建的会话", not got_late)
        time.sleep(0.3)
        e = log_entries(client2, "接收器→客户端", "late1")
        check("日志中无 late1 的回包下发记录", not e, str(e))

        # 7. 网页试传报文
        http_post("/test", {"payload": "webtest1", "wait": "5", "delay": "0",
                            "client_port": ""})
        time.sleep(0.2)
        lt = http_get("/api/state")["last_test"]
        check("网页试传成功且回包一致",
              lt and lt["ok"] and lt["reply"] == "webtest1", str(lt))
        check("网页试传显示实际去向为 B", lt and lt["receiver"] == "B", str(lt))

        # 8. 端口池冷却期试算：观察期间照常转发、结束给出两档端口数
        check_observation_e2e()

        # 9. 重启：目标保留、会话清空
        http_post("/observe", {"cooldown": "90"})  # 观察途中重启
        time.sleep(0.3)
        check("重启前观察已开始", observation()["status"] == "running")
        c3 = udp_client()
        c3.send(b"before-restart")
        c3.settimeout(3)
        c3.recv(65535)
        proxy.terminate()
        proxy.wait(timeout=10)
        proxy = start_proxy(db_path, tmp, "run2")
        procs.append(proxy)
        check("代理重启后恢复", wait_http_up())
        st = http_get("/api/state")
        check("重启后目标保留为 B", st["target"] == "B")
        check("重启后会话已清空", st["sessions"] == [])
        o = observation()
        check("观察途中重启后本轮只显示原因",
              o["status"] == "invalid" and "重启" in o["reason"], str(o))
        c3.send(b"after-restart")  # 旧会话若未清空仍在 5 秒空闲窗口内
        c3.settimeout(3)
        c3.recv(65535)
        time.sleep(0.2)
        e = log_entries(f"127.0.0.1:{c3.getsockname()[1]}", "事件", "会话建立")
        check("重启后同客户端重建了全新会话", len(e) >= 1, str(e))

        # 10. 切换保存失败时保留原目标（进程内模拟数据库故障）
        sys.path.insert(0, os.path.join(ROOT, "proxy"))
        os.environ["DB_PATH"] = os.path.join(tmp, "other.db")
        import importlib
        import app as proxy_app
        importlib.reload(proxy_app)
        s = proxy_app.Store(os.path.join(tmp, "t.db"))
        s.set_target("B")
        s._conn.close()  # 模拟存储故障
        failed = False
        try:
            s.set_target("A")
        except Exception:
            failed = True
        check("保存失败时切换报错且保留原目标", failed and s.get_target() == "B")

        # 11. 端口占用回放的纯计算语义
        check_observation_unit()

    finally:
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()

    print("---")
    if _failures:
        print(f"共 {len(_failures)} 项失败: {_failures}")
        return 1
    print("全部验证通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
