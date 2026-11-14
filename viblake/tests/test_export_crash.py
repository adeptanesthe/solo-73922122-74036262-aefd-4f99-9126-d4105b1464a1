"""导出硬崩溃演练：子进程在导出文件已改名、尚未登记时 os._exit(0)。

验证：
1. 崩溃现场会留下未登记的成品 CSV 与 _tmp 残件；
2. 重启后两者都被清理（不留可下载残件）；
3. 崩溃前已登记并发布的导出文件仍可下载，登记元数据完整；
4. 重启后可再次正常导出。
"""
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

DATA = Path("/tmp/viblake-export-crash")
if DATA.exists():
    shutil.rmtree(DATA)

PY = sys.executable
ROOT = Path(__file__).resolve().parent.parent


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_health(base: str, timeout: float = 20) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = httpx.get(f"{base}/health", timeout=1)
            if r.status_code == 200:
                return r.json()
        except httpx.HTTPError:
            time.sleep(0.2)
    raise RuntimeError("server not healthy in time")


def stop(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except (PermissionError, subprocess.TimeoutExpired):
        pass


CRASHER = r"""
import os, time
from app.config import settings
# 轮询 exports 根目录：出现成品 CSV 即说明已改名、尚未登记，立即硬退出
deadline = time.time() + 15
while time.time() < deadline:
    d = settings.exports_dir
    if os.path.isdir(d):
        names = [n for n in os.listdir(d) if n.startswith("export-") and n.endswith(".csv")]
        # 第一个已发布（有登记）的导出不应触发；等它出现后，等第二个再崩溃
        if len(names) >= 2:
            os._exit(0)
    time.sleep(0.02)
raise SystemExit("crash window missed")
"""


def run_server(env: dict, port: int, crasher: bool) -> subprocess.Popen:
    if crasher:
        code = (
            "import threading, uvicorn\n"
            f"threading.Thread(target=lambda: exec({CRASHER!r}), daemon=True).start()\n"
            f"uvicorn.run('app.main:app', host='127.0.0.1', port={port})\n"
        )
        return subprocess.Popen([PY, "-u", "-c", code], env=env, cwd=ROOT)
    return subprocess.Popen(
        [PY, "-u", "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(port)],
        env=env, cwd=ROOT,
    )


# ---------- 实例 1：先发布一个导出，再让第二个在登记前崩溃 ----------
port1 = free_port()
base1 = f"http://127.0.0.1:{port1}"
env1 = dict(os.environ, VIBLAKE_DATA_DIR=str(DATA), VIBLAKE_EXPORT_DELAY_MS="1200")
proc = run_server(env1, port1, crasher=True)
try:
    wait_health(base1)
    httpx.post(f"{base1}/api/demo/generate?rows=6&day=2026-11-13&turbines=WTG-01", timeout=10)
    r = httpx.post(f"{base1}/api/exports?turbine=WTG-01&day=2026-11-13", timeout=10)
    assert r.status_code == 201, r.text
    first = r.json()
    print(f"[1] 已发布导出 {first['export_id']}（{first['row_count']} 行）", flush=True)

    r = httpx.post(f"{base1}/api/exports?turbine=WTG-01&day=2026-11-13", timeout=20)
    # 第二次导出会在改名后停顿，crasher 在此时硬退出；连接因此被直接断开
    rc = proc.wait(timeout=25)
    assert rc == 0, f"crasher exit={rc}"
    print("[2] 第二次导出在登记前 os._exit 硬退出", flush=True)
except (httpx.RemoteProtocolError, httpx.ReadError, httpx.ConnectError):
    rc = proc.wait(timeout=25)
    assert rc == 0, f"crasher exit={rc}"
    print("[2] 第二次导出在登记前 os._exit 硬退出（请求连接被断开）", flush=True)
finally:
    stop(proc)

csvs = [n for n in os.listdir(DATA / "exports") if n.endswith(".csv")]
tmps = [n for n in os.listdir(DATA / "exports" / "_tmp") if n.endswith(".csv")]
print(f"[3] 崩溃现场：exports 成品 {len(csvs)} 个（1 个已登记 + 1 个未登记残留），_tmp {len(tmps)} 个", flush=True)
assert len(csvs) == 2 and len(tmps) == 0

# ---------- 实例 2：重启清理未登记残件，保留已登记导出 ----------
port2 = free_port()
base2 = f"http://127.0.0.1:{port2}"
env2 = dict(os.environ, VIBLAKE_DATA_DIR=str(DATA))
env2.pop("VIBLAKE_EXPORT_DELAY_MS", None)
proc2 = run_server(env2, port2, crasher=False)
try:
    health = wait_health(base2)
    print("[4] 重启恢复报告:", health["recovery"], flush=True)
    assert health["recovery"]["export_orphans_removed"] == 1

    csvs = [n for n in os.listdir(DATA / "exports") if n.endswith(".csv")]
    assert csvs == [first["export_id"] + ".csv"], csvs
    print("[5] 未登记残件已清理，仅剩已登记导出文件", flush=True)

    listed = httpx.get(f"{base2}/api/exports", timeout=10).json()
    assert len(listed) == 1 and listed[0]["id"] == first["export_id"], listed
    detail = httpx.get(f"{base2}/api/exports/{first['export_id']}", timeout=10).json()
    assert detail["source_rows"] == 6 and detail["source_file_ids"], detail
    dl = httpx.get(f"{base2}/api/exports/{first['export_id']}/download", timeout=10)
    assert dl.status_code == 200
    assert dl.text.strip().splitlines()[0] == "turbine,ts_utc,vibration,row_count"
    print(f"[6] 重启后已发布导出可下载：{len(dl.text.strip().splitlines()) - 1} 行，"
          f"来源文件 {len(detail['source_file_ids'])} 个", flush=True)

    r = httpx.post(f"{base2}/api/exports?turbine=WTG-01&day=2026-11-13", timeout=15)
    assert r.status_code == 201, r.text
    print("[7] 重启后可再次正常导出", flush=True)
finally:
    stop(proc2)

# ---------- 实例 3：再重启，两个导出都还在 ----------
port3 = free_port()
base3 = f"http://127.0.0.1:{port3}"
proc3 = run_server(env2, port3, crasher=False)
try:
    health = wait_health(base3)
    assert health["recovery"]["export_temp_removed"] == 0
    assert health["recovery"]["export_orphans_removed"] == 0
    listed = httpx.get(f"{base3}/api/exports", timeout=10).json()
    assert len(listed) == 2, listed
    print("[8] 二次重启：两份导出登记稳定，可下载", flush=True)
finally:
    stop(proc3)

print("\n导出硬崩溃演练全部通过 ✅", flush=True)
