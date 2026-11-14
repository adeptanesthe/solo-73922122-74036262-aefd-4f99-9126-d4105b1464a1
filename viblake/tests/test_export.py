"""训练数据导出测试（不依赖 pytest，直接 asyncio 运行）。

覆盖：
1. 同刻同值合并为一行、原行数记录、按时间排序；
2. 同刻异值整体拒绝、列出时刻与取值、不产生任何文件 / 登记；
3. 成功导出登记文件、来源文件 ID 与源行数，可下载；
4. 导出期间上传 / 压实不影响受理时结果；旧件回收不打断导出读取；
5. 写文件 / 登记失败不留可下载残件；重启后已发布文件仍可下载；
6. 未发布的 _tmp 与无元数据孤儿 CSV 在重启时清理，遗留读快照被释放。
"""
from __future__ import annotations

import asyncio
import csv
import io
import os
import shutil
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import parquet_store as ps  # noqa: E402
from app.compaction import CompactionManager  # noqa: E402
from app.config import Settings  # noqa: E402
from app.db import Database  # noqa: E402
from app.gates import PauseGate  # noqa: E402
from app.service import DataLakeService, ExportRejected  # noqa: E402

TMP_ROOT = Path(__file__).resolve().parent / "tmp_export"
DAY = "2026-11-13"
NEXT_DAY = "2026-11-14"
TURBINE = "WTG-01"


def make_settings(case: str) -> Settings:
    root = TMP_ROOT / case
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    return Settings(data_dir=str(root), max_batch_rows=10000)


def csv_bytes(rows: list[tuple[str, str, float]]) -> bytes:
    lines = ["turbine,ts_utc,vibration"]
    lines.extend(f"{t},{ts},{v}" for t, ts, v in rows)
    return ("\n".join(lines) + "\n").encode()


def ts(hour: int, minute: int = 0, second: int = 0, day: str = DAY) -> str:
    return f"{day}T{hour:02d}:{minute:02d}:{second:02d}Z"


class Harness:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.db = Database(settings.db_path)
        self.gate = PauseGate()
        self.cm = CompactionManager(self.db, settings.lake_dir, self.gate)
        self.svc = DataLakeService(self.db, settings, self.gate, self.cm)

    async def start(self, recover: bool = False) -> dict | None:
        ps.ensure_dirs(self.settings.lake_dir)
        os.makedirs(self.settings.exports_dir, exist_ok=True)
        await self.db.connect()
        await self.gate.load(self.db)
        report = None
        if recover:
            await self.db.release_all_open_snapshots()
            report = await self.cm.recover()
            report.update(await self.svc.recover_exports())
        await self.cm.start_reaper()
        return report

    async def stop(self) -> None:
        await self.svc.close()
        await self.cm.close()
        await self.db.close()

    async def upload(self, rows, name: str = "x.csv"):
        return await self.svc.upload_csv(csv_bytes(rows), name)

    async def compact(self, day: str = DAY):
        cid, _ = await self.svc.start_compaction(day)
        for _ in range(200):
            row = await self.svc.compaction_status(cid)
            if row["status"] != "running":
                return row
            await asyncio.sleep(0.05)
        raise AssertionError("压实超时")

    def exports_dir_files(self) -> list[str]:
        d = self.settings.exports_dir
        return sorted(
            n for n in os.listdir(d) if n.endswith(".csv") and os.path.isfile(os.path.join(d, n))
        )


PASSED = 0
FAILED: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASSED
    if cond:
        PASSED += 1
        print(f"  ✓ {name}")
    else:
        FAILED.append(f"{name} {detail}")
        print(f"  ✗ {name} {detail}")


def read_csv(payload: bytes) -> tuple[list[str], list[list[str]]]:
    reader = csv.reader(io.StringIO(payload.decode("utf-8")))
    rows = list(reader)
    return rows[0], rows[1:]


async def test_merge_and_sort() -> None:
    print("[1] 同刻同值合并、原行数、按时间排序")
    s = make_settings("merge")
    h = Harness(s)
    await h.start()
    try:
        # 两个批次，含同刻同值重复与乱序时间
        await h.upload([
            (TURBINE, ts(10, 0), 1.5),
            (TURBINE, ts(8, 0), 0.25),
            (TURBINE, ts(10, 0), 1.5),
            ("WTG-02", ts(9, 0), 9.9),  # 其它机组不进入本次导出
        ])
        await h.upload([
            (TURBINE, ts(8, 0), 0.25),
            (TURBINE, ts(8, 0), 0.25),
            (TURBINE, ts(12, 30), -2.0),
        ])
        r = await h.svc.export_training_csv(TURBINE, DAY)
        check("输出 3 行（两个同刻同值组各合并为 1 行）", r["row_count"] == 3, str(r))
        check("源行数为受理时的 6 行", r["source_rows"] == 6, str(r))
        check("来源文件为 2 个批次件", r["source_file_count"] == 2, str(r))
        check("合计数 = 源行数 - 输出行数", r["merged_rows"] == 3, str(r))

        path = os.path.join(s.exports_dir, r["export_id"] + ".csv")
        with open(path, "rb") as fh:
            header, rows = read_csv(fh.read())
        check("CSV 表头含 row_count", header == ["turbine", "ts_utc", "vibration", "row_count"], str(header))
        check("按时间升序排列", [x[1] for x in rows] == sorted(x[1] for x in rows), str(rows))
        check("时间序列正确", [x[1] for x in rows] == [
            ts(8, 0), ts(10, 0), ts(12, 30)
        ], str(rows))
        check("同刻同值原行数记录为 3", rows[0][3] == "3" and rows[1][3] == "2", str(rows))
        check("振动值原样保留", rows[1][2] == "1.5" and rows[2][2] == "-2.0", str(rows))
        check("机组号写到每一行", all(x[0] == TURBINE for x in rows))
        check("磁盘仅 1 个导出文件", len(h.exports_dir_files()) == 1, str(h.exports_dir_files()))

        meta = await h.svc.get_export(r["export_id"])
        check("登记来源文件 ID 数 = 2", len(meta["source_file_ids"]) == 2, str(meta))
        check("登记的源行数 = 6", meta["source_rows"] == 6)
        check("登记的输出行数 = 3", meta["row_count"] == 3)
        listed = await h.svc.list_exports()
        check("导出列表包含该文件", any(x["id"] == r["export_id"] for x in listed))
        check("列表返回来源文件数", listed[0]["source_file_count"] == 2)
    finally:
        await h.stop()


async def test_conflict_rejected() -> None:
    print("[2] 同刻异值：列出时刻与取值，本次不发布")
    s = make_settings("conflict")
    h = Harness(s)
    await h.start()
    try:
        await h.upload([
            (TURBINE, ts(8, 0), 1.0),
            (TURBINE, ts(8, 0), 2.0),
            (TURBINE, ts(9, 0), 3.0),
        ])
        await h.upload([
            (TURBINE, ts(9, 0), 3.0),      # 同刻同值，不冲突
            (TURBINE, ts(11, 0), 4.0),
            (TURBINE, ts(11, 0), 4.5),
        ])
        try:
            await h.svc.export_training_csv(TURBINE, DAY)
            check("冲突时拒绝导出", False)
        except ExportRejected as exc:
            check("冲突时拒绝导出", True)
            check("冲突时间点数为 2", exc.conflict_count == 2, str(exc.conflict_count))
            first = exc.conflicts[0]
            check("冲突明细含时刻", first["ts_utc"] == ts(8, 0), str(first))
            check("冲突明细列出全部不同取值", sorted(first["values"]) == [1.0, 2.0], str(first))
            check("第二处冲突取值正确",
                  sorted(exc.conflicts[1]["values"]) == [4.0, 4.5], str(exc.conflicts))
        check("未发布任何导出文件", h.exports_dir_files() == [], str(h.exports_dir_files()))
        check("未登记任何导出记录", await h.svc.list_exports() == [])
        tmp = os.path.join(s.exports_dir, "_tmp")
        tmp_left = os.listdir(tmp) if os.path.isdir(tmp) else []
        check("_tmp 无残留", tmp_left == [], str(tmp_left))
    finally:
        await h.stop()


async def test_failure_leaves_nothing() -> None:
    print("[3] 登记失败 / 写入失败不留可下载残件")
    s = make_settings("failure")
    h = Harness(s)
    await h.start()
    try:
        await h.upload([(TURBINE, ts(8, 0), 1.0), (TURBINE, ts(9, 0), 2.0)])
        try:
            await h.svc.export_training_csv(TURBINE, DAY, fail_after_publish=True)
            check("登记失败时抛出异常", False)
        except RuntimeError:
            check("登记失败时抛出异常", True)
        check("成品已删除，无残留", h.exports_dir_files() == [], str(h.exports_dir_files()))
        tmp_dir = os.path.join(s.exports_dir, "_tmp")
        check("_tmp 无残留", not os.listdir(tmp_dir), str(os.listdir(tmp_dir)))
        check("未登记导出记录", await h.svc.list_exports() == [])

        # 空数据导出：不发布
        try:
            await h.svc.export_training_csv("WTG-NONE", DAY)
            check("无数据导出被拒绝", False)
        except Exception as exc:  # ExportEmpty
            check("无数据导出被拒绝", "没有可导出的读数" in str(exc), str(exc))
        check("空导出未产生文件", h.exports_dir_files() == [])
    finally:
        await h.stop()


async def test_snapshot_isolation() -> None:
    print("[4] 导出期间上传 / 压实：结果只含受理时读数，旧件不被打断")
    s = make_settings("isolation")
    h = Harness(s)
    await h.start()
    try:
        await h.upload([
            (TURBINE, ts(8, 0), 1.0),
            (TURBINE, ts(9, 0), 2.0),
        ])
        await h.upload([
            (TURBINE, ts(10, 0), 3.0),
            (TURBINE, ts(9, 0), 2.0),     # 同刻同值，应与上一批合并
        ])

        # 让导出在读文件前暂停，制造“导出中”窗口
        gate = threading.Event()
        original = ps.read_export_rows

        def delayed(*args, **kwargs):
            gate.wait(timeout=30)
            return original(*args, **kwargs)

        ps.read_export_rows = delayed
        export_task = asyncio.create_task(h.svc.export_training_csv(TURBINE, DAY))
        await asyncio.sleep(0.2)

        # 期间：新上传 + 压实（旧文件被 superseded）
        await h.upload([(TURBINE, ts(11, 0), 4.0)])
        job = await h.compact(DAY)
        check("导出期间压实成功", job["status"] == "published", str(job))
        check("旧件仍在磁盘（导出快照仍引用）",
              len(ps.list_disk_paths(s.lake_dir)) >= 2)

        gate.set()
        r = await asyncio.wait_for(export_task, timeout=30)
        ps.read_export_rows = original
        check("导出源行数 = 受理时 4 行（不含期间上传）", r["source_rows"] == 4, str(r))
        check("导出输出行数 = 3（9 点同值合并）", r["row_count"] == 3, str(r))
        check("来源文件 = 受理时 2 个", r["source_file_count"] == 2, str(r))
        rows = (await h.svc.get_export(r["export_id"]))
        check("登记来源文件 ID 仍为受理时的 2 个", len(rows["source_file_ids"]) == 2)
        # 导出结束后快照释放，旧件可回收；数据仍完整可导出
        for _ in range(40):
            stats = await h.svc.stats()
            if stats["totals"]["superseded_files"] == 0:
                break
            await asyncio.sleep(0.1)
        check("导出结束后旧件被回收", stats["totals"]["superseded_files"] == 0,
              str(stats["totals"]))
        r2 = await h.svc.export_training_csv(TURBINE, DAY)
        check("回收后重新导出为 4 行（含期间上传）", r2["row_count"] == 4, str(r2))
    finally:
        await h.stop()


async def test_restart_persistence_and_cleanup() -> None:
    print("[5] 重启：已发布导出仍可下载，未发布残件被清理")
    s = make_settings("restart")
    h = Harness(s)
    await h.start()
    await h.upload([(TURBINE, ts(8, 0), 1.0), (TURBINE, ts(8, 0), 1.0)])
    r = await h.svc.export_training_csv(TURBINE, DAY)
    # 模拟崩溃现场的未发布残件
    tmp_dir = os.path.join(s.exports_dir, "_tmp")
    with open(os.path.join(tmp_dir, "export-ghost.csv"), "wb") as fh:
        fh.write(b"turbine,ts_utc,vibration,row_count\n")
    with open(os.path.join(s.exports_dir, "export-orphan.csv"), "wb") as fh:
        fh.write(b"orphan\n")
    # 模拟上一个进程遗留的读快照
    snap_id, _ = await h.db.create_snapshot(None, DAY, DAY)
    await h.stop()

    h2 = Harness(s)
    report = await h2.start(recover=True)
    try:
        check("启动清理未发布 _tmp 残件", report["temp_removed"] >= 1, str(report))
        check("启动清理无元数据孤儿 CSV", report["orphans_removed"] == 1, str(report))
        check("遗留读快照被释放（旧件可继续回收）",
              await h2.db.query_one("SELECT 1 FROM snapshots WHERE released_at IS NULL") is None)
        check("已发布导出文件仍在磁盘",
              os.path.isfile(os.path.join(s.exports_dir, r["export_id"] + ".csv")))
        meta = await h2.svc.get_export(r["export_id"])
        check("重启后导出登记仍完整",
              meta["source_rows"] == 2 and meta["row_count"] == 1, str(meta))
        check("重启后列表可见", len(await h2.svc.list_exports()) == 1)
    finally:
        await h2.stop()

    # 再重启一次：稳定
    h3 = Harness(s)
    report3 = await h3.start(recover=True)
    try:
        check("二次重启无残件需清理",
              report3["temp_removed"] == 0 and report3["orphans_removed"] == 0, str(report3))
        check("导出记录稳定", len(await h3.svc.list_exports()) == 1)
    finally:
        await h3.stop()


async def main() -> int:
    if TMP_ROOT.exists():
        shutil.rmtree(TMP_ROOT)
    await test_merge_and_sort()
    await test_conflict_rejected()
    await test_failure_leaves_nothing()
    await test_snapshot_isolation()
    await test_restart_persistence_and_cleanup()
    print("\n" + "=" * 56)
    print(f"通过 {PASSED} 项，失败 {len(FAILED)} 项")
    if FAILED:
        for f in FAILED:
            print("  FAIL:", f)
        return 1
    print("全部断言通过 ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
