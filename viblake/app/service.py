"""业务服务层：把 DB、Parquet、压实、开关串成完整用例。"""
from __future__ import annotations

import asyncio
import math
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from . import export_store as es
from . import parquet_store as ps
from .compaction import CompactionError, CompactionManager
from .config import Settings
from .csv_import import parse_readings
from .db import Database
from .gates import PauseGate


class ExportNotFound(LookupError):
    pass


class ExportRejected(ValueError):
    """导出因同刻异值被拒绝；conflicts/conflict_count 供 HTTP 层返回明细。"""

    def __init__(
        self, conflicts: list[dict], conflict_count: int, truncated: bool = False
    ) -> None:
        self.conflicts = conflicts
        self.conflict_count = conflict_count
        self.truncated = truncated
        super().__init__(
            f"同一时刻存在 {conflict_count} 个不同振动值的时间点，本次不发布导出文件"
        )


class Snapshot:
    """上下文管理器：查询期间固定文件集合，结束即释放引用。"""

    def __init__(self, db: Database, snapshot_id: int, paths: list[str]) -> None:
        self._db = db
        self.snapshot_id = snapshot_id
        self.paths = paths

    async def __aenter__(self) -> "Snapshot":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self._db.release_snapshot(self.snapshot_id)


class DataLakeService:
    def __init__(
        self,
        db: Database,
        settings: Settings,
        gate: PauseGate,
        compaction: CompactionManager,
    ) -> None:
        self._db = db
        self._settings = settings
        self._gate = gate
        self._compaction = compaction
        self._io_pool = ThreadPoolExecutor(max_workers=3, thread_name_prefix="svc-io")
        self._bg: dict[int, asyncio.Task] = {}

    async def close(self) -> None:
        for task in self._bg.values():
            task.cancel()
        self._io_pool.shutdown(wait=False, cancel_futures=True)

    async def _io(self, func, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._io_pool, func, *args)

    # ---------------- 上传 ----------------

    async def upload_csv(self, data: bytes, filename: str) -> dict[str, object]:
        """整批校验通过后，写成一个 Parquet 并登记；保留全部重复行。"""
        readings = parse_readings(data, self._settings)  # 失败直接 422，不产生文件
        day = readings[0].ts_utc.date().isoformat()
        file_id = ps.new_file_id("batch")

        # Parquet 写在 DB 写事务之外
        rel_path = await self._io(ps.write_batch, self._settings.lake_dir, day, file_id, readings)
        try:
            await self._db.insert_file(
                file_id=file_id,
                day=day,
                rel_path=rel_path,
                row_count=len(readings),
                turbines=(r.turbine for r in readings),
            )
        except BaseException:
            # 元数据登记失败：移除磁盘文件，避免成为孤儿
            await self._io(ps.delete_file, self._settings.lake_dir, rel_path)
            raise
        return {
            "file_id": file_id,
            "day": day,
            "path": rel_path,
            "rows": len(readings),
            "turbines": sorted({r.turbine for r in readings}),
            "filename": filename,
        }

    # ---------------- 查询（读快照固定文件集合） ----------------

    async def query(
        self,
        turbine: str | None,
        start: datetime | None,
        end: datetime | None,
        limit: int,
    ) -> dict[str, object]:
        # 暂停开关：在进入快照流程前等待，恢复后照常服务，不丢请求
        await self._gate.wait_query_open()

        limit = max(1, min(int(limit), self._settings.query_limit_max))
        start_day = start.date().isoformat() if start else None
        end_day = (end - timedelta(microseconds=1)).date().isoformat() if end else None

        snapshot_id, files = await self._db.create_snapshot(turbine, start_day, end_day)
        snapshot = Snapshot(self._db, snapshot_id, [f["path"] for f in files])
        try:
            rows, total_matched, scanned = await self._io(
                ps.read_filtered,
                self._settings.lake_dir,
                snapshot.paths,
                turbine,
                start,
                end,
                limit,
            )
        except BaseException:
            await snapshot.__aexit__(None, None, None)
            raise
        else:
            await snapshot.__aexit__(None, None, None)

        return {
            "snapshot_id": snapshot_id,
            "files_in_snapshot": len(files),
            "files_scanned": scanned,
            "matched_rows": total_matched,
            "returned_rows": len(rows),
            "truncated": total_matched > len(rows),
            "limit": limit,
            "rows": rows,
        }

    # ---------------- 训练数据导出 ----------------

    async def export_training_csv(
        self, turbine: str, day: str, *, fail_after_publish: bool = False
    ) -> dict[str, object]:
        """按机组 + UTC 日，从受理时的活动文件生成训练 CSV。

        - 受理时在单个事务内固定文件集合（写读快照），导出全程只读这些文件，
          期间的上传 / 压实不会改变本次结果，旧件回收也不会打断读取；
        - 同一时刻同值合并为一行并记录原行数；同刻异值则整体拒绝、不发布；
        - 文件先写 exports/_tmp 并 fsync，再原子改名，最后在单事务内登记
          文件路径 + 来源文件 ID + 源行数；登记失败则删除成品。
        """
        turbine = turbine.strip()
        if not turbine:
            raise ValueError("机组号不能为空")
        try:
            base = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise ValueError(f"日期格式非法: {day!r}（需 YYYY-MM-DD）") from exc
        # 统一为零填充 ISO 日期，与文件分区 day=YYYY-MM-DD 对齐
        day = base.date().isoformat()
        start = base
        end = base + timedelta(days=1)

        # 暂停查询开关同样适用于导出读取（服务端等待，恢复后继续）
        await self._gate.wait_query_open()

        snapshot_id, files = await self._db.create_snapshot(turbine, day, day)
        try:
            rel_paths = [f["path"] for f in files]
            ts_values, vibrations = await self._io(
                ps.read_export_rows,
                self._settings.lake_dir,
                rel_paths,
                turbine,
                start,
                end,
            )
            merged, conflicts = es.merge_duplicate_timestamps(ts_values, vibrations)
            source_rows = len(ts_values)
            if conflicts:
                limit = self._settings.export_conflict_detail_limit
                raise ExportRejected(
                    conflicts[:limit], len(conflicts), truncated=len(conflicts) > limit
                )
            if not merged:
                raise es.ExportEmpty(
                    f"机组 {turbine} 在 UTC 日 {day} 没有可导出的读数"
                )

            export_id = es.new_export_id()
            payload = es.build_csv(turbine, merged)
            rel_tmp = await self._io(
                es.write_temp, self._settings.exports_dir, export_id, payload
            )
            rel_final = ""
            try:
                rel_final = await self._io(
                    es.publish_temp, self._settings.exports_dir, export_id, rel_tmp
                )
                # 测试钩子：发布后、登记前延时，便于演练“中途退出”留下孤儿 CSV
                if delay_ms := int(os.environ.get("VIBLAKE_EXPORT_DELAY_MS", "0")):
                    await asyncio.sleep(delay_ms / 1000.0)
                if fail_after_publish:
                    raise RuntimeError("模拟登记失败（测试钩子）：删除已写出的成品")
                await self._db.insert_export(
                    export_id=export_id,
                    turbine=turbine,
                    day=day,
                    rel_path=rel_final,
                    row_count=len(merged),
                    source_rows=source_rows,
                    source_file_ids=[f["id"] for f in files],
                )
            except BaseException:
                # 写文件或登记失败 / 中途退出：不留下可下载残件。
                # 若登记其实已经提交（例如提交后收到取消信号），则保留成品，
                # 避免出现“有登记、无文件”的悬挂记录。
                registered = await self._db.get_export(export_id)
                if registered is not None:
                    raise
                if rel_final:
                    await self._io(
                        es.delete_file, self._settings.exports_dir, rel_final
                    )
                await self._io(es.delete_file, self._settings.exports_dir, rel_tmp)
                raise
        finally:
            await self._db.release_snapshot(snapshot_id)

        return {
            "export_id": export_id,
            "turbine": turbine,
            "day": day,
            "row_count": len(merged),
            "source_rows": source_rows,
            "merged_rows": source_rows - len(merged),
            "source_file_ids": [f["id"] for f in files],
            "source_file_count": len(files),
            "download_url": f"/api/exports/{export_id}/download",
        }

    async def list_exports(self) -> list[dict[str, object]]:
        rows = await self._db.list_exports()
        return [
            {
                **dict(r),
                "download_url": f"/api/exports/{r['id']}/download",
            }
            for r in rows
        ]

    async def get_export(self, export_id: str) -> dict[str, object]:
        row = await self._db.get_export(export_id)
        if row is None:
            raise ExportNotFound(export_id)
        return {
            **dict(row),
            "source_file_ids": await self._db.export_source_file_ids(export_id),
            "download_url": f"/api/exports/{export_id}/download",
        }

    def export_abs_path(self, row) -> str:  # noqa: ANN001 - aiosqlite.Row
        return os.path.join(self._settings.exports_dir, row["path"])

    async def recover_exports(self) -> dict[str, int]:
        """启动清理：删除未登记临时件与无元数据的孤儿 CSV（已发布文件保留）。"""
        result = {"temp_removed": 0, "orphans_removed": 0}
        if not os.path.isdir(self._settings.exports_dir):
            return result
        for rel_tmp in await self._io(es.list_temp_paths, self._settings.exports_dir):
            await self._io(es.delete_file, self._settings.exports_dir, rel_tmp)
            result["temp_removed"] += 1
        known = await self._db.all_export_paths()
        for rel in await self._io(es.list_disk_paths, self._settings.exports_dir):
            if rel not in known:
                await self._io(es.delete_file, self._settings.exports_dir, rel)
                result["orphans_removed"] += 1
        return result

    # ---------------- 统计 / 状态 ----------------

    async def stats(self) -> dict[str, object]:
        daily = [dict(r) for r in await self._db.daily_stats()]
        totals = {
            "active_files": sum(int(d["active_files"] or 0) for d in daily),
            "superseded_files": sum(int(d["superseded_files"] or 0) for d in daily),
            "deleted_files": sum(int(d["deleted_files"] or 0) for d in daily),
            "active_rows": sum(int(d["active_rows"] or 0) for d in daily),
            "partitions": len(daily),
        }
        running = await self._db.has_running_compaction()
        return {
            "totals": totals,
            "daily": daily,
            "running_compaction": dict(running) if running else None,
            "gates": self._gate.snapshot(),
        }

    # ---------------- 压实 ----------------

    async def start_compaction(self, day: str) -> tuple[int, dict[str, object]]:
        """启动后台压实任务，立即返回任务 id（HTTP 202）。"""
        if self._gate.merge_paused:
            raise CompactionError("演示开关已暂停合并，请先恢复后再启动压实")
        existing = await self._db.has_running_compaction()
        if existing is not None:
            raise CompactionError(
                f"一次只能压实一份日分区，当前任务 id={existing['id']} day={existing['day']} 仍在运行"
            )
        # 先占位任务行，保证即使尚未被事件循环调度也能在 DB 层面互斥
        compaction_id = await self._db.insert_compaction(day)

        async def runner() -> None:
            try:
                await self._compaction.compact_day(day, compaction_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass  # 失败已在编排层落库（fail_compaction）

        task = asyncio.create_task(runner(), name=f"compact-{day}")
        self._bg[compaction_id] = task
        task.add_done_callback(lambda t: self._bg.pop(compaction_id, None))
        return compaction_id, {"day": day, "compaction_id": compaction_id, "status": "running"}

    async def compaction_status(self, compaction_id: int) -> dict[str, object] | None:
        row = await self._db.get_compaction(compaction_id)
        return dict(row) if row else None

    async def compaction_history(self) -> list[dict[str, object]]:
        return [dict(r) for r in await self._db.list_compactions()]

    # ---------------- 演示开关 ----------------

    async def set_gate(self, kind: str, paused: bool) -> dict[str, object]:
        if kind == "merge":
            self._gate.set_merge_paused(paused)
            await self._db.set_setting("pause_merge", "true" if paused else "false")
        elif kind == "query":
            self._gate.set_query_paused(paused)
            await self._db.set_setting("pause_query", "true" if paused else "false")
        else:
            raise ValueError(f"未知开关: {kind}")
        return self._gate.snapshot()

    # ---------------- 虚构读数 ----------------

    async def generate_demo_csv(
        self,
        rows: int,
        day: str | None,
        turbines: list[str],
    ) -> bytes:
        if not 1 <= rows <= self._settings.max_batch_rows:
            raise ValueError(f"虚构行数需在 1..{self._settings.max_batch_rows} 之间")
        if day is None:
            day = datetime.now(timezone.utc).date().isoformat()
        # 校验日期格式
        datetime.strptime(day, "%Y-%m-%d")

        def _build() -> bytes:
            import csv
            import io
            import random

            rng = random.Random(20261113)
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(["turbine", "ts_utc", "vibration"])
            base = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
            for i in range(rows):
                turbine = turbines[i % len(turbines)]
                ts = base + timedelta(
                    seconds=rng.randint(0, 24 * 3600 - 1),
                    microseconds=rng.randint(0, 999_999),
                )
                # 带机组差异的正弦 + 噪声，保证有限值
                phase = turbines.index(turbine)
                value = (
                    3.0 * math.sin(2 * math.pi * (i % 720) / 720.0 + phase)
                    + rng.gauss(0.0, 0.4)
                )
                writer.writerow(
                    [turbine, ts.isoformat().replace("+00:00", "Z"), f"{value:.4f}"]
                )
            return buf.getvalue().encode("utf-8")

        return await self._io(_build)
