"""训练数据导出文件层。

布局：
  <data_dir>/exports/<export_id>.csv        已登记、可下载的导出文件
  <data_dir>/exports/_tmp/<export_id>.csv   尚未登记的临时件（重启清理）

发布顺序：先写临时件并 fsync，再原子改名进入 exports/，最后在单个 DB 事务里
登记导出记录 + 来源文件 ID + 源行数。若登记失败则删除成品；若在改名与登记之间
进程退出，重启恢复会删除没有元数据记录的孤儿 CSV，因此不会留下“可下载残件”。
"""
from __future__ import annotations

import csv
import io
import os
import uuid
from datetime import datetime, timezone

CSV_HEADER = ("turbine", "ts_utc", "vibration", "row_count")


class ExportError(RuntimeError):
    pass


class ExportEmpty(ExportError):
    pass


def new_export_id() -> str:
    return f"export-{uuid.uuid4().hex}"


def ensure_dirs(exports_dir: str) -> None:
    os.makedirs(os.path.join(exports_dir, "_tmp"), exist_ok=True)


def final_path(export_id: str) -> str:
    return f"{export_id}.csv"


def temp_path(export_id: str) -> str:
    return os.path.join("_tmp", f"{export_id}.csv")


def _abs(exports_dir: str, rel_path: str) -> str:
    return os.path.join(exports_dir, rel_path)


def _fsync_dir(path: str) -> None:
    """rename 之后刷目录项，保证断电后的可见性（平台不支持时忽略）。"""
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _format_ts(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def format_ts(ts: datetime) -> str:
    return _format_ts(ts)


def merge_duplicate_timestamps(
    ts_values: list[datetime], vibrations: list[float]
) -> tuple[list[tuple[datetime, float, int]], list[dict]]:
    """按时间排序的输入 -> 合并同刻同值，收集同刻异值冲突。

    返回 (合并行 [(ts, value, 原行数)], 冲突列表)。冲突记录保留全部不同取值。
    """
    merged: list[tuple[datetime, float, int]] = []
    conflicts: list[dict] = []
    i = 0
    n = len(ts_values)
    while i < n:
        j = i + 1
        values = [vibrations[i]]
        while j < n and ts_values[j] == ts_values[i]:
            value = vibrations[j]
            if value not in values:  # 浮点 == 语义；NaN 已在入库前拒绝
                values.append(value)
            j += 1
        if len(values) == 1:
            merged.append((ts_values[i], values[0], j - i))
        else:
            conflicts.append(
                {
                    "ts_utc": _format_ts(ts_values[i]),
                    "values": values,
                    "row_count": j - i,
                }
            )
        i = j
    return merged, conflicts


def build_csv(turbine: str, merged: list[tuple[datetime, float, int]]) -> bytes:
    """生成按时间排序的 CSV；同一时刻同值只保留一行并附原行数。"""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(CSV_HEADER)
    for ts, value, count in merged:
        writer.writerow([turbine, _format_ts(ts), repr(float(value)), count])
    return buf.getvalue().encode("utf-8")


def write_temp(exports_dir: str, export_id: str, payload: bytes) -> str:
    ensure_dirs(exports_dir)
    rel_tmp = temp_path(export_id)
    full = _abs(exports_dir, rel_tmp)
    with open(full, "wb") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    return rel_tmp


def publish_temp(exports_dir: str, export_id: str, rel_tmp: str) -> str:
    rel_final = final_path(export_id)
    os.replace(_abs(exports_dir, rel_tmp), _abs(exports_dir, rel_final))
    _fsync_dir(exports_dir)
    return rel_final


def delete_file(exports_dir: str, rel_path: str) -> None:
    try:
        os.remove(_abs(exports_dir, rel_path))
    except FileNotFoundError:
        pass


def list_temp_paths(exports_dir: str) -> list[str]:
    tmp_dir = os.path.join(exports_dir, "_tmp")
    if not os.path.isdir(tmp_dir):
        return []
    return [
        os.path.join("_tmp", name)
        for name in os.listdir(tmp_dir)
        if name.endswith(".csv")
    ]


def list_disk_paths(exports_dir: str) -> set[str]:
    """exports/ 根目录下的 CSV（不含 _tmp）。"""
    if not os.path.isdir(exports_dir):
        return set()
    return {
        name
        for name in os.listdir(exports_dir)
        if name.endswith(".csv") and os.path.isfile(os.path.join(exports_dir, name))
    }
