from __future__ import annotations

import json
import os
import threading
import time
from typing import Any, Dict, List, Optional


class EventLog:
    def __init__(self, path: str, max_segment_size: int = 10_000_000):
        self._path = path
        self._max_segment_size = max_segment_size
        self._lock = threading.Lock()
        self._segment_index = 0
        self._current_size = 0
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
        self._index_path = os.path.join(os.path.dirname(path) if os.path.dirname(path) else ".",
                                         os.path.basename(path).replace(".log", "") + "_index.json")
        self._offset = 0
        self._load_index()

    def _log_path(self, segment: Optional[int] = None) -> str:
        seg = segment if segment is not None else self._segment_index
        base = self._path
        dir_name = os.path.dirname(base)
        file_name = os.path.basename(base)
        name, ext = os.path.splitext(file_name)
        seg_name = f"{name}_{seg:06d}{ext}" if ext else f"{name}_{seg:06d}.log"
        return os.path.join(dir_name, seg_name) if dir_name else seg_name

    def _load_index(self) -> None:
        if os.path.exists(self._index_path):
            try:
                with open(self._index_path, "r", encoding="utf-8") as f:
                    idx = json.load(f)
                self._segment_index = idx.get("segment", 0)
                self._offset = idx.get("offset", 0)
                self._current_size = idx.get("size", 0)
            except Exception:
                pass

    def _save_index(self) -> None:
        try:
            with open(self._index_path, "w", encoding="utf-8") as f:
                json.dump({"segment": self._segment_index, "offset": self._offset,
                           "size": self._current_size}, f)
        except Exception:
            pass

    def append(self, record: Any) -> int:
        with self._lock:
            line = json.dumps(record, default=str, sort_keys=True) + "\n"
            line_bytes = line.encode("utf-8")
            if self._current_size + len(line_bytes) > self._max_segment_size and self._current_size > 0:
                self._segment_index += 1
                self._current_size = 0
            path = self._log_path()
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
            offset = self._offset
            self._offset += 1
            self._current_size += len(line_bytes)
            self._save_index()
            return offset

    def append_batch(self, records: List[Any]) -> List[int]:
        offsets = []
        for record in records:
            offsets.append(self.append(record))
        return offsets

    def read(self, offset: int = 0, limit: Optional[int] = None) -> List[Any]:
        records = []
        count = 0
        seg = 0
        global_idx = 0
        while True:
            path = self._log_path(segment=seg)
            if not os.path.exists(path):
                break
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if global_idx >= offset:
                        try:
                            records.append(json.loads(line))
                            count += 1
                        except json.JSONDecodeError:
                            pass
                        if limit is not None and count >= limit:
                            return records
                    global_idx += 1
            seg += 1
        return records

    def read_segment(self, segment: int) -> List[Any]:
        path = self._log_path(segment=segment)
        if not os.path.exists(path):
            return []
        records = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        return records

    @property
    def offset(self) -> int:
        return self._offset

    @property
    def segment_count(self) -> int:
        return self._segment_index + 1

    def truncate(self, before_offset: int) -> None:
        with self._lock:
            seg = 0
            global_idx = 0
            while True:
                path = self._log_path(segment=seg)
                if not os.path.exists(path):
                    break
                seg_records = self.read_segment(seg)
                if not seg_records:
                    seg += 1
                    continue
                keep = []
                for _ in seg_records:
                    if global_idx >= before_offset:
                        keep.append(seg_records[global_idx - (global_idx - len(keep))])
                    global_idx += 1
                if all(global_idx - len(seg_records) + i < before_offset for i in range(len(seg_records))):
                    os.remove(path)
                seg += 1
            self._save_index()

    def compact(self, merge_fn, checkpoint_offset: int) -> None:
        old_records = self.read(0, checkpoint_offset)
        if not old_records:
            return
        merged = merge_fn(old_records)
        self.truncate(checkpoint_offset)
        self.append_batch(merged if isinstance(merged, list) else [merged])