#!/usr/bin/env python3
"""
RaagDosa Files — safe file and folder operations.

Move, copy, rename, checksum, collision detection, path validation.
Used by the main raagdosa.py pipeline for all filesystem operations.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from raagdosa.ui import C, VERBOSE, out


# ─────────────────────────────────────────────────────────────────
# Path utilities
# ─────────────────────────────────────────────────────────────────

def file_mtime(path: Path) -> float:
    """Return file mtime, or 0.0 on error."""
    try:
        return path.stat().st_mtime
    except Exception:
        return 0.0


def sanitize_name(name: str, repl: str = " - ", trim: bool = True) -> str:
    """Strip filesystem-illegal characters from a name."""
    name = re.sub(r"[\/\\]", " ", name)
    name = re.sub(r'[\:\*\?\"\<\>\|]', repl, name)
    name = re.sub(r"\s+", " ", name).strip()
    return name.rstrip(". ").strip() if trim else name


def ensure_dir(path: Path) -> None:
    """Create directory and parents if needed."""
    path.mkdir(parents=True, exist_ok=True)


def check_path_length(path: Path, limit: int = 260) -> bool:
    """True if the full path is within the character limit."""
    return len(str(path)) <= limit


# ─────────────────────────────────────────────────────────────────
# File integrity
# ─────────────────────────────────────────────────────────────────

def file_checksum(path: Path, algo: str = "md5") -> str:
    """Compute a file checksum (default MD5)."""
    h = hashlib.new(algo)
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def get_folder_size(path: Path) -> int:
    """Total size in bytes of all files under a directory."""
    total = 0
    try:
        for p in path.rglob("*"):
            if p.is_file():
                try:
                    total += p.stat().st_size
                except Exception:
                    pass
    except Exception:
        pass
    return total


# ─────────────────────────────────────────────────────────────────
# Lock detection
# ─────────────────────────────────────────────────────────────────

def is_file_locked(path: Path) -> bool:
    """True if the file cannot be opened for writing (locked by another process)."""
    try:
        with path.open("a+b"):
            pass
        return False
    except (IOError, OSError, PermissionError):
        return True


def check_folder_locked(folder: Path, exts: List[str]) -> List[Path]:
    """Return list of locked audio files in a folder."""
    locked: List[Path] = []
    try:
        for p in folder.rglob("*"):
            if p.is_file() and p.suffix.lower() in exts and is_file_locked(p):
                locked.append(p)
    except Exception:
        pass
    return locked


# ─────────────────────────────────────────────────────────────────
# Device detection & timestamp preservation
# ─────────────────────────────────────────────────────────────────

def _same_device(a: Path, b: Path) -> bool:
    """True if two paths are on the same filesystem device.
    Walks up b's ancestry until finding an existing path (dest may not exist yet)."""
    try:
        pb = b
        while not pb.exists():
            parent = pb.parent
            if parent == pb:
                return False  # hit filesystem root
            pb = parent
        return a.stat().st_dev == pb.stat().st_dev
    except Exception:
        return False


def _restore_creation_date(src_stat, target: Path) -> None:
    """Restore the original creation date (birthtime) on macOS using SetFile."""
    if not hasattr(src_stat, "st_birthtime"):
        return
    try:
        import subprocess
        birthtime = dt.datetime.fromtimestamp(src_stat.st_birthtime)
        date_str = birthtime.strftime("%m/%d/%Y %H:%M:%S")
        subprocess.run(["SetFile", "-d", date_str, str(target)],
                       capture_output=True, timeout=5)
    except Exception:
        pass  # best-effort


# ─────────────────────────────────────────────────────────────────
# Safe folder move
# ─────────────────────────────────────────────────────────────────

def safe_move_folder(src: Path, dst: Path, use_checksum: bool = False) -> Tuple[str, float]:
    """
    Move src folder to dst.

    Fast path (same filesystem):
      Uses os.rename() — atomic, ~1ms regardless of folder size.
      No copy, no verify needed (rename is guaranteed consistent by the OS).

    Slow path (cross-device):
      copytree → count+size verify → optional checksum → rmtree.
      Only triggered when source and destination are on different drives.

    Preserves original creation dates and modification times on both paths.

    Returns (method, elapsed_seconds).
    """
    t0 = dt.datetime.now().timestamp()

    # Capture original timestamps before any move
    folder_stat = src.stat()
    file_stats: Dict[str, os.stat_result] = {}
    try:
        for f in src.rglob("*"):
            file_stats[str(f.relative_to(src))] = f.stat()
    except Exception as e:
        out(f"  {C.DIM}Could not pre-read stats: {e}{C.RESET}", level=VERBOSE)

    if _same_device(src, dst):
        # ── Fast path: atomic rename ──────────────────────────────────────
        ensure_dir(dst.parent)
        try:
            src.rename(dst)
        except OSError:
            pass
        else:
            # Restore folder timestamps
            try:
                os.utime(str(dst), (folder_stat.st_atime, folder_stat.st_mtime))
            except Exception:
                pass
            _restore_creation_date(folder_stat, dst)
            # Restore file timestamps
            for rel, fstat in file_stats.items():
                fpath = dst / rel
                if fpath.exists():
                    try:
                        os.utime(str(fpath), (fstat.st_atime, fstat.st_mtime))
                    except Exception:
                        pass
                    _restore_creation_date(fstat, fpath)
            return "rename", dt.datetime.now().timestamp() - t0

    # ── Slow path: copy → verify → delete ────────────────────────────────
    shutil.copytree(str(src), str(dst), copy_function=shutil.copy2)
    sf = sorted([f for f in src.rglob("*") if f.is_file()])
    df = sorted([f for f in dst.rglob("*") if f.is_file()])
    if len(sf) != len(df):
        shutil.rmtree(str(dst), ignore_errors=True)
        raise RuntimeError(f"File count mismatch: src={len(sf)} dst={len(df)}")
    ss = sum(f.stat().st_size for f in sf)
    ds = sum(f.stat().st_size for f in df)
    if ss != ds:
        shutil.rmtree(str(dst), ignore_errors=True)
        raise RuntimeError(f"Size mismatch: src={ss:,} dst={ds:,} bytes")
    if use_checksum:
        for s, d in zip(sf, df):
            if file_checksum(s) != file_checksum(d):
                shutil.rmtree(str(dst), ignore_errors=True)
                raise RuntimeError(f"Checksum mismatch: {s.name}")
    shutil.rmtree(str(src))
    # Restore creation dates on copied folder and files
    try:
        os.utime(str(dst), (folder_stat.st_atime, folder_stat.st_mtime))
    except Exception:
        pass
    _restore_creation_date(folder_stat, dst)
    for rel, fstat in file_stats.items():
        fpath = dst / rel
        if fpath.exists():
            try:
                os.utime(str(fpath), (fstat.st_atime, fstat.st_mtime))
            except Exception:
                pass
            _restore_creation_date(fstat, fpath)
    return "copy", dt.datetime.now().timestamp() - t0


# ─────────────────────────────────────────────────────────────────
# JSON utilities
# ─────────────────────────────────────────────────────────────────

def write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


# ─────────────────────────────────────────────────────────────────
# JSONL utilities
# ─────────────────────────────────────────────────────────────────

def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def iter_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    out_list: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out_list.append(json.loads(line))
            except Exception:
                continue
    return out_list


# ─────────────────────────────────────────────────────────────────
# File classification
# ─────────────────────────────────────────────────────────────────

def is_hidden_file(p: Path) -> bool:
    n = p.name.lower()
    return (n in {".ds_store", "thumbs.db", "desktop.ini", ".localized"}
            or n.startswith("._") or n.startswith("__macosx"))


def list_audio_files(folder: Path, exts: List[str],
                     follow_symlinks: bool = False) -> List[Path]:
    out_f: List[Path] = []
    try:
        for p in folder.iterdir():
            if not follow_symlinks and p.is_symlink():
                continue
            if p.is_file() and p.suffix.lower() in exts and not is_hidden_file(p):
                out_f.append(p)
    except PermissionError:
        pass
    return out_f


# ─────────────────────────────────────────────────────────────────
# Empty parent cleanup
# ─────────────────────────────────────────────────────────────────

def cleanup_empty_parents(start: Path, stop_at: Path) -> None:
    """Remove start and then any empty ancestor directories up to (not including) stop_at."""
    current = start
    while True:
        if current == stop_at or current == current.parent:
            break
        try:
            if current.exists() and not any(current.iterdir()):
                current.rmdir()
                current = current.parent
            else:
                break
        except Exception:
            break
