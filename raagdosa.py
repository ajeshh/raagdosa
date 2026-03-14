#!/usr/bin/env python3
"""
RaagDosa
Deterministic library cleanup for DJ music folders — CLI-first, safe-by-default, undoable.

Commands:
  go / run / scan / apply / folders / tracks / resume
  show / verify / learn / init / status / report
  profile / history / undo / doctor
  orphans / artists / review-list / clean-report
  extract / compare / diff
  tree / catchall / genre / cache
"""
from __future__ import annotations

import argparse, csv, dataclasses, datetime as dt, difflib, fnmatch, hashlib, html
import json, os, platform, re, shutil, signal, sys, unicodedata, uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import yaml
except Exception:
    yaml = None

try:
    from mutagen import File as MutagenFile
except Exception:
    MutagenFile = None

try:
    from PIL import Image as _PILImage
    _HAS_PIL = True
except Exception:
    _PILImage = None
    _HAS_PIL = False

try:
    import readchar as _readchar
    _HAS_READCHAR = True
except Exception:
    _readchar = None
    _HAS_READCHAR = False

try:
    from importlib.metadata import version as _pkg_version
    APP_VERSION = _pkg_version("raagdosa")
except Exception:
    APP_VERSION = "8.0.0"

# ─────────────────────────────────────────────────────────────────
# Hardware performance tiers
# ─────────────────────────────────────────────────────────────────
_PERF_TIERS: Dict[str, Dict[str, int]] = {
    "slow":   {"workers": 1, "lookahead": 1,  "sleep_copy_ms": 50},
    "medium": {"workers": 2, "lookahead": 4,  "sleep_copy_ms": 10},
    "fast":   {"workers": 4, "lookahead": 8,  "sleep_copy_ms": 0},
    "ultra":  {"workers": 8, "lookahead": 16, "sleep_copy_ms": 0},
}

def resolve_perf_settings(cfg: Dict[str, Any], cli_tier: Optional[str] = None) -> Dict[str, Any]:
    """Return resolved performance settings, merging tier defaults with per-key overrides."""
    pc = cfg.get("performance", {})
    tier_name = (cli_tier or pc.get("tier", "medium")).lower()
    if tier_name not in _PERF_TIERS:
        tier_name = "medium"
    base = dict(_PERF_TIERS[tier_name])
    # Per-key overrides win
    if "workers"               in pc: base["workers"]        = int(pc["workers"])
    if "streaming_lookahead"   in pc: base["lookahead"]      = int(pc["streaming_lookahead"])
    if "sleep_between_moves_ms" in pc: base["sleep_copy_ms"] = int(pc["sleep_between_moves_ms"])
    # Legacy: scan.workers still honoured if no performance section
    if "workers" not in (cfg.get("performance") or {}) and "workers" in cfg.get("scan", {}):
        base["workers"] = int(cfg["scan"]["workers"])
    return base

def detect_recommended_tier() -> str:
    """Lightweight hardware heuristic — no psutil required."""
    cores = os.cpu_count() or 1
    ram_gb = 0
    try:
        import platform
        if platform.system() == "Darwin":
            import subprocess as _sp
            r = _sp.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True, timeout=2)
            ram_gb = int(r.stdout.strip()) // 1024 // 1024 // 1024
        elif platform.system() == "Linux":
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal"):
                        ram_gb = int(line.split()[1]) // 1024 // 1024; break
    except Exception:
        pass
    if cores <= 2 or (0 < ram_gb <= 8):  return "slow"
    if cores <= 4 or (0 < ram_gb <= 16): return "medium"
    if cores <= 8 or (0 < ram_gb <= 32): return "fast"
    return "ultra"

# ─────────────────────────────────────────────
# Output / colour / verbosity
# ─────────────────────────────────────────────
_IS_TTY = sys.stdout.isatty() and sys.stderr.isatty()

class C:
    if _IS_TTY:
        RESET="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"
        GREEN="\033[32m"; YELLOW="\033[33m"; RED="\033[31m"
        CYAN="\033[36m"; BLUE="\033[34m"; MAGENTA="\033[35m"
    else:
        RESET=BOLD=DIM=GREEN=YELLOW=RED=CYAN=BLUE=MAGENTA=""

QUIET=0; NORMAL=1; VERBOSE=2
_verbosity=NORMAL

def set_verbosity(v:int)->None:
    global _verbosity; _verbosity=v

def out(msg:str,level:int=NORMAL,file=None)->None:
    if _verbosity>=level: print(msg,file=file or sys.stdout)

def err(msg:str)->None:
    print(f"{C.RED}{msg}{C.RESET}",file=sys.stderr)

def warn(msg:str)->None:
    if _verbosity>=QUIET: print(f"{C.YELLOW}⚠  {msg}{C.RESET}")

def ok_msg(msg:str)->None:
    out(f"{C.GREEN}✓  {msg}{C.RESET}")

def _open_in_finder(path:Path)->None:
    """Open a folder in the system file manager (Finder on macOS)."""
    import subprocess as _sp
    if not path.exists():
        warn(f"Path does not exist: {path}"); return
    system=platform.system()
    try:
        if system=="Darwin":
            _sp.Popen(["open",str(path)])
        elif system=="Linux":
            _sp.Popen(["xdg-open",str(path)])
        elif system=="Windows":
            _sp.Popen(["explorer",str(path)])
        else:
            warn(f"Unsupported platform for open: {system}"); return
        out(f"  {C.CYAN}Opened:{C.RESET} {path.name}")
    except Exception as e:
        err(f"  Could not open folder: {e}")

def _read_key(prompt:str="  > ")->str:
    """Read a single keypress if readchar is available, otherwise fall back to input()."""
    if _HAS_READCHAR and _IS_TTY:
        print(prompt,end="",flush=True)
        try:
            ch=_readchar.readkey()
        except (EOFError,KeyboardInterrupt):
            print(); return "q"
        # Map special keys
        if ch in ("\r","\n"): print(); return ""
        if ch==" ": print("tracks"); return "b"
        print(ch)  # echo the key
        # Preserve case for uppercase-only bindings (R=rescan)
        if ch in ("R",): return ch
        return ch.lower()
    # Fallback: normal input
    try:
        raw=input(prompt).strip()
        # Preserve case for uppercase-only bindings
        if raw in ("R",): return raw
        return raw.lower()
    except (EOFError,KeyboardInterrupt):
        return "q"

def status_tag(dest:str)->str:
    m={"clean":f"{C.GREEN}[CLEAN ]{C.RESET}","review":f"{C.YELLOW}[REVIEW]{C.RESET}","duplicate":f"{C.RED}[DUPE  ]{C.RESET}"}
    return m.get(dest,f"[{dest.upper()[:6]:6}]")

def conf_color(c:float)->str:
    if c>=0.90: return f"{C.GREEN}{c:.2f}{C.RESET}"
    if c>=0.75: return f"{C.YELLOW}{c:.2f}{C.RESET}"
    return f"{C.RED}{c:.2f}{C.RESET}"

# ─────────────────────────────────────────────
# Progress bar
# ─────────────────────────────────────────────
class Progress:
    def __init__(self,total:int,label:str="Scanning"):
        self.total=total; self.current=0; self.label=label
        self._active=_IS_TTY and _verbosity>=NORMAL
        self._lock=Lock()
        self._start=dt.datetime.now()
    def tick(self,msg:str="")->None:
        with self._lock:
            self.current+=1
            if not self._active: return
            pct=int(self.current/max(self.total,1)*100)
            bw=22; filled=int(bw*self.current/max(self.total,1))
            bar="█"*filled+"░"*(bw-filled)
            # ETA calculation
            elapsed=(dt.datetime.now()-self._start).total_seconds()
            rate=self.current/elapsed if elapsed>0 else 0
            remaining=self.total-self.current
            eta_s=remaining/rate if rate>0 else 0
            if eta_s>3600:   eta=f"~{eta_s/3600:.0f}h"
            elif eta_s>60:   eta=f"~{eta_s/60:.0f}m"
            elif eta_s>0:    eta=f"~{eta_s:.0f}s"
            else:            eta=""
            rate_s=f"{rate:.0f}/s" if rate>=1 else f"{rate*60:.0f}/min"
            line=(f"\r{C.CYAN}{self.label}{C.RESET} [{bar}] {self.current}/{self.total} {pct}%"
                  f"  {C.DIM}{rate_s}  {eta}  {msg[:28]:<28}{C.RESET}")
            print(line,end="",flush=True)
    def done(self)->None:
        if self._active:
            elapsed=(dt.datetime.now()-self._start).total_seconds()
            rate=self.current/elapsed if elapsed>0 else 0
            print(f"  {C.DIM}({elapsed:.1f}s, {rate:.0f} folders/s){C.RESET}")

class SizeProgress:
    """Progress bar driven by bytes transferred, not count. More accurate for mixed WAV/MP3."""
    def __init__(self,total_bytes:int,total_count:int,label:str="Moving"):
        self.total_bytes=max(total_bytes,1); self.total_count=total_count
        self.bytes_done=0; self.count_done=0; self.label=label
        self._active=_IS_TTY and _verbosity>=NORMAL
        self._lock=Lock(); self._start=dt.datetime.now()
    def tick(self,nbytes:int,msg:str="")->None:
        with self._lock:
            self.bytes_done+=nbytes; self.count_done+=1
            if not self._active: return
            pct=int(self.bytes_done/self.total_bytes*100)
            bw=22; filled=int(bw*self.bytes_done/self.total_bytes)
            bar="█"*filled+"░"*(bw-filled)
            elapsed=(dt.datetime.now()-self._start).total_seconds()
            rate=self.bytes_done/elapsed if elapsed>0 else 0
            remaining=self.total_bytes-self.bytes_done
            eta_s=remaining/rate if rate>0 else 0
            if eta_s>3600:   eta=f"~{eta_s/3600:.0f}h"
            elif eta_s>60:   eta=f"~{eta_s/60:.0f}m"
            elif eta_s>0:    eta=f"~{eta_s:.0f}s"
            else:            eta=""
            rate_s=f"{_human_size(int(rate))}/s" if rate>=1024 else ""
            line=(f"\r{C.CYAN}{self.label}{C.RESET} [{bar}] {self.count_done}/{self.total_count} {pct}%"
                  f"  {C.DIM}{rate_s}  {eta}  {msg[:28]:<28}{C.RESET}")
            print(line,end="",flush=True)
    def done(self)->None:
        if self._active:
            elapsed=(dt.datetime.now()-self._start).total_seconds()
            print(f"  {C.DIM}({elapsed:.1f}s, {_human_size(self.bytes_done)}){C.RESET}")

# ─────────────────────────────────────────────
# Graceful stop (SIGINT)
# ─────────────────────────────────────────────
_stop_after_current=False
_force_stop=False
_sigint_count=0

def _sigint_handler(sig,frame)->None:
    global _stop_after_current,_force_stop,_sigint_count
    _sigint_count+=1
    if _sigint_count==1:
        _stop_after_current=True
        print(f"\n{C.YELLOW}⚡ Ctrl+C — finishing current folder then stopping. Press again to force quit.{C.RESET}")
    else:
        _force_stop=True
        print(f"\n{C.RED}⚡ Force stop.{C.RESET}"); sys.exit(130)

def register_stop_handler()->None:
    signal.signal(signal.SIGINT,_sigint_handler)

def should_stop()->bool:
    return _stop_after_current or _force_stop

# ─────────────────────────────────────────────
# Core utilities
# ─────────────────────────────────────────────
def now_iso()->str: return dt.datetime.now().isoformat(timespec="seconds")

def slugify(s: str, max_len: int = 24) -> str:
    """Convert a string to a lowercase, hyphen-separated filesystem-safe slug."""
    s = normalize_unicode(s.strip().lower())
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s[:max_len] if len(s) > max_len else s

def make_session_id(profile: str = "", source_folder: str = "", session_name: str = "") -> str:
    """
    Human-readable session ID: YYYY-MM-DD_HH-MM_<name-or-profile>_<source-folder-slug>
    With --session-name: 2026-03-08_14-30_bandcamp-friday
    Without:             2026-03-08_14-30_incoming_slsk-complete-march
    Duplicate-minute collision → appends _2, _3, …  (detected by caller if needed).
    """
    ts = dt.datetime.now().strftime("%Y-%m-%d_%H-%M")
    parts = [ts]
    if session_name:
        parts.append(slugify(session_name, 40))
    else:
        if profile:       parts.append(slugify(profile, 20))
        if source_folder: parts.append(slugify(Path(source_folder).name, 32))
    return "_".join(p for p in parts if p)

def read_yaml(path:Path)->Dict[str,Any]:
    if yaml is None: raise RuntimeError("Missing: pyyaml — pip install pyyaml")
    if not path.exists(): raise FileNotFoundError(f"Config not found: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}

def write_yaml(path:Path,cfg:Dict[str,Any])->None:
    if yaml is None: raise RuntimeError("Missing: pyyaml — pip install pyyaml")
    path.write_text(yaml.safe_dump(cfg,sort_keys=False,allow_unicode=True),encoding="utf-8")

def _deep_merge(base:Dict[str,Any],overlay:Dict[str,Any])->Dict[str,Any]:
    """Recursively merge overlay into base. overlay values win for non-dict leaves."""
    merged=dict(base)
    for k,v in overlay.items():
        if k in merged and isinstance(merged[k],dict) and isinstance(v,dict):
            merged[k]=_deep_merge(merged[k],v)
        else:
            merged[k]=v
    return merged

def load_paths_overlay(cfg_path:Path)->Dict[str,Any]:
    """Load paths.local.yaml if it exists alongside the config file."""
    paths_file=cfg_path.parent/"paths.local.yaml"
    if paths_file.exists():
        return read_yaml(paths_file)
    return {}

_PATH_KEYS_IN_PROFILE={"source_root","clean_mode"}
_PATH_KEYS_TOPLEVEL={"active_profile"}

def _has_paths_in_config(cfg:Dict[str,Any])->bool:
    """Check if config.yaml still contains path-related keys that should be in paths.local.yaml."""
    for pname,pdata in (cfg.get("profiles") or {}).items():
        if isinstance(pdata,dict) and "source_root" in pdata:
            return True
    lcfg=cfg.get("logging",{})
    if lcfg.get("root_dir") and not lcfg.get("_paths_migrated"):
        return True
    return False

def _migrate_paths_to_local(cfg_path:Path,cfg:Dict[str,Any])->None:
    """Offer to extract paths from config.yaml into paths.local.yaml."""
    paths_file=cfg_path.parent/"paths.local.yaml"
    if paths_file.exists():
        return  # already migrated
    if not _has_paths_in_config(cfg):
        return
    out(f"\n{C.YELLOW}v7.0 change:{C.RESET} Paths now belong in {C.BOLD}paths.local.yaml{C.RESET} (keeps config.yaml safe to share).")
    out(f"  Found paths in config.yaml — extracting to paths.local.yaml...")
    paths_data:Dict[str,Any]={}
    # Extract profile paths
    if cfg.get("profiles"):
        paths_data["profiles"]={}
        for pname,pdata in cfg["profiles"].items():
            if isinstance(pdata,dict):
                extracted={}
                for k in list(pdata.keys()):
                    if k in _PATH_KEYS_IN_PROFILE:
                        extracted[k]=pdata[k]
                if extracted:
                    paths_data["profiles"][pname]=extracted
    if cfg.get("active_profile"):
        paths_data["active_profile"]=cfg["active_profile"]
    # Extract logging paths
    lcfg=cfg.get("logging",{})
    log_keys=["root_dir","session_dir","history_log","skipped_log","track_history_log","track_skipped_log"]
    extracted_log={k:lcfg[k] for k in log_keys if k in lcfg}
    if extracted_log:
        paths_data["logging"]=extracted_log
    if paths_data:
        write_yaml(paths_file,paths_data)
        out(f"  {C.GREEN}Created:{C.RESET} {paths_file}")
        out(f"  {C.DIM}Paths in config.yaml still work but are now overridden by paths.local.yaml.{C.RESET}")
        out(f"  {C.DIM}You can safely remove path keys from config.yaml when ready.{C.RESET}\n")

def load_config_with_paths(cfg_path:Path)->Dict[str,Any]:
    """Load config.yaml, then overlay paths.local.yaml, then handle brain→reference migration."""
    cfg=read_yaml(cfg_path)
    # Migrate paths on first v7 run
    _migrate_paths_to_local(cfg_path,cfg)
    # Overlay paths.local.yaml
    paths=load_paths_overlay(cfg_path)
    if paths:
        cfg=_deep_merge(cfg,paths)
    # brain → reference migration (v7.0)
    if "brain" in cfg and "reference" not in cfg:
        cfg["reference"]=cfg.pop("brain")
    elif "brain" in cfg and "reference" in cfg:
        cfg["reference"]=_deep_merge(cfg.pop("brain"),cfg["reference"])
    return cfg

def _validate_config(cfg: Dict[str, Any]) -> None:
    """Validate critical config values at load time."""
    errors: List[str] = []
    rr = cfg.get("review_rules", {})
    for key in ("min_confidence_for_clean", "auto_approve_threshold"):
        val = rr.get(key)
        if val is not None:
            try:
                f = float(val)
                if not 0.0 <= f <= 1.0:
                    errors.append(f"review_rules.{key} must be between 0.0 and 1.0, got {val}")
            except (TypeError, ValueError):
                errors.append(f"review_rules.{key} must be a number, got '{val}'")
    sc = cfg.get("scan", {})
    min_tracks = sc.get("min_tracks")
    if min_tracks is not None:
        try:
            n = int(min_tracks)
            if n < 1:
                errors.append(f"scan.min_tracks must be >= 1, got {min_tracks}")
        except (TypeError, ValueError):
            errors.append(f"scan.min_tracks must be an integer, got '{min_tracks}'")
    profiles = cfg.get("profiles", {})
    for pname, pdata in profiles.items():
        if not isinstance(pdata, dict):
            errors.append(f"profiles.{pname} must be a mapping, got {type(pdata).__name__}")
    if errors:
        err("Config validation failed:")
        for e in errors:
            out(f"  - {e}")
        sys.exit(2)

def _folder_size(path: Path) -> int:
    """Total bytes of all files under *path*."""
    total = 0
    try:
        for f in path.rglob("*"):
            if f.is_file():
                total += f.stat().st_size
    except (OSError, PermissionError):
        pass
    return total

def _human_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"

def ensure_dir(path:Path)->None: path.mkdir(parents=True,exist_ok=True)
def write_json(path:Path,obj:Any)->None: path.write_text(json.dumps(obj,indent=2,ensure_ascii=False),encoding="utf-8")
def read_json(path:Path)->Any: return json.loads(path.read_text(encoding="utf-8"))

def append_jsonl(path:Path,obj:Dict[str,Any])->None:
    ensure_dir(path.parent)
    with path.open("a",encoding="utf-8") as f: f.write(json.dumps(obj,ensure_ascii=False)+"\n")

def iter_jsonl(path:Path)->List[Dict[str,Any]]:
    if not path.exists(): return []
    out_list:List[Dict[str,Any]]=[]
    with path.open("r",encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            try: out_list.append(json.loads(line))
            except Exception: continue
    return out_list

def normalize_unicode(s:str)->str: return unicodedata.normalize("NFC",s or "")
def lower(s:Optional[str])->str: return (s or "").strip().lower()

def is_hidden_file(p:Path)->bool:
    n=p.name.lower()
    return n in {".ds_store","thumbs.db","desktop.ini",".localized"} or n.startswith("._") or n.startswith("__macosx")

# v4.1 — extensions silently skipped during audio scan (default set; override via scan.skip_sidecar_extensions in config)
_SKIP_AUDIO_EXTENSIONS_DEFAULT: Set[str] = {".sfk", ".asd", ".reapeaks", ".pkf", ".db", ".lrc"}
_SKIP_AUDIO_EXTENSIONS: Set[str] = set(_SKIP_AUDIO_EXTENSIONS_DEFAULT)  # merged at runtime by _init_skip_sets

# v4.1 — folder names always skipped during walk (default set; override via scan.skip_system_folders in config)
_SKIP_FOLDER_NAMES_DEFAULT: Set[str] = {"__MACOSX", "__macosx"}
_SKIP_FOLDER_NAMES: Set[str] = set(_SKIP_FOLDER_NAMES_DEFAULT)  # merged at runtime by _init_skip_sets

def _init_skip_sets(cfg: Dict[str, Any]) -> None:
    """Merge config-defined skip lists with hardcoded defaults. Call once after cfg is loaded."""
    global _SKIP_AUDIO_EXTENSIONS, _SKIP_FOLDER_NAMES
    sc = cfg.get("scan", {})
    _SKIP_AUDIO_EXTENSIONS = set(_SKIP_AUDIO_EXTENSIONS_DEFAULT) | {
        e.lower() for e in (sc.get("skip_sidecar_extensions") or [])
    }
    _SKIP_FOLDER_NAMES = set(_SKIP_FOLDER_NAMES_DEFAULT) | set(sc.get("skip_system_folders") or [])

# ── v4.2 filename regexes ────────────────────────────────────────────
# Disc-track compound: "1-01 Title" or "2-07 Title"
_TRACK_DISC_COMPOUND = re.compile(r"^(\d{1})-(\d{2})\s+")
# Vinyl side lettering in filename stem: "A1 - Title" / "b2 - Title"
_TRACK_VINYL_STEM = re.compile(r"^([A-Da-d]\d{1,2})\s*[-–—]?\s+")
# Beatport multi-artist pattern in stem: "Title - Artist1, Artist2 (Mix)"
_BEATPORT_MULTI_ARTIST = re.compile(r"^(.+?)\s+-\s+(.+?,\s*.+?)(?:\s+\(([^)]+)\))?\s*$")
# ── v4.2 filename regexes (see above block) ──

def parse_int_prefix(s:str)->Optional[int]:
    m=re.match(r"^\s*(\d+)",s or "")
    return int(m.group(1)) if m else None

def sanitize_name(name:str,repl:str=" - ",trim:bool=True)->str:
    name=re.sub(r"[\/\\]"," ",name)
    name=re.sub(r'[\:\*\?\"\<\>\|]',repl,name)
    name=re.sub(r"\s+"," ",name).strip()
    return name.rstrip(". ").strip() if trim else name

def folder_mtime(path:Path)->float:
    try: return path.stat().st_mtime
    except Exception: return 0.0

def file_mtime(path:Path)->float:
    try: return path.stat().st_mtime
    except Exception: return 0.0

# ─────────────────────────────────────────────
# Tag cache — persisted between runs
# ─────────────────────────────────────────────
# Cache key: absolute path string
# Cache value: {"mtime": float, "tags": dict}
# Invalidated whenever the file's mtime changes.
# Written atomically after each scan pass.

class TagCache:
    """
    Persistent tag cache. Keyed by absolute path → {mtime, tags}.
    Thread-safe for concurrent reads/writes during parallel scan.
    """
    def __init__(self,cache_path:Path):
        self._path=cache_path
        self._lock=Lock()
        self._dirty=False
        self._data:Dict[str,Any]={}
        self._load()

    def _load(self)->None:
        if not self._path.exists(): return
        try:
            raw=read_json(self._path)
            # Support versioned format
            self._data=raw.get("entries",raw) if isinstance(raw,dict) else {}
        except Exception:
            self._data={}

    def get(self,path:Path)->Optional[Dict[str,Optional[str]]]:
        """Return cached tags if file mtime matches, else None."""
        key=str(path.resolve())
        mtime=file_mtime(path)
        with self._lock:
            entry=self._data.get(key)
        if entry and abs(entry.get("mtime",0)-mtime)<0.01:
            return entry["tags"]
        return None

    def set(self,path:Path,tags:Dict[str,Optional[str]])->None:
        """Store tags for path with current mtime."""
        key=str(path.resolve())
        mtime=file_mtime(path)
        with self._lock:
            self._data[key]={"mtime":mtime,"tags":tags}
            self._dirty=True

    def save(self)->None:
        """Flush cache to disk if dirty. Atomic write via temp file."""
        with self._lock:
            if not self._dirty: return
            tmp=self._path.with_suffix(".tmp")
            try:
                ensure_dir(self._path.parent)
                payload={"version":APP_VERSION,"saved":now_iso(),"entries":self._data}
                tmp.write_text(json.dumps(payload,ensure_ascii=False,separators=(',',':')),encoding="utf-8")
                tmp.replace(self._path)
                self._dirty=False
            except Exception as e:
                try: tmp.unlink(missing_ok=True)
                except Exception: pass

    def evict_missing(self)->int:
        """Remove entries for files that no longer exist. Returns count removed."""
        removed=0
        with self._lock:
            keys=list(self._data.keys())
        for k in keys:
            if not Path(k).exists():
                with self._lock:
                    self._data.pop(k,None); removed+=1
                self._dirty=True
        return removed

    @property
    def size(self)->int:
        with self._lock: return len(self._data)

# Module-level cache singleton — initialised in scan_folders
_tag_cache:Optional["TagCache"]=None

def _get_tag_cache(cfg:Dict[str,Any])->Optional["TagCache"]:
    global _tag_cache
    if _tag_cache is not None: return _tag_cache
    if not cfg.get("scan",{}).get("tag_cache_enabled",True): return None
    cache_path=Path(cfg.get("logging",{}).get("root_dir","logs"))/"tag_cache.json"
    _tag_cache=TagCache(cache_path)
    return _tag_cache

def reset_tag_cache()->None:
    global _tag_cache; _tag_cache=None

# ─────────────────────────────────────────────
# Artist normalisation
# ─────────────────────────────────────────────
def normalize_artist_name(name:str,cfg:Dict[str,Any])->str:
    """
    Full normalisation pipeline:
    1. Unicode NFC
    2. Optional unicode char map  (MØ → MO etc — user-defined)
    3. ALL-CAPS → Title Case
    4. Alias map lookup  (Jay Z / JAYZ / jay-z → Jay-Z)
    5. Hyphen variant normalisation
    6. "The" prefix policy: keep-front | move-to-end | strip
    """
    if not name: return name
    acfg=cfg.get("artist_normalization",{})
    if not acfg.get("enabled",True): return name

    s=normalize_unicode(name.strip())

    # Unicode char map (opt-in, e.g. {"Ø":"O","ø":"o"})
    for src_c,dst_c in (acfg.get("unicode_map",{}) or {}).items():
        s=s.replace(src_c,dst_c)

    # ALL CAPS → Title Case
    words=s.split()
    if words and all(w.isupper() for w in words if len(w)>2):
        small={"a","an","the","and","but","or","for","of","in","at","to","by","vs"}
        s=" ".join(w.capitalize() if i==0 or w.lower() not in small else w.lower() for i,w in enumerate(words))
        words=s.split()

    # all-lowercase → Smart Title Case (same logic used for folder names)
    alpha_words=[w for w in words if any(c.isalpha() for c in w)]
    if alpha_words and all(w.islower() for w in alpha_words):
        s=_smart_title_case_v43(s, cfg)
        words=s.split()

    # Alias map (case-insensitive exact match wins immediately)
    # Check artist_normalization.aliases first, then reference.artist_aliases
    aliases:Dict[str,str]=acfg.get("aliases",{}) or {}
    ref_aliases:Dict[str,str]=cfg.get("reference",{}).get("artist_aliases",{}) or {}
    merged_aliases={**ref_aliases,**aliases}  # artist_normalization wins on conflict
    for alias_key,canonical in merged_aliases.items():
        if alias_key.lower()==s.lower(): return canonical

    # Hyphen variants → ASCII hyphen
    if acfg.get("normalize_hyphens",True):
        s=re.sub(r"[\u2013\u2014\u2010\u2212]","-",s)

    # "The" prefix
    the_policy=acfg.get("the_prefix","keep-front")
    m=re.match(r"^[Tt]he\s+(.+)$",s)
    if m:
        base=m.group(1)
        if the_policy=="move-to-end": s=f"{base}, The"
        elif the_policy=="strip":     s=base
        # keep-front → no change

    return s

def artists_are_same(a:str,b:str,cfg:Dict[str,Any])->bool:
    thresh=float(cfg.get("artist_normalization",{}).get("fuzzy_dedup_threshold",0.92))
    an=normalize_unicode(a.strip().lower()); bn=normalize_unicode(b.strip().lower())
    if an==bn: return True
    ac=re.sub(r"^the\s+","",an); bc=re.sub(r"^the\s+","",bn)
    if ac==bc: return True
    aset=set(ac.split()); bset=set(bc.split())
    if not aset or not bset: return False
    return len(aset&bset)/len(aset|bset)>=thresh

# ─────────────────────────────────────────────
# Windows reserved filename sanitisation
# ─────────────────────────────────────────────
_WINDOWS_RESERVED = {
    "CON","PRN","AUX","NUL",
    "COM1","COM2","COM3","COM4","COM5","COM6","COM7","COM8","COM9",
    "LPT1","LPT2","LPT3","LPT4","LPT5","LPT6","LPT7","LPT8","LPT9",
}

def sanitize_windows_reserved(name:str)->str:
    """Append underscore if name (sans extension) is a Windows reserved word."""
    stem=Path(name).stem.upper()
    if stem in _WINDOWS_RESERVED: return name+"_"
    return name

# ─────────────────────────────────────────────
# Extension case normalisation
# ─────────────────────────────────────────────
def normalise_extension(path:Path)->Optional[Path]:
    """Return new Path with lowercase extension if it needs renaming, else None."""
    if path.suffix and path.suffix != path.suffix.lower():
        return path.with_suffix(path.suffix.lower())
    return None

# ─────────────────────────────────────────────
# Empty parent cleanup
# ─────────────────────────────────────────────
def cleanup_empty_parents(start:Path,stop_at:Path)->None:
    """Remove start and then any empty ancestor directories up to (not including) stop_at."""
    current=start
    while True:
        if current==stop_at or current==current.parent: break
        try:
            if current.exists() and not any(current.iterdir()):
                current.rmdir()
                current=current.parent
            else:
                break
        except Exception:
            break

# ─────────────────────────────────────────────
# Bracket content classifier
# ─────────────────────────────────────────────
_BRACKET_YEAR   = re.compile(r'^(19|20)\d{2}$')
_BRACKET_FORMAT = re.compile(r'\b(mp3|flac|aac|ogg|wav|aiff|320|256|192|128|lossless|hi.?res|24.?bit|vinyl|vinyl.?rip|web|cd|dvd)\b',re.I)
_BRACKET_EDITION= re.compile(r'\b(deluxe|expanded|anniversary|remaster(ed)?|special|bonus|collector|limited|explicit|censored|edition|version|complete|extended|import|original)\b',re.I)
_BRACKET_REMIX  = re.compile(r'\b(remix|edit|mix|rework|bootleg|mashup|vip|flip|dub|club|radio|instrumental|acapella|version)\b',re.I)
_BRACKET_PROMO  = re.compile(r'www\.|\.com|\.net|\.org|free\s+download|promo\s+only|for\s+promo|not\s+for\s+sale|leaked|rip|ripped|uploaded|download',re.I)
_BRACKET_NOISE  = re.compile(r'^\s*(hd|hq|\d+k|\d+\s*hz|\d+\s*kbps|stereo|mono|clean|dirty)\s*$',re.I)

def classify_bracket(text:str)->str:
    """Classify a bracket group's contents: year|format|edition|remix|promo|noise"""
    t=text.strip()
    if _BRACKET_YEAR.match(t):              return "year"
    if _BRACKET_PROMO.search(t):            return "promo"
    if _BRACKET_FORMAT.search(t):           return "format"
    if _BRACKET_EDITION.search(t):          return "edition"
    if _BRACKET_REMIX.search(t):            return "remix"
    if _BRACKET_NOISE.match(t):             return "noise"
    return "unknown"

# ─────────────────────────────────────────────
# Mojibake detection
# ─────────────────────────────────────────────
_MOJIBAKE_CHARS = set('ÃÂ€„†‡ˆ‰Šš›œžŸ¡¢£¤¥¦§¨©ª«¬\xad®¯°±²³´µ¶·¸¹º»¼½¾¿')
_MOJIBAKE_SEQ   = re.compile(r'Ã[\x80-\xff]|Â[\x80-\xff]',re.S)

def detect_mojibake(s:str)->bool:
    """True if the string likely contains double-encoded or misencoded Unicode."""
    if not s: return False
    if _MOJIBAKE_SEQ.search(s): return True
    return sum(1 for c in s if c in _MOJIBAKE_CHARS) >= 3

# ─────────────────────────────────────────────
# Garbage naming detection
# ─────────────────────────────────────────────
_PROMO_WATERMARK = re.compile(
    r'www\.|\.com\b|\.net\b|\.org\b|free\s+download|promo\s+only|not\s+for\s+sale|'
    r'leaked|visit\s+us|check\s+out|follow\s+us|\bvk\.com\b|\bsoundcloud\.com\b',
    re.I)

def detect_garbage_name(name:str)->List[str]:
    """Return list of garbage reasons (empty = clean). Does not modify the name."""
    reasons:List[str]=[]
    # Token flood: 5+ parenthetical/bracket groups
    groups=re.findall(r'[\[\(][^\[\]\(\)]*[\]\)]',name)
    if len(groups)>=5: reasons.append("token_flood")
    # Promo watermarks
    if _PROMO_WATERMARK.search(name): reasons.append("promo_watermark")
    # Mojibake
    if detect_mojibake(name): reasons.append("mojibake")
    # All-caps + very long (shouting noise filenames)
    words=[w for w in name.split() if w.isalpha()]
    if len(words)>=4 and all(w.isupper() for w in words) and len(name)>60:
        reasons.append("all_caps_long")
    return reasons

def strip_bracket_stack(name:str,max_passes:int=8)->str:
    """Repeatedly strip trailing bracket groups classified as noise/promo."""
    result=name.strip()
    for _ in range(max_passes):
        m=re.search(r'\s*[\[\(]([^\[\]\(\)]*)[\]\)]\s*$',result)
        if not m: break
        cls=classify_bracket(m.group(1))
        if cls in ("noise","promo","format"):
            result=result[:m.start()].strip()
        else:
            break
    return result

# ─────────────────────────────────────────────
# Display name noise stripping
# ─────────────────────────────────────────────
_DISPLAY_NOISE_PATTERNS=[
    re.compile(r'\s*\[official\s+(?:audio|video|music\s+video)\]\s*$',re.I),
    re.compile(r'\s*\(official\s+(?:audio|video|music\s+video)\)\s*$',re.I),
    re.compile(r'\s*-\s*official\s+(?:audio|video|music\s+video)\s*$',re.I),
    re.compile(r'\s*\[(?:hd|hq|4k|1080p|720p)\]\s*$',re.I),
    re.compile(r'\s*\((?:lyrics?|lyric\s+video)\)\s*$',re.I),
    re.compile(r'\s*\[(?:lyrics?|lyric\s+video)\]\s*$',re.I),
    re.compile(r'\s*-\s*lyrics?\s*$',re.I),
]

def strip_display_noise(name:str)->str:
    """Strip common display/upload noise suffixes from album/track names."""
    result=name
    for pat in _DISPLAY_NOISE_PATTERNS:
        result=pat.sub('',result).strip()
    return result

# ─────────────────────────────────────────────
# Disc indicator stripping from album names
# ─────────────────────────────────────────────
# Note: Volume/Vol is intentionally NOT stripped — it's part of the album name
# (e.g. "Now That's What I Call Music Volume 2"). Only disc/cd/disk indicators
# are stripped for unified matching of multi-disc albums.
_DISC_INDICATOR=re.compile(r'\s*[-–—:,]\s*(?:disc|cd|disk)\s*\d+\s*$',re.I)
_DISC_INDICATOR2=re.compile(r'\s*\((?:disc|cd|disk)\s*\d+\)\s*$',re.I)
_DISC_INDICATOR3=re.compile(r'\s*\[(?:disc|cd|disk)\s*\d+\]\s*$',re.I)

def strip_disc_indicator(album_name:str)->str:
    """Remove trailing disc/cd indicators from an album name for unified matching.
    Volume/Vol are preserved — they are part of the album identity, not disc markers."""
    s=album_name
    for pat in (_DISC_INDICATOR,_DISC_INDICATOR2,_DISC_INDICATOR3):
        s=pat.sub('',s).strip()
    return s

# ─────────────────────────────────────────────
# Capitalisation pathology fixes
# ─────────────────────────────────────────────
def apply_title_case(s:str,cfg:Dict[str,Any])->str:
    """
    Apply intelligent title-casing with configurable exceptions.
    Handles: ALL CAPS, all lowercase, Every Word Capitalised (over-casing).
    Respects title_case.never_cap and title_case.always_cap lists from config.
    """
    if not s: return s
    tc=cfg.get("title_case",{})
    never:Set[str]={w.lower() for w in (tc.get("never_cap",[]) or [])}
    always:Set[str]={w.lower() for w in (tc.get("always_cap",[]) or [])}

    # Default small words (never-cap unless first word)
    _default_small={"a","an","the","and","but","or","for","nor","on","at","to","by","in","of","vs","via","feat","feat."}
    never=never | _default_small

    words=s.split()
    if not words: return s

    # Detect pathologies
    alpha_words=[w for w in words if any(c.isalpha() for c in w)]
    if not alpha_words: return s
    is_all_caps=all(w.isupper() for w in alpha_words if len(w)>1)
    is_all_lower=all(w.islower() for w in alpha_words)

    if not (is_all_caps or is_all_lower): return s  # already mixed — leave alone

    result=[]
    for i,word in enumerate(words):
        w_lower=word.lower()
        # Strip leading/trailing punct to check the word
        core=re.sub(r'^[^\w]+|[^\w]+$','',word)
        core_lower=core.lower()
        if core_lower in always:
            result.append(word.upper() if len(core)<=3 else word[0:word.index(core)]+core.upper()+word[word.index(core)+len(core):])
        elif i==0 or core_lower not in never:
            # Capitalise
            if core:
                idx=word.index(core)
                result.append(word[:idx]+core[0].upper()+core[1:].lower()+word[idx+len(core):])
            else:
                result.append(word)
        else:
            result.append(word.lower())
    return " ".join(result)

# ─────────────────────────────────────────────
# Vinyl track notation (A1/B2/C3)
# ─────────────────────────────────────────────
_VINYL_RE=re.compile(r'^([A-Da-d])(\d{1,2})$')

def parse_vinyl_track(s:str)->Optional[Tuple[str,int,int]]:
    """
    Parse vinyl side-track notation like A1, B2, C3.
    Returns (side_letter, side_track_num, absolute_track_num) or None.
    Absolute: A1=1, A2=2 … A8=8, B1=9 … B8=16, C1=17 …
    """
    m=_VINYL_RE.match((s or "").strip())
    if not m: return None
    side=m.group(1).upper(); n=int(m.group(2))
    absolute=(ord(side)-ord('A'))*8+n
    return side,n,absolute

# ─────────────────────────────────────────────
# EP detection
# ─────────────────────────────────────────────
_EP_NAME_RE=re.compile(r'(?:^|[\s\[\(])E\.?P\.?(?:$|[\s\]\)])',re.I)

def detect_ep(audio_files:List[Path],cfg:Dict[str,Any],folder_name:str="")->bool:
    """True if track count is in the EP range OR folder/album name contains an EP keyword."""
    ep=cfg.get("ep_detection",{})
    if not ep.get("enabled",True): return False
    mn=int(ep.get("min_tracks",2)); mx=int(ep.get("max_tracks",6))
    if mn<=len(audio_files)<=mx: return True
    # Also detect from folder/album name — catches EPs with non-standard track counts
    if folder_name and _EP_NAME_RE.search(folder_name): return True
    return False

# ─────────────────────────────────────────────
# Mix / chart folder classifier
# ─────────────────────────────────────────────
_MIX_FOLDER_KW=re.compile(
    r'\b(mix(tape)?|presents|sessions?|podcast|promo\s+set|live\s+at|'
    r'compiled\s+by|mixed\s+by|chart|top\s*\d+|best\s+of|'
    r'greatest\s+hits|collection|discography|anthology)\b',re.I)

def classify_folder_content(
    audio_files:List[Path],
    folder_name:str,
    all_tags:List[Dict[str,Optional[str]]],
    cfg:Dict[str,Any]
)->str:
    """
    Returns one of: 'album' | 'va' | 'mix' | 'ep'
    Priority: override > keyword > artist-ratio heuristic
    """
    mc=cfg.get("mix_detection",{}); enabled=mc.get("enabled",True)

    # EP first — check track count and folder name
    if detect_ep(audio_files,cfg,folder_name): return "ep"

    if not enabled: return "album"

    # Folder-name keywords → mix
    extra_kw=[k.lower() for k in (mc.get("folder_name_keywords",[]) or [])]
    if _MIX_FOLDER_KW.search(folder_name) or any(k in folder_name.lower() for k in extra_kw):
        return "mix"

    tagged=[t for t in all_tags if any(v for v in t.values())]
    if not tagged: return "album"

    # High unique-artist ratio — but only classify as mix/VA with strong evidence.
    # v6.1: Also check albumartist — if it's a real artist (not VA keyword), this is
    # a normal album regardless of how many different track artists there are.
    artists=[t.get("artist","").strip().lower() for t in tagged if t.get("artist")]
    albumartists=[t.get("albumartist","").strip().lower() for t in tagged if t.get("albumartist")]
    _va_kw_set={"various artists","various","va","v/a","v.a.","v.a","vvaa","varios artistas","varios","artistes variés","aa.vv.",
                 "разные исполнители","различные исполнители","сборник","verschiedene interpreten","diverse interpreten",
                 "artistas varios","vários artistas","vários intérpretes"}

    # If albumartist is present and NOT a VA keyword, it's a normal album
    if albumartists:
        _aa_dom = Counter(albumartists).most_common(1)[0][0] if albumartists else ""
        if _aa_dom and _aa_dom not in _va_kw_set:
            return "album"

    if len(artists)>=3:
        unique_ratio=len(set(artists))/len(artists)
        # v6.1: raised from 0.65→0.80 to match the conservative VA philosophy
        if unique_ratio>=float(mc.get("unique_artist_ratio_mix",0.80)):
            if albumartists:
                aa_set=set(albumartists)
                if any(x in aa_set for x in _va_kw_set): return "va"
            return "mix"

    return "album"

# ─────────────────────────────────────────────
# Named confidence factors
# ─────────────────────────────────────────────
def detect_track_gaps(track_nums:List[int])->List[int]:
    """Return list of missing track numbers in the sequence."""
    if len(track_nums)<2: return []
    s=sorted(set(track_nums)); gaps=[]
    for i in range(s[0],s[-1]+1):
        if i not in set(s): gaps.append(i)
    return gaps

def detect_duplicate_track_numbers(track_nums:List[int])->List[int]:
    """Return list of track numbers that appear more than once."""
    c=Counter(track_nums)
    return [n for n,cnt in c.items() if cnt>1]

def compute_meaningful_title_ratio(titles:List[str])->float:
    """Song title confidence: fraction of titles that look like real titles vs garbage/missing.
    Also penalises if all titles are identical (likely a tagging error) or too short."""
    if not titles: return 0.5
    meaningful=0
    for t in titles:
        if not t or len(t.strip())<2: continue
        g=detect_garbage_name(t)
        if not g and not detect_mojibake(t): meaningful+=1
    ratio=meaningful/len(titles)
    # Penalise if all titles are identical (copy-paste tags)
    unique_titles=set(t.strip().lower() for t in titles if t and t.strip())
    if len(unique_titles)==1 and len(titles)>2:
        ratio=min(ratio,0.30)  # all tracks same title = likely bad tags
    return ratio

def compute_filename_tag_consistency(
    audio_files:List[Path],
    all_tags:List[Dict[str,Optional[str]]]
)->float:
    """
    Score 0–1 measuring how well filename stem content agrees with tag content.
    Compares the 'Artist - Title' pattern from filenames against tags.
    """
    if not audio_files or not all_tags: return 0.5
    scores:List[float]=[]
    for f,tags in zip(audio_files,all_tags):
        fn_artist,fn_title=_parse_fn_artitle(f.stem)
        tag_title=(tags.get("title") or "").strip()
        tag_artist=(tags.get("artist") or "").strip()
        if fn_title and tag_title:
            scores.append(string_similarity(fn_title,tag_title))
        elif not fn_title and not tag_title:
            scores.append(0.5)
    return sum(scores)/len(scores) if scores else 0.5

def _parse_fn_artitle(stem: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Quick filename stem → (artist, title) for consistency scoring.
    Handles common DJ filename formats:
      "Artist - Title"                          → (artist, title)
      "NN - Artist - Title"                     → (artist, title)
      "NN. Artist - Title"                      → (artist, title)
      "NN. - Artist - Title"                    → (artist, title)   v4.3: dot+dash
      "Artist - Album - NN - Title"             → (artist, title)   4-part
      "Artist - Album - Title"                  → (artist, title)   3-part
    v4.3 additions:
      Hash/checksum tail stripped: "07-track-cd4051c3" → "07-track"
      NNN/NN tag-style track num: "001/12 - Title" → (None, title)
    """
    s = re.sub(r'_-_', ' - ', stem).replace('_', ' ')

    # v4.3: strip trailing hash/checksum (-7c10a753 / _cd4051c3)
    s = _HASH_CHECKSUM_TAIL.sub('', s).strip()

    # v4.3: NNN/NN tag-style track number prefix: "001/12 - Title"
    s = re.sub(r'^\d{1,3}/\d{1,3}\s*[-–—.]\s*', '', s)

    # Strip leading track number prefix: 01 / 01. / 01 - / 01. -
    s = re.sub(r'^\d{1,3}\s*\.?\s*[-–—]?\s*', '', s)

    parts = [p.strip() for p in re.split(r'\s*[-–—]\s*', s) if p.strip()]

    if len(parts) == 1:
        return None, parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    if len(parts) >= 3:
        # "Artist - Album - NN - Title" → (Artist, Title)
        if re.match(r'^\d{1,3}$', parts[2]) and len(parts) >= 4:
            return parts[0], parts[3]
        return parts[0], parts[-1]
    return None, parts[0] if parts else None

def compute_albumartist_consistency(all_tags:List[Dict[str,Optional[str]]])->float:
    """Fraction of tagged tracks sharing the dominant albumartist."""
    tagged=[t for t in all_tags if t.get("albumartist")]
    if not tagged: return 0.5
    c=Counter(t["albumartist"].strip().lower() for t in tagged)
    _,dom_share,_=compute_dominant(c)
    return dom_share

def _tokenise_for_alignment(name:str,noise_tokens:Optional[Set[str]]=None)->List[str]:
    """
    Tokenise a folder/proposed name for alignment comparison.
    1. Lowercase + split on non-alphanumeric separators.
    2. Apply year-anchored cutoff: discard everything after the first 4-digit year
       (removes scene group suffixes like FTD, MFDOS cleanly without a group list).
    3. Strip known noise tokens (format/quality/scene markers).
    """
    import re as _re
    tokens=_re.split(r'[^a-z0-9]+',name.lower())
    tokens=[t for t in tokens if t]
    # Year-anchored cutoff
    cutoff=len(tokens)
    for i,t in enumerate(tokens):
        if _re.fullmatch(r'(?:19|20)\d{2}',t):
            cutoff=i+1; break
    tokens=tokens[:cutoff]
    # Default noise set (augmented by config reference.folder_alignment_noise_tokens)
    default_noise={
        "web","webflac","webrip","flac","mp3","aac","ogg","opus","wav","aiff",
        "cd","cdda","vinyl","dvd","lp","ep","320","256","192","128","v0","v2","vbr",
        "proper","repack","retail","promo","advance","limited","reissue","remaster",
        "remastered","deluxe","expanded","anniversary","nfo","sfv","readnfo",
        "dirfix","nfofix","kbps","khz","lossless","hq",
    }
    effective_noise=(noise_tokens or set()) | default_noise
    return [t for t in tokens if t not in effective_noise and len(t)>1]

def compute_folder_alignment_bonus(folder_name:str,proposed_name:str,cfg:Optional[Dict[str,Any]]=None)->float:
    """
    v2: Token-coverage alignment.
    Measures what fraction of the proposed name's tokens appear in the original
    folder name, after stripping scene noise and applying year-anchored cutoff.

    Artist-prefix adjustment: if proposed has the form "Artist - Album" and the
    artist tokens are absent from the folder (we found the artist from tags), that is
    a good thing — use album-only coverage to avoid penalising clean renames.

    0.0 = no meaningful token overlap (completely different content)
    1.0 = all proposed tokens found in folder (perfect or fully covered)
    """
    fn=normalize_unicode(folder_name.strip().lower())
    pn=normalize_unicode(proposed_name.strip().lower())
    if fn==pn: return 1.0

    # Load config noise tokens if available
    noise_cfg:Set[str]=set()
    if cfg is not None:
        extra=[t.lower() for t in cfg.get("reference",{}).get("folder_alignment_noise_tokens",[]) if isinstance(t,str)]
        noise_cfg=set(extra)

    folder_toks=set(_tokenise_for_alignment(folder_name,noise_cfg))
    proposed_toks=_tokenise_for_alignment(proposed_name,noise_cfg)

    if not proposed_toks:
        return 0.5  # neutral — no meaningful tokens in proposed

    def _coverage(p_toks:List[str])->float:
        if not p_toks: return 0.5
        hits=sum(1 for t in p_toks if t in folder_toks)
        return hits/len(p_toks)

    coverage=_coverage(proposed_toks)

    # Artist-prefix adjustment: if "Artist - Rest" format, also try Rest-only coverage
    if " - " in proposed_name:
        _,rest=proposed_name.split(" - ",1)
        rest_toks=_tokenise_for_alignment(rest,noise_cfg)
        rest_cov=_coverage(rest_toks)
        coverage=max(coverage,rest_cov)

    # Neutral floor for opaque originals (all tokens were noise or too short)
    if not folder_toks:
        return 0.5

    return coverage

def compute_confidence_factors(
    audio_files:List[Path],
    tagged:int,
    alb_share:float,
    aa_share:float,
    art_share:float,
    used_heuristic:bool,
    folder_name:str,
    proposed_name:str,
    all_tags:List[Dict[str,Optional[str]]],
    cfg:Optional[Dict[str,Any]]=None,
)->Dict[str,float]:
    """
    Compute named confidence factors that replace/complement the legacy single float.
    Returns a dict of factor_name → value (0–1 each).
    """
    total=len(audio_files)
    factors:Dict[str,float]={}

    # tag_coverage: what fraction of tracks have any tags
    factors["tag_coverage"]=tagged/total if total else 0.0

    # dominance: core voting quality
    if aa_share>0:
        factors["dominance"]=alb_share*0.60+aa_share*0.40
    else:
        factors["dominance"]=alb_share*0.70+art_share*0.30

    # title_quality: meaningful title ratio
    titles=[t.get("title","") for t in all_tags if t.get("title")]
    factors["title_quality"]=compute_meaningful_title_ratio(titles) if titles else 0.5

    # filename_consistency: filename stems vs tag content
    factors["filename_consistency"]=compute_filename_tag_consistency(audio_files,all_tags)

    # completeness: penalise track-number gaps and duplicates
    track_nums:List[int]=[]
    for t in all_tags:
        raw=t.get("tracknumber") or ""
        # Handle vinyl notation
        vt=parse_vinyl_track(raw.split("/")[0].strip())
        n=vt[2] if vt else parse_int_prefix(raw)
        if n: track_nums.append(n)
    gaps=detect_track_gaps(track_nums)
    dupes=detect_duplicate_track_numbers(track_nums)
    gap_pen=min(0.30,len(gaps)*0.06)
    dup_pen=min(0.20,len(dupes)*0.07)
    factors["completeness"]=max(0.0,1.0-gap_pen-dup_pen)
    factors["track_gaps"]=len(gaps)    # informational integer stored as float
    factors["track_dupes"]=len(dupes)

    # albumartist_consistency
    factors["aa_consistency"]=compute_albumartist_consistency(all_tags)

    # folder_alignment: v2 token-coverage alignment (see compute_folder_alignment_bonus)
    factors["folder_alignment"]=compute_folder_alignment_bonus(folder_name,proposed_name,cfg if cfg is not None else {})

    return factors

def confidence_from_factors(factors:Dict[str,float],used_heuristic:bool)->float:
    """Compute a single 0–1 confidence score from the factor dict."""
    # v7.1: folder_alignment bumped 0.05→0.08 now that v2 token-coverage is reliable.
    # filename_consistency reduced 0.10→0.07 (both measure name/tag agreement).
    weights={"dominance":0.40,"tag_coverage":0.15,"title_quality":0.12,
             "filename_consistency":0.07,"completeness":0.12,
             "aa_consistency":0.06,"folder_alignment":0.08}
    score=sum(factors.get(k,0.5)*w for k,w in weights.items())
    if used_heuristic: score*=0.60
    return min(1.0,max(0.0,score))

# ─────────────────────────────────────────────
# Per-folder .raagdosa override file
# ─────────────────────────────────────────────
def load_folder_override(folder:Path)->Optional[Dict[str,Any]]:
    """
    Load a .raagdosa YAML override from inside a folder.
    Supported keys: name, artist, album, year, skip, force_clean, confidence_boost
    """
    p=folder/".raagdosa"
    if not p.exists(): return None
    if yaml is None: return None
    try:
        data=yaml.safe_load(p.read_text(encoding="utf-8"))
        return data if isinstance(data,dict) else None
    except Exception: return None

# ─────────────────────────────────────────────
# ignore_folder_names glob support
# ─────────────────────────────────────────────
def folder_matches_ignore(folder_name:str,patterns:List[str])->bool:
    """True if folder_name matches any pattern (exact or glob)."""
    for pat in patterns:
        if folder_name==pat or fnmatch.fnmatch(folder_name,pat): return True
    return False

# ─────────────────────────────────────────────
# ─────────────────────────────────────────────
# Library templates — builtin presets
# ─────────────────────────────────────────────
BUILTIN_TEMPLATES:Dict[str,Dict[str,Any]]={
    "standard":      {"name":"Standard Archive",   "template":"{artist}/{album}",
                      "description":"Artist → Album. Safe default for any collection.",
                      "requires":[]},
    "dated":         {"name":"Dated Archive",       "template":"{artist}/{year} - {album}",
                      "description":"Artist → Year - Album. Chronological discography view.",
                      "requires":["year"]},
    "flat":          {"name":"Flat",                "template":"{artist} - {album}",
                      "description":"Artist - Album in one folder. Minimal depth, fast browsing.",
                      "requires":[]},
    "bpm":           {"name":"DJ — BPM Zones",      "template":"{bpm_range}/{artist} - {album}",
                      "description":"BPM range → Artist - Album. Tempo-first for single-genre DJs.",
                      "requires":["bpm"],"note":"Best for track-level use. Album BPM uses average."},
    "genre-bpm":     {"name":"DJ — Genre + BPM",    "template":"{genre}/{bpm_range}/{artist} - {album}",
                      "description":"Genre → BPM → Artist - Album. Open-format DJ structure.",
                      "requires":["genre","bpm"],"note":"Best for track-level use."},
    "genre-bpm-key": {"name":"DJ — Harmonic",       "template":"{genre}/{bpm_range}/{camelot_key}/{artist} - {album}",
                      "description":"Genre → BPM → Key → Artist. Harmonic mixing structure.",
                      "requires":["genre","bpm","key"],"note":"Track-level only. Albums span multiple keys."},
    "genre":         {"name":"Genre Curator",       "template":"{genre}/{artist}/{album}",
                      "description":"Genre → Artist → Album. Best for large multi-genre collections.",
                      "requires":["genre"]},
    "label":         {"name":"Label Archive",       "template":"{label}/{artist} - {album}",
                      "description":"Label → Artist - Album. For label-focused collectors.",
                      "requires":["label"],"note":"Label tag is often unpopulated."},
    "decade":        {"name":"Era / Decade",        "template":"{decade}/{genre}/{artist} - {album}",
                      "description":"Decade → Genre → Artist - Album. Era-first browsing.",
                      "requires":["year","genre"]},
}

# ─────────────────────────────────────────────
# Library path template
# ─────────────────────────────────────────────
def _resolve_lib_cfg(profile:Dict[str,Any],cfg:Dict[str,Any])->Dict[str,Any]:
    """Merge library config: profile-level overrides global, key by key."""
    global_lib=cfg.get("library",{})
    profile_lib=profile.get("library",{})
    if not profile_lib: return dict(global_lib)
    merged=dict(global_lib)
    merged.update(profile_lib)
    return merged

def _derive_decade(year:Optional[int])->str:
    if not year: return ""
    return f"{(year//10)*10}s"

def _normalize_genre(genre:Optional[str],cfg:Dict[str,Any])->str:
    """Normalize a raw genre tag using the genre_map in config, if present."""
    if not genre: return ""
    g=genre.strip()
    genre_map=cfg.get("genre_map",{})
    # Case-insensitive lookup
    for raw,canonical in genre_map.items():
        if g.lower()==raw.lower(): return canonical
    return g

# ── Camelot wheel mapping ─────────────────────────────────────────────────
# Maps raw musical key names to Camelot notation (used by DJs for harmonic mixing)
_CAMELOT_MAP:Dict[str,str]={
    # Minor keys → A column
    "Abm":"1A","G#m":"1A",
    "Ebm":"2A","D#m":"2A",
    "Bbm":"3A","A#m":"3A",
    "Fm":"4A",
    "Cm":"5A",
    "Gm":"6A",
    "Dm":"7A",
    "Am":"8A",
    "Em":"9A",
    "Bm":"10A",
    "F#m":"11A","Gbm":"11A",
    "C#m":"12A","Dbm":"12A",
    # Major keys → B column
    "B":"1B","Cb":"1B",
    "F#":"2B","Gb":"2B",
    "C#":"3B","Db":"3B",
    "Ab":"4B","G#":"4B",
    "Eb":"5B","D#":"5B",
    "Bb":"6B","A#":"6B",
    "F":"7B",
    "C":"8B",
    "G":"9B",
    "D":"10B",
    "A":"11B",
    "E":"12B",
}

def _raw_key_to_camelot(key:Optional[str])->str:
    """Convert a raw musical key tag to Camelot notation. Returns '' if unmapped."""
    if not key: return ""
    k=key.strip()
    # Direct lookup (case-sensitive first for speed)
    if k in _CAMELOT_MAP: return _CAMELOT_MAP[k]
    # Normalize: try common formats — "A minor" → "Am", "C major" → "C"
    k2=re.sub(r"\s*(minor|min)\s*$","m",k,flags=re.IGNORECASE)
    k2=re.sub(r"\s*(major|maj)\s*$","",k2,flags=re.IGNORECASE)
    k2=k2.strip()
    if k2 in _CAMELOT_MAP: return _CAMELOT_MAP[k2]
    # Try capitalizing first letter (e.g. "am" → "Am", "c" → "C")
    k3=k2[0].upper()+k2[1:] if len(k2)>1 else k2.upper()
    if k3 in _CAMELOT_MAP: return _CAMELOT_MAP[k3]
    return ""

def _compute_bpm_range(bpm:Optional[float],cfg:Dict[str,Any])->str:
    """Bucket a BPM value into a range string using config-defined buckets."""
    if not bpm or bpm<=0: return ""
    bpm_cfg=cfg.get("bpm_buckets",{})
    # Check named zones first (order matters)
    named=bpm_cfg.get("named_zones",{})
    for zone_name,bounds in named.items():
        if isinstance(bounds,(list,tuple)) and len(bounds)==2:
            if bounds[0]<=bpm<=bounds[1]:
                return zone_name
    # Fall back to numeric bucket
    width=int(bpm_cfg.get("width",10))
    lo=int(bpm//width)*width
    hi=lo+width-1
    return f"{lo}-{hi}"

def _normalize_label(label:Optional[str],cfg:Dict[str,Any])->str:
    """Normalize a raw label/publisher tag. Strips common suffixes."""
    if not label: return ""
    l=label.strip()
    # Strip common corporate suffixes (loop to handle stacked: "Records LLC")
    for _ in range(3):
        l2=re.sub(r"\s*(Records?|Recordings?|Music|Entertainment|Ltd\.?|Inc\.?|LLC)\s*$","",l,flags=re.IGNORECASE).strip()
        if l2==l: break
        l=l2
    return l

def resolve_library_path(base:Path,artist:str,album:str,year:Optional[int],
                          is_flac_only:bool,is_va:bool,is_single:bool,
                          is_mix:bool,cfg:Dict[str,Any],
                          profile:Optional[Dict[str,Any]]=None,
                          genre:Optional[str]=None,
                          bpm:Optional[float]=None,
                          key:Optional[str]=None,
                          label:Optional[str]=None)->Path:
    lib=_resolve_lib_cfg(profile or {},cfg)
    template=lib.get("template","{artist}/{album}")
    va_folder=lib.get("va_folder","_Various Artists")
    singles_folder=lib.get("singles_folder","_Singles")
    mixes_folder=lib.get("mixes_folder","_Mixes")
    unknown=lib.get("unknown_artist_label","_Unknown")
    flac_seg=bool(lib.get("flac_segregation",False))

    artist_c=sanitize_name(artist or unknown)
    album_c =sanitize_name(album  or "_Untitled")
    album_y =f"{album_c} ({year})" if year else album_c

    if is_mix:    return base/mixes_folder/album_y
    if is_va:     return base/va_folder/album_y
    if is_single: return base/artist_c/singles_folder
    if flac_seg and is_flac_only: return base/artist_c/"FLAC"/album_c

    # Resolve all token values for template substitution
    genre_val=_normalize_genre(genre,cfg)
    decade_val=_derive_decade(year)
    bpm_range_val=_compute_bpm_range(bpm,cfg)
    camelot_val=_raw_key_to_camelot(key)
    label_val=_normalize_label(label,cfg)
    # Fallback labels for missing token data
    genre_fallback=lib.get("genre_fallback","_Unsorted")
    decade_fallback=lib.get("decade_fallback","_Unknown Era")
    bpm_fallback=lib.get("bpm_fallback","_Unknown BPM")
    key_fallback=lib.get("key_fallback","_Unknown Key")
    label_fallback=lib.get("label_fallback","_Unknown Label")

    tokens={
        "artist":artist_c, "album":album_c,
        "year":year or "", "album_year":album_y,
        "genre":genre_val or genre_fallback,
        "decade":decade_val or decade_fallback,
        "bpm_range":bpm_range_val or bpm_fallback,
        "camelot_key":camelot_val or key_fallback,
        "label":label_val or label_fallback,
    }
    try:
        sub=template.format(**tokens)
    except KeyError:
        sub=f"{artist_c}/{album_c}"
    return base/sub

# ─────────────────────────────────────────────
# Config validation
# ─────────────────────────────────────────────
def validate_config(cfg:Dict[str,Any])->List[str]:
    warns:List[str]=[]
    cv=str(cfg.get("app",{}).get("version",""))
    if cv and cv!=APP_VERSION:
        warns.append(f"Config version '{cv}' (script is v{APP_VERSION}). Run 'raagdosa init --update' to review new options.")
    for sec in ["scan","decision","review_rules","year","track_rename","logging"]:
        if sec not in cfg: warns.append(f"Missing section '{sec}' — defaults used.")
    ap=cfg.get("active_profile")
    if not ap: warns.append("active_profile not set.")
    elif ap not in cfg.get("profiles",{}): warns.append(f"active_profile '{ap}' not found.")
    conf=cfg.get("review_rules",{}).get("min_confidence_for_clean",0.85)
    if not(0.0<float(conf)<=1.0): warns.append(f"min_confidence_for_clean={conf} out of range (0,1].")
    return warns

# ─────────────────────────────────────────────
# Manifest — persistent Clean index
# ─────────────────────────────────────────────
def _mfpath(cfg:Dict[str,Any])->Path:
    return Path(cfg.get("logging",{}).get("root_dir","logs"))/"clean_manifest.json"

def read_manifest(cfg:Dict[str,Any])->Dict[str,Any]:
    p=_mfpath(cfg)
    if not p.exists(): return {"version":APP_VERSION,"last_run":None,"entries":{}}
    try: return read_json(p)
    except Exception: return {"version":APP_VERSION,"last_run":None,"entries":{}}

def write_manifest(cfg:Dict[str,Any],m:Dict[str,Any])->None:
    ensure_dir(_mfpath(cfg).parent); write_json(_mfpath(cfg),m)

def manifest_add(cfg:Dict[str,Any],name:str,entry:Dict[str,Any])->None:
    m=read_manifest(cfg); m["entries"][normalize_unicode(name)]={**entry,"committed_at":now_iso()}; write_manifest(cfg,m)

def manifest_has(cfg:Dict[str,Any],name:str)->bool:
    return normalize_unicode(name) in read_manifest(cfg).get("entries",{})

def manifest_set_last_run(cfg:Dict[str,Any])->None:
    m=read_manifest(cfg); m["last_run"]=now_iso(); write_manifest(cfg,m)

def manifest_get_last_run(cfg:Dict[str,Any])->Optional[dt.datetime]:
    ts=read_manifest(cfg).get("last_run")
    if not ts: return None
    try: return dt.datetime.fromisoformat(ts)
    except Exception: return None

# ─────────────────────────────────────────────
# Proposal path validation (anti-traversal)
# ─────────────────────────────────────────────
def validate_proposal_paths(raw_props:List[Dict],allowed_roots:List[Path])->List[str]:
    viols:List[str]=[]; resolved=[r.resolve() for r in allowed_roots]
    for p in raw_props:
        t=Path(p.get("target_path",""))
        try:
            rt=t.resolve()
            if not any(str(rt).startswith(str(r)) for r in resolved):
                viols.append(f"Target escapes allowed roots: {t}")
        except Exception as e:
            viols.append(f"Cannot resolve {t}: {e}")
    return viols

# ─────────────────────────────────────────────
# Disk + file safety
# ─────────────────────────────────────────────
def file_checksum(path:Path,algo:str="md5")->str:
    h=hashlib.new(algo)
    with path.open("rb") as f:
        for chunk in iter(lambda:f.read(65536),b""): h.update(chunk)
    return h.hexdigest()

def get_folder_size(path:Path)->int:
    total=0
    try:
        for p in path.rglob("*"):
            if p.is_file():
                try: total+=p.stat().st_size
                except Exception: pass
    except Exception: pass
    return total

def is_file_locked(path:Path)->bool:
    try:
        with path.open("a+b"): pass
        return False
    except (IOError,OSError,PermissionError): return True

def check_folder_locked(folder:Path,exts:List[str])->List[Path]:
    locked:List[Path]=[]
    try:
        for p in folder.rglob("*"):
            if p.is_file() and p.suffix.lower() in exts and is_file_locked(p): locked.append(p)
    except Exception: pass
    return locked

def check_path_length(path:Path,limit:int=260)->bool: return len(str(path))<=limit

def _same_device(a:Path,b:Path)->bool:
    """True if two paths are on the same filesystem device.
    Walks up b's ancestry until finding an existing path (dest may not exist yet)."""
    try:
        pb=b
        while not pb.exists():
            parent=pb.parent
            if parent==pb: return False  # hit filesystem root
            pb=parent
        return a.stat().st_dev==pb.stat().st_dev
    except Exception:
        return False

def _restore_creation_date(src_stat,target:Path)->None:
    """Restore the original creation date (birthtime) on macOS using SetFile."""
    if not hasattr(src_stat,"st_birthtime"): return
    try:
        import subprocess
        birthtime=dt.datetime.fromtimestamp(src_stat.st_birthtime)
        # SetFile -d expects "MM/DD/YYYY HH:MM:SS"
        date_str=birthtime.strftime("%m/%d/%Y %H:%M:%S")
        subprocess.run(["SetFile","-d",date_str,str(target)],
                      capture_output=True,timeout=5)
    except Exception:
        pass  # best-effort

def safe_move_folder(src:Path,dst:Path,use_checksum:bool=False)->Tuple[str,float]:
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
    t0=dt.datetime.now().timestamp()

    # Capture original timestamps before any move
    folder_stat=src.stat()
    file_stats:Dict[str,os.stat_result]={}
    try:
        for f in src.rglob("*"):
            file_stats[str(f.relative_to(src))]=f.stat()
    except Exception as e:
        out(f"  {C.DIM}Could not pre-read stats: {e}{C.RESET}",level=VERBOSE)

    if _same_device(src,dst):
        # ── Fast path: atomic rename ──────────────────────────────────────
        ensure_dir(dst.parent)
        try:
            src.rename(dst)
        except OSError:
            pass
        else:
            # Restore folder timestamps (rename usually preserves, but be safe)
            try:
                os.utime(str(dst),(folder_stat.st_atime,folder_stat.st_mtime))
            except Exception: pass
            _restore_creation_date(folder_stat,dst)
            # Restore file timestamps
            for rel,fstat in file_stats.items():
                fpath=dst/rel
                if fpath.exists():
                    try: os.utime(str(fpath),(fstat.st_atime,fstat.st_mtime))
                    except Exception: pass
                    _restore_creation_date(fstat,fpath)
            return "rename", dt.datetime.now().timestamp()-t0

    # ── Slow path: copy → verify → delete ────────────────────────────────
    shutil.copytree(str(src),str(dst),copy_function=shutil.copy2)
    sf=sorted([f for f in src.rglob("*") if f.is_file()])
    df=sorted([f for f in dst.rglob("*") if f.is_file()])
    if len(sf)!=len(df):
        shutil.rmtree(str(dst),ignore_errors=True)
        raise RuntimeError(f"File count mismatch: src={len(sf)} dst={len(df)}")
    ss=sum(f.stat().st_size for f in sf); ds=sum(f.stat().st_size for f in df)
    if ss!=ds:
        shutil.rmtree(str(dst),ignore_errors=True)
        raise RuntimeError(f"Size mismatch: src={ss:,} dst={ds:,} bytes")
    if use_checksum:
        for s,d in zip(sf,df):
            if file_checksum(s)!=file_checksum(d):
                shutil.rmtree(str(dst),ignore_errors=True)
                raise RuntimeError(f"Checksum mismatch: {s.name}")
    shutil.rmtree(str(src))
    # Restore creation dates on copied folder and files
    try:
        os.utime(str(dst),(folder_stat.st_atime,folder_stat.st_mtime))
    except Exception: pass
    _restore_creation_date(folder_stat,dst)
    for rel,fstat in file_stats.items():
        fpath=dst/rel
        if fpath.exists():
            try: os.utime(str(fpath),(fstat.st_atime,fstat.st_mtime))
            except Exception: pass
            _restore_creation_date(fstat,fpath)
    return "copy", dt.datetime.now().timestamp()-t0

# ─────────────────────────────────────────────
# DJ database detection
# ─────────────────────────────────────────────
_DJ_PATTERNS_DEFAULT = ["export.pdb","database2","rekordbox.xml","_Serato_","Serato Scratch","Serato DJ","PIONEER"]

def find_dj_databases(source_root:Path, cfg:Dict[str,Any]=None)->List[str]:
    patterns = list((cfg or {}).get("dj_safety", {}).get("database_patterns") or _DJ_PATTERNS_DEFAULT)
    found:List[str]=[]
    for pat in patterns:
        try: matches=list(source_root.rglob(f"*{pat}*"))
        except Exception: matches=[]
        if matches: found.append(f"'{pat}' ({len(matches)})")
    return found

# ─────────────────────────────────────────────
# Log rotation
# ─────────────────────────────────────────────
def rotate_log_if_needed(log_path:Path,max_mb:float=10.0)->None:
    if not log_path.exists(): return
    if log_path.stat().st_size/(1024*1024)>max_mb:
        archive=log_path.with_suffix(f".{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.archive.jsonl")
        log_path.rename(archive)
        out(f"[log rotation] Archived {log_path.name} → {archive.name}",level=VERBOSE)

# ─────────────────────────────────────────────
# Roots
# ─────────────────────────────────────────────
def _rname(profile:Dict,key:str,default:str)->str: return profile.get(key,default)

def derive_wrapper_root(profile:Dict,source_root:Path)->Path:
    """
    The raagdosa wrapper folder — parent of Clean/, Review/, logs/.
    All output lives here so logs and organised music are co-located with the source,
    making it obvious which drive/folder they belong to.
    Config key: profiles.<name>.wrapper_folder_name  (default: 'raagdosa')
    """
    base=source_root.parent if profile.get("clean_mode")=="inside_parent" else source_root
    return base/_rname(profile,"wrapper_folder_name","raagdosa")

def derive_clean_root(profile:Dict,source_root:Path)->Path:
    return derive_wrapper_root(profile,source_root)/_rname(profile,"clean_folder_name","Clean")

def derive_review_root(profile:Dict,source_root:Path)->Path:
    return derive_wrapper_root(profile,source_root)/_rname(profile,"review_folder_name","Review")

def derive_clean_albums_root(profile:Dict,source_root:Path)->Path:
    return derive_clean_root(profile,source_root)/_rname(profile,"clean_albums_folder_name","Albums")

def derive_review_albums_root(profile:Dict,source_root:Path)->Path:
    return derive_review_root(profile,source_root)/_rname(profile,"review_albums_folder_name","Albums")

def derive_duplicates_root(profile:Dict,source_root:Path)->Path:
    return derive_review_root(profile,source_root)/_rname(profile,"duplicates_folder_name","Duplicates")

def ensure_roots(profile:Dict,source_root:Path)->Dict[str,Path]:
    roots={
        "clean_root":    derive_clean_root(profile,source_root),
        "review_root":   derive_review_root(profile,source_root),
        "clean_albums":  derive_clean_albums_root(profile,source_root),
        "clean_tracks":  derive_clean_root(profile,source_root)/_rname(profile,"clean_tracks_folder_name","Tracks"),
        "review_albums": derive_review_albums_root(profile,source_root),
        "duplicates":    derive_duplicates_root(profile,source_root),
        "review_orphans":derive_review_root(profile,source_root)/_rname(profile,"orphans_folder_name","Orphans"),
    }
    for p in roots.values(): ensure_dir(p)
    return roots

def setup_logging_paths(cfg:Dict[str,Any],profile:Dict[str,Any],source_root:Path,profile_name:str="")->None:
    """
    Resolve all logging paths and mutate cfg["logging"] in-place with absolute paths.
    Call once per command after profile and source_root are known.

    v6.1 structure — logs grouped by profile for easy identification:
      logs/
        <profile_name>/              ← e.g. "incoming"
          sessions/
            2026-03-10_11-22/        ← timestamp-only (profile context in path)
              proposals.json
              report.txt / .csv / .html
          trees/
          history.jsonl
          skipped.jsonl
          track-history.jsonl
          track-skipped.jsonl
          tag_cache.json

    If logging.root_dir is absolute (recommended), logs stay in the app directory
    regardless of which source volume is scanned — safe from accidental deletion.
    """
    wrapper=derive_wrapper_root(profile,source_root)
    lcfg=cfg.setdefault("logging",{})
    log_base=wrapper/lcfg.get("root_dir","logs")
    # v6.1: nest under profile name so logs for each source are grouped together
    log_root=log_base/slugify(profile_name,40) if profile_name else log_base
    ensure_dir(log_root)
    # Patch all keys with absolute resolved paths so every downstream caller
    # just does Path(cfg["logging"]["<key>"]) and gets the right location.
    lcfg["root_dir"]          = str(log_root)
    lcfg["session_dir"]       = str(log_root/"sessions")
    lcfg["history_log"]       = str(log_root/"history.jsonl")
    lcfg["skipped_log"]       = str(log_root/"skipped.jsonl")
    lcfg["track_history_log"] = str(log_root/"track-history.jsonl")
    lcfg["track_skipped_log"] = str(log_root/"track-skipped.jsonl")

# ─────────────────────────────────────────────
# Normalise for voting (not for display names)
# ─────────────────────────────────────────────
def normalize_for_vote(s:str,cfg:Dict[str,Any])->str:
    nc=cfg.get("normalize",{})
    o=(s or "").strip()
    if nc.get("lower_case",True): o=o.lower()
    if nc.get("strip_bracketed_phrases_for_voting",True): o=re.sub(r"[\(\[].*?[\)\]]","",o)
    if nc.get("strip_punctuation_for_voting",True): o=re.sub(r"[^\w\s]"," ",o)
    if nc.get("collapse_whitespace",True): o=re.sub(r"\s+"," ",o).strip()
    for suf in nc.get("strip_common_suffixes_for_voting",[]) or []:
        if o.endswith(suf.lower()): o=o[:-len(suf)].strip()
    return o

def string_similarity(a:str,b:str)->float:
    a=re.sub(r"\s+"," ",a.strip().lower()); b=re.sub(r"\s+"," ",b.strip().lower())
    if not a or not b: return 0.0
    if a==b: return 1.0
    aset=set(a.split()); bset=set(b.split())
    j=len(aset&bset)/max(1,len(aset|bset))
    pref=sum(1 for ca,cb in zip(a,b) if ca==cb)
    return max(j,pref/max(len(a),len(b)))

# ─────────────────────────────────────────────
# Tag reading
# ─────────────────────────────────────────────
def mutagen_first(tag_obj:Any,keys:List[str])->Optional[str]:
    if not tag_obj: return None
    for k in keys:
        if k in tag_obj:
            v=tag_obj.get(k)
            if isinstance(v,list): v=v[0] if v else None
            if v is not None: return str(v)
    return None

def read_audio_tags(path:Path,cfg:Dict[str,Any])->Dict[str,Optional[str]]:
    # Cache check — skip mutagen entirely for unchanged files
    cache=_tag_cache
    if cache is not None:
        cached=cache.get(path)
        if cached is not None: return cached

    keys=cfg.get("tags",{})
    result:Dict[str,Optional[str]]={k:None for k in ["album","albumartist","artist","title","tracknumber","discnumber","year","bpm","key","genre","label"]}
    if MutagenFile is None: return result
    try:
        mf=MutagenFile(str(path),easy=True)
        if not mf or not getattr(mf,"tags",None):
            if cache is not None: cache.set(path,result)
            return result
        t=mf.tags
        result["album"]      =mutagen_first(t,keys.get("album_keys",["album"]))
        result["albumartist"]=mutagen_first(t,keys.get("albumartist_keys",["albumartist"]))
        result["artist"]     =mutagen_first(t,keys.get("artist_keys",["artist"]))
        result["title"]      =mutagen_first(t,keys.get("title_keys",["title"]))
        result["tracknumber"]=mutagen_first(t,keys.get("tracknumber_keys",["tracknumber"]))
        result["discnumber"] =mutagen_first(t,keys.get("discnumber_keys",["discnumber"]))
        result["bpm"]        =mutagen_first(t,keys.get("bpm_keys",["bpm","tbpm"]))
        result["key"]        =mutagen_first(t,keys.get("key_keys",["initialkey","key"]))
        result["genre"]      =mutagen_first(t,keys.get("genre_keys",["genre"]))
        result["label"]      =mutagen_first(t,keys.get("label_keys",["organization","label","publisher"]))
        for yk in keys.get("year_keys_prefer",["date","year"]):
            if yk in t:
                yv=t.get(yk); yv=yv[0] if isinstance(yv,list) else yv
                if yv:
                    m=re.search(r"(\d{4})",str(yv))
                    if m: result["year"]=m.group(1); break
    except Exception as e:
        out(f"  {C.DIM}Tag read failed: {e}{C.RESET}",level=VERBOSE)
    if cache is not None: cache.set(path,result)
    return result

def detect_bpm_dj_encoding(stem:str)->bool:
    return bool(re.match(r"^\d{2,3}\s*bpm|^\d{2,3}\s*[-–]\s*[A-Ga-g][#b]?\s*(m|min|maj)?\s*[-–]",stem.strip(),re.IGNORECASE))

# ─────────────────────────────────────────────
# Folder name heuristic parser
# ─────────────────────────────────────────────
# ─────────────────────────────────────────────
# v4.1 — Folder name pre-processor
# ─────────────────────────────────────────────

# Cyrillic → Latin lookalike normalisation table
_CYRILLIC_MAP: Dict[str,str] = {
    "А":"A","В":"B","С":"C","Е":"E","Н":"H","І":"I","К":"K","М":"M",
    "О":"O","Р":"P","Т":"T","Х":"X","а":"a","е":"e","і":"i","о":"o",
    "р":"p","с":"c","х":"x","у":"y",
}

# Country codes to protect from being stripped as catalog IDs
_COUNTRY_CODES: Set[str] = {
    "AU","UK","US","CA","DE","FR","JP","NL","SE","NO","DK","FI","IT",
    "ES","PT","PL","RU","BR","MX","NZ","ZA","BE","CH","AT","IE",
}

# Known iTunes genre bucket names to strip in itunes_hierarchy mode
_ITUNES_GENRE_BUCKETS: Set[str] = {
    "Alternative","Blues","Children's Music","Classical","Comedy","Country",
    "Dance","Electronic","Folk","Hip-Hop/Rap","Holiday","Indie Pop",
    "Jazz","Latin","New Age","Opera","Pop","R&B/Soul","Reggae","Religious",
    "Rock","Singer/Songwriter","Soundtracks","Spoken Word","Vocal","World",
    "Ambient","Bass","Breaks","Deep House","Disco","Drum & Bass","Dub",
    "Dubstep","Electro","Funk","Garage","Grime","Hard Techno","House",
    "Industrial","Jungle","Minimal Techno","Progressive House","Psychedelic",
    "Reggaeton","Rave","Soul","Techno","Trance","Trip Hop","UK Garage",
    "Afrobeats","Afro House","Melodic House & Techno","Organic Electronic",
    "Organic House","Downtempo","Glitch Hop","IDM","Lo-fi","Experimental",
    "Noise","Post-Rock","Shoegaze","Dream Pop","Art Rock","Avant-garde",
    "Contemporary Classical","Electroacoustic","Sound Art","New Wave",
    "Punk","Hardcore","Metal","Grunge","Emo","Post-Punk","Gothic Rock",
    "Psychedelic Rock","Stoner Rock","Doom Metal","Death Metal","Black Metal",
    "Rap","Trap","Cloud Rap","Drill","Grime","UK Drill","Afrorap",
    "Dancehall","Dub Reggae","Rocksteady","Ska","Cumbia","Merengue","Salsa",
    "Bossa Nova","Samba","Forro","Baile Funk","Afropop","Highlife","Afrojuju",
    "Afrobeats","Afrohouse","Amapiano","Gqom",
}

def _normalise_cyrillic_lookalikes(s: str) -> str:
    """Replace Cyrillic characters that look identical to Latin ones."""
    return "".join(_CYRILLIC_MAP.get(c, c) for c in s)

def _strip_catalog_prefix(name: str) -> str:
    """
    Strip catalog-ID-style prefix like 'ANJDEE786D Artist - Album'.
    Heuristic: all-caps run of letters+digits before a recognisable artist/album separator.
    Does NOT strip country codes (AU, UK, US, etc.).
    """
    m = re.match(r"^([A-Z]{2,8}[0-9]{2,8}[A-Z0-9]*)\s+(.+)$", name)
    if m:
        code = m.group(1)
        # Protect country codes (pure alpha, 2 chars)
        if code in _COUNTRY_CODES:
            return name
        return m.group(2).strip()
    return name

def _strip_leading_bracket_catalog(name: str) -> str:
    """
    Strip a leading [CATALOG-CODE] that looks like a label code, not a word.
    Examples stripped:  [HYPE004], [atg030], [bbp012], [basshead001], [sol selectas - sol045]
    NOT stripped: [Deep House], [Human], [FLAC]  — word-only content without digits
    Rule: strip if bracket content contains digits OR is all-caps abbreviation with digits.
    """
    m = re.match(r"^\[([^\]]+)\]\s*(.*)$", name)
    if not m:
        return name
    code, rest = m.group(1).strip(), m.group(2).strip()
    if not rest:   # bracket is the whole name — don't strip
        return name
    # Strip if: contains digits (most label codes do), or looks like pure alphanum code
    has_digits = bool(re.search(r"\d", code))
    is_word_only = bool(re.match(r"^[A-Za-z\s]+$", code)) and len(code.split()) <= 2
    if has_digits and not is_word_only:
        return rest
    # Also strip multi-word catalog codes like "sol selectas - sol045"
    if re.search(r"[A-Za-z]+\d+", code):   # alphanum run like "sol045"
        return rest
    return name

def _strip_bang_delimiters(name: str) -> str:
    """Strip !!! … !!! from start and end, preserving internal !"""
    name = re.sub(r"^!{2,}\s*", "", name)
    name = re.sub(r"\s*!{2,}$", "", name)
    return name.strip()

def _strip_self_released(name: str) -> str:
    """Strip (Selfreleased CD) / (Self Released) / (Self-Released) noise."""
    return re.sub(r"\s*\(self[- ]?released(?:\s+cd)?\)\s*", " ", name, flags=re.IGNORECASE).strip()

def _strip_mashup_keyword(name: str) -> Tuple[str, bool]:
    """Strip MASHUP ALBUM keyword; return (cleaned_name, is_mashup)."""
    if re.search(r'\bmashup\s+album\b', name, re.IGNORECASE):
        cleaned = re.sub(r'\bmashup\s+album\b', '', name, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        return cleaned, True
    return name, False

def _strip_beatport_compilation_noise(name: str) -> str:
    """
    Clean Beatport/Traxsource compilation folder names.
    Handles: dot-separators, trailing underscore dates, 'Best New Hype' prefix,
    dots as word separators, double spaces (Traxsource).
    Examples:
      "Beatport Best New Afro House_ November 2024" → "Beatport Afro House (November 2024)"
      "Beatport.Top.100.Deep.House.2017.07"         → "Beatport Deep House Top 100 (July 2017)"
      "Best new Organic House  Beatport ..."         → "Beatport & Traxsource Organic House Top 100"
    """
    n = name
    # Convert dot-separated names ("Beatport.Top.100.Deep.House.2017.07")
    if re.search(r"^Beatport\.[A-Za-z]", n):
        n = n.replace(".", " ")
    # Strip noise prefixes
    n = re.sub(r"^Best\s+New\s+(?:Hype\s+)?", "", n, flags=re.IGNORECASE)
    # Handle trailing underscore date
    n = _strip_beatport_trailing_date(n)
    # Collapse double+ spaces (Traxsource export artefact)
    n = re.sub(r"  +", " ", n).strip()
    # Strip trailing punctuation/underscore artifacts
    n = n.strip(". _").strip()
    return n

def _is_discography_folder(name: str) -> bool:
    """Detect folders that are artist discography containers."""
    return bool(re.search(r'\b(discography|complete\s+collection|complete\s+works|all\s+albums)\b', name, re.IGNORECASE))

def _strip_bitrate_noise(name: str) -> str:
    """Strip bitrate/format noise from discography folder names."""
    return re.sub(r'\s*[\(\[]\s*(?:\d+\s*kbps|mp3|flac|320|256|192|128|lossless)\s*[\)\]]\s*', ' ', name, flags=re.IGNORECASE).strip()

# ── v4.2 Beatport helpers ──────────────────────────────────────────────────────

def _is_beatport_format(stem: str, folder_name: str = "") -> bool:
    """
    Detect Beatport-exported filenames.  Leading space is the strongest signal
    (194/194 files in real libraries).  Folder name starting with Beatport/
    Traxsource is secondary.  Multi-artist comma pattern is tertiary.
    NEVER invert without one of these signals — false positives are catastrophic.
    """
    if stem != stem.lstrip():          return True   # leading space
    fn_lower = folder_name.lower()
    if fn_lower.startswith(("beatport", "traxsource")): return True
    # Title - Artist1, Artist2 (Mix) where title has no commas and artists section does
    m = _BEATPORT_MULTI_ARTIST.match(stem)
    if m:
        title_part, artists_part = m.group(1), m.group(2)
        if "," in artists_part and "," not in title_part:
            # Extra guard: title part should not look like a track-number prefix
            if not re.match(r"^\d{1,3}[\s\-]", title_part):
                return True
    return False

def _invert_beatport_filename(stem: str, keep_all_artists: bool = False) -> Tuple[str, str, str]:
    """
    Invert Beatport "Title - Artist1, Artist2 (Mix)" → (primary_artist, title, mix_suffix).
    Strips leading space before processing.
    Returns (artist, title, mix_suffix) where mix_suffix includes parentheses e.g. "(Extended Mix)".
    """
    stem = stem.strip()
    # Extract trailing (Mix) if present
    mix_suffix = ""
    m_mix = re.search(r"\s+(\([^)]+(?:mix|remix|edit|version|dub|instrumental|reprise|rework)[^)]*\))\s*$", stem, re.I)
    if m_mix:
        mix_suffix = " " + m_mix.group(1)
        stem = stem[:m_mix.start()].strip()
    # Split on first " - "
    parts = re.split(r"\s+-\s+", stem, maxsplit=1)
    if len(parts) == 2:
        raw_title, raw_artists = parts[0].strip(), parts[1].strip()
    else:
        return stem, "", mix_suffix   # can't parse — return as-is
    # Identify primary artist (first before comma)
    artist_list = [a.strip() for a in raw_artists.split(",")]
    if keep_all_artists:
        primary = ", ".join(artist_list)
    else:
        primary = artist_list[0]
    return primary, raw_title, mix_suffix

def _strip_beatport_trailing_date(name: str) -> str:
    """
    Replace Beatport trailing "_ Month YYYY" or "_ YYYY-MM" date separator.
    Examples:
      "Afro House_ November 2024" → "Afro House (November 2024)"
      "Afro House_ 2024-11"       → "Afro House (November 2024)"
    """
    _MONTHS = ["january","february","march","april","may","june",
               "july","august","september","october","november","december"]
    # "_  Month YYYY"
    m = re.search(r"_\s+(" + "|".join(_MONTHS) + r")\s+(\d{4})\s*$", name, re.IGNORECASE)
    if m:
        return name[:m.start()].strip() + f" ({m.group(1).capitalize()} {m.group(2)})"
    # "_ YYYY-MM"
    m2 = re.search(r"_\s+(\d{4})[.\-](\d{2})\s*$", name)
    if m2:
        yr, mo = int(m2.group(1)), int(m2.group(2))
        if 1 <= mo <= 12:
            mname = ["January","February","March","April","May","June",
                     "July","August","September","October","November","December"][mo-1]
            return name[:m2.start()].strip() + f" ({mname} {yr})"
    return name

# ── v4.3 folder-name pre-processor regexes ───────────────────────────────────
# Scene release group suffix: Artist-Album-WEB-2023-GROUP
_SCENE_RELEASE_SUFFIX = re.compile(
    r'[\s_-]+(WEB|FLAC|MP3|CD|VINYL|DIGITAL|WEB-FLAC|WEB-MP3)'
    r'[-_]+(\d{4})[-_]+[A-Z0-9]{2,12}\s*$', re.IGNORECASE)

# Format in trailing brackets/parens: [FLAC] ( 320 Kbps ) [16Bit-44.1kHz]
_FORMAT_BRACKET_TRAIL = re.compile(
    r'\s*\[\s*(FLAC|MP3|320|256|192|128|VBR|CBR|WEB|16Bit[^\]]*|24Bit[^\]]*'
    r'|lossless|hi.?res|vinyl.?rip)\s*\]\s*$', re.IGNORECASE)
_FORMAT_PAREN_TRAIL = re.compile(
    r'\s*\(\s*(FLAC|MP3|320|256|192|128|VBR|CBR|\d{2,3}\s*[Kk]bps)\s*\)\s*$',
    re.IGNORECASE)

# Catalog code in parens/brackets at end: (CA046) [KOSA043] {MFM031}
_CATALOG_TAIL = re.compile(
    r'\s*[\(\[\{]\s*[A-Z]{1,6}\d{2,6}[A-Z]?\s*[\)\]\}]\s*$')

# Curly brace noise: {Digital Media} {MFM031}
_CURLY_NOISE = re.compile(r'\s*\{[^}]{1,40}\}\s*')

# Duplicate year: "2010 - Río Arriba (2010)" → keep prefix year only
_DUPLICATE_YEAR = re.compile(
    r'^((19|20)\d{2})(.*?)\(\s*\2\d{2}\s*\)\s*$')

# 4-dash label-year-artist-album: "Label - 2024 - Artist - Album"
_LABEL_4DASH = re.compile(
    r'^(?P<label>.+?)\s+-\s+(?P<year>(?:19|20)\d{2})\s+-\s+(?P<artist>.+?)\s+-\s+(?P<album>.+)$')

# Double-dash slug separator: Artist--Album_Name
_DOUBLE_DASH = re.compile(r'--+')

# Mid-name paren year: "Artist - (2017) Album"
_MID_PAREN_YEAR = re.compile(r'\s+-\s+\(((19|20)\d{2})\)\s+')

# Mid-name bracket year (aukai. pattern): "Artist  [2016] Album"
_MID_BRACKET_YEAR = re.compile(
    r'^(?P<pre>.+?)\s{1,2}\[(?P<year>(19|20)\d{2})\]\s*(?:[-\u2013]\s*)?(?P<post>.+)$')

# Hash/checksum tail on track stems: -7c10a753 (7-12 hex chars)
_HASH_CHECKSUM_TAIL = re.compile(r'[-_][0-9a-f]{7,12}$', re.IGNORECASE)

# Trailing CD+bitrate slug noise: "2012 cd 320 tmgk"
_CD_BITRATE_SLUG = re.compile(
    r'\s+(?:cd|cdl?)\s+\d{2,3}(?:\s*kbps?)?\s+[a-z0-9]{2,8}\s*$', re.IGNORECASE)

# Known-label names in trailing brackets: [warp 2008] [strike 45]
_LABEL_BRACKET_TRAIL = re.compile(
    r'\s*\[(?:warp|ninja|brainfeeder|hyperdub|kranky|ghostly|erased\s*tapes|'
    r'zencd|strike|sol\s*selectas|!k7|anticon|mush|def\s*jux|big\s*dada)'
    r'[^\]]{0,30}\]\s*', re.IGNORECASE)

# Tilde separator → normalise to " - "
_TILDE_SEP = re.compile(r'\s*~\s*')

# Trailing type bracket: [Anthology] [album]
_TYPE_BRACKET_TRAIL = re.compile(
    r'\s*\[\s*(anthology|collection|box\s*set|compilation|bootleg|unreleased)\s*\]\s*$',
    re.IGNORECASE)

def _strip_scene_release_suffix(name: str) -> Tuple[str, bool]:
    """Strip scene release group suffix. Returns (cleaned, was_stripped)."""
    # Full slug (all underscores/dashes, no spaces): normalise first
    if '_' in name and ' ' not in name:
        expanded = name.replace('_', ' ')
        m = _SCENE_RELEASE_SUFFIX.search(expanded)
        if m:
            return expanded[:m.start()].strip().rstrip('- '), True
    m = _SCENE_RELEASE_SUFFIX.search(name)
    if m:
        return name[:m.start()].strip().rstrip('-_ '), True
    return name, False

def _normalise_double_dash_slug(name: str) -> str:
    """Convert Artist--Album_Name → Artist - Album Name."""
    if '--' not in name:
        return name
    name = _DOUBLE_DASH.sub(' - ', name)
    name = name.replace('_', ' ')
    return re.sub(r'\s+', ' ', name).strip()

def _extract_mid_paren_year(name: str) -> Tuple[str, Optional[str]]:
    """'Artist - (2017) Album' → ('Artist - Album', '2017')"""
    m = _MID_PAREN_YEAR.search(name)
    if m:
        year = m.group(1)
        cleaned = name[:m.start()] + ' - ' + name[m.end():]
        return re.sub(r'\s+', ' ', cleaned).strip(), year
    return name, None

def _extract_mid_bracket_year(name: str) -> Tuple[str, Optional[str]]:
    """'aukai.  [2016] aukai' → ('aukai. - aukai', '2016')"""
    m = _MID_BRACKET_YEAR.match(name)
    if m:
        pre  = m.group('pre').strip()
        year = m.group('year')
        post = m.group('post').strip()
        return f"{pre} - {post}", year
    return name, None

def _smart_title_case_v43(s: str, cfg: Optional[Dict[str, Any]] = None) -> str:
    """
    v4.3 Smart Title Case — fires only on all-lowercase input.
    Confirmed rule from 147 all-lowercase folders across two library sessions.
    Config toggle: title_case.auto_titlecase_lowercase_folders (default: true).
    """
    if not s:
        return s
    alpha_words = [w for w in s.split() if any(c.isalpha() for c in w)]
    if not alpha_words or not all(w.islower() for w in alpha_words):
        return s  # not all-lowercase — leave as-is

    tc = (cfg or {}).get("title_case", {})
    if not tc.get("auto_titlecase_lowercase_folders", True):
        return s  # user disabled

    _SMALL = {
        "a","an","the","and","but","or","for","nor","of","in","on","at",
        "to","by","up","as","vs","via","feat","feat.","ft.","b/w","vs.",
    }
    never  = _SMALL | {w.lower() for w in (tc.get("never_cap", []) or [])}
    # Always-uppercase short tokens
    _ALWAYS_UPPER = {"dj","mc","ep","lp","va","uk","us","la","nyc","ny","ii","iii","iv","vi"}
    always = _ALWAYS_UPPER | {w.lower() for w in (tc.get("always_cap", []) or [])}

    words  = s.split()
    result = []
    for i, word in enumerate(words):
        m = re.match(r'^([^\w]*)(.+?)([^\w]*)$', word)
        if not m:
            result.append(word); continue
        lead, core, trail = m.group(1), m.group(2), m.group(3)
        core_lower = core.lower()
        is_first = (i == 0)
        is_last  = (i == len(words) - 1)
        if core_lower in always:
            result.append(lead + core.upper() + trail)
        elif is_first or is_last or core_lower not in never:
            result.append(lead + core[0].upper() + core[1:] + trail)
        else:
            result.append(lead + core_lower + trail)
    return " ".join(result)

def _detect_label_as_albumartist(albumartist: str) -> bool:
    """
    True if the albumartist tag looks like a record label, not an artist name.
    Pattern confirmed in 37 folders (3.7%) from session 2 (Tropical Twista Records cluster).
    """
    if not albumartist:
        return False
    return bool(re.search(
        r'\b(Records?|Discos?|Recordings?|Label|Music\s+Group|Inc\.?|Ltd\.?|Disques?)\b',
        albumartist, re.IGNORECASE))


def apply_v41_folder_pre_processor(name: str, cfg: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """
    v4.3 folder name pre-processor (supersedes v4.1, backwards-compatible).
    Returns (cleaned_name, metadata_dict) where metadata may contain:
      year, folder_type, extracted_label, noise_stripped flags.
    Steps 1-13 are the v4.1 pipeline; steps 14-27 are new v4.3 patterns
    confirmed across 2,000+ real folders in two library sessions.
    """
    meta: Dict[str, Any] = {}
    nc = cfg.get("title_cleanup", {})

    # ── v4.1 pipeline ──────────────────────────────────────────────────────

    # 1. Cyrillic lookalike normalisation
    name = _normalise_cyrillic_lookalikes(name)

    # 2. Double-space collapse (Traxsource artefact)
    name = re.sub(r"  +", " ", name).strip()

    # 3. URL-decode
    if nc.get("url_decode", True):
        import urllib.parse
        name = urllib.parse.unquote(name)

    # 4. Strip leading macOS resource-fork prefix (._)
    if name.startswith("._"):
        name = name[2:]

    # 5. Strip !!! … !!! bang delimiters
    if nc.get("strip_bang_delimiters", True):
        name = _strip_bang_delimiters(name)

    # 6. Strip catalog ID prefix (ANJDEE786D Artist - Album)
    name = _strip_catalog_prefix(name)

    # 7. Strip leading [bracket] catalog code
    name = _strip_leading_bracket_catalog(name)

    # 8. Strip (Self Released) / (Selfreleased CD) edition noise
    name = _strip_self_released(name)

    # 9. MASHUP ALBUM keyword
    name, is_mashup = _strip_mashup_keyword(name)
    if is_mashup:
        meta["folder_type"] = "mashup"

    # 10. Beatport compilation noise
    name = _strip_beatport_compilation_noise(name)

    # 11. Discography container detection + bitrate noise
    if _is_discography_folder(name):
        meta["folder_type"] = "discography"
        name = _strip_bitrate_noise(name)

    # 12. Year-dot separator: "Artist - 2011. Album" → year=2011, album=Album
    m = re.match(r"^(.+?)\s*-\s*((?:19|20)\d{2})\.\s*(.+)$", name)
    if m:
        meta["year"] = m.group(2)
        name = f"{m.group(1).strip()} - {m.group(3).strip()}"

    # 13. Normalise spaces after v4.1 stripping
    name = re.sub(r"\s+", " ", name).strip()

    # ── v4.3 pipeline ──────────────────────────────────────────────────────

    # 14. Scene release group suffix: Artist-Album-WEB-2023-GROUP
    name, was_scene = _strip_scene_release_suffix(name)
    if was_scene:
        meta["noise_scene_stripped"] = True

    # 15. Double-dash slug: Artist--Album_Name → Artist - Album Name
    if '--' in name:
        name = _normalise_double_dash_slug(name)

    # 16. Tilde separator ~ → " - "
    name = _TILDE_SEP.sub(' - ', name)

    # 17. Curly brace noise blocks: {Digital Media} {MFM031}
    name = _CURLY_NOISE.sub(' ', name).strip()

    # 18. Known-label bracket at end: [warp 2008] [strike 45]
    name = _LABEL_BRACKET_TRAIL.sub('', name).strip()

    # 19. Trailing type annotation: [Anthology] [album]
    name = _TYPE_BRACKET_TRAIL.sub('', name).strip()

    # 20. Format bracket/paren noise: [FLAC] ( 320 Kbps ) [16Bit-44.1kHz]
    for _ in range(4):  # may stack: "(EP) ( FLAC ) [320]"
        prev = name
        name = _FORMAT_BRACKET_TRAIL.sub('', name).strip()
        name = _FORMAT_PAREN_TRAIL.sub('', name).strip()
        if name == prev:
            break

    # 21. Trailing CD+bitrate slug noise: "2012 cd 320 tmgk"
    name = _CD_BITRATE_SLUG.sub('', name).strip()

    # 22. Trailing catalog code in parens/brackets/curly: (CA046) [KOSA043]
    name = _CATALOG_TAIL.sub('', name).strip()

    # 23. Duplicate year: "2010 - Río Arriba (2010)" → keep prefix year
    dm = _DUPLICATE_YEAR.match(name)
    if dm:
        year_prefix = dm.group(1)
        rest = dm.group(3).strip().rstrip('-–').strip()
        if not meta.get("year"):
            meta["year"] = year_prefix
        name = f"{year_prefix} - {rest}" if '-' not in rest[:3] else f"{year_prefix}{rest}"
        name = re.sub(r"\s+", " ", name).strip()

    # 24. Mid-name paren year: "Artist - (2017) Album" → year extracted, moved
    name, paren_year = _extract_mid_paren_year(name)
    if paren_year and not meta.get("year"):
        meta["year"] = paren_year

    # 25. Mid-name bracket year (aukai. pattern): "Artist  [2016] Album"
    name, bracket_year = _extract_mid_bracket_year(name)
    if bracket_year and not meta.get("year"):
        meta["year"] = bracket_year

    # 26. 4-dash label-year-artist-album pattern:
    #     "Cosmovision Records - 2024 - VA - Electropical"
    # Guard: only fires when the first segment looks like a label (contains a label keyword
    # OR is present in reference.known_labels). Prevents false positives on Artist-Year-Artist-Album.
    lm = _LABEL_4DASH.match(name)
    if lm:
        label   = lm.group("label").strip()
        year    = lm.group("year")
        artist  = lm.group("artist").strip()
        album   = lm.group("album").strip()
        known_labels = {lb.lower() for lb in (
            (cfg or {}).get("reference", {}).get("known_labels", []) or [])}
        label_is_label = (
            _detect_label_as_albumartist(label)
            or label.lower() in known_labels
        )
        if label_is_label:
            meta["extracted_label"] = label
            if not meta.get("year"):
                meta["year"] = year
            # Reassemble as standard artist - year - album
            name = f"{artist} - {year} - {album}"

    # 27. Final space normalisation and cleanup
    name = re.sub(r"\s+", " ", name).strip()
    # Strip orphaned trailing separators left by aggressive stripping
    name = name.rstrip("-– ").strip()
    # Strip orphaned trailing year digits (e.g. "Album remixed2012" → "Album remixed")
    # Only when immediately abutted to the previous word (no space before the year)
    name = re.sub(r'([a-z])(?:19|20)\d{2}\s*$', r'\1', name, flags=re.IGNORECASE).strip()
    # Final trailing separator cleanup after year strip
    name = name.rstrip("-– ").strip()
    name = re.sub(r"\s+", " ", name).strip()

    return name, meta

# ─────────────────────────────────────────────
# Folder name heuristic parser
# ─────────────────────────────────────────────
def smart_title_case(s: str, cfg: Optional[Dict[str, Any]] = None) -> str:
    """
    Intelligent title case — handles both ALL CAPS and all-lowercase inputs.
    v4.3: all-lowercase support confirmed across 147 real library folders.
    Delegates to _smart_title_case_v43 for lowercase; handles ALL CAPS inline.
    """
    if not s:
        return s
    words = s.split()
    alpha_words = [w for w in words if any(c.isalpha() for c in w)]
    if not alpha_words:
        return s
    # ALL CAPS path (pre-existing behaviour)
    if all(w.isupper() for w in alpha_words if len(w) > 1):
        small = {"a","an","the","and","but","or","for","nor","on","at","to","by","in","of","vs"}
        return " ".join(w.capitalize() if i==0 or w.lower() not in small else w.lower()
                        for i, w in enumerate(words))
    # All-lowercase path (v4.3 new)
    if all(w.islower() for w in alpha_words):
        return _smart_title_case_v43(s, cfg)
    return s

def parse_folder_name_heuristic(folder_name:str, cfg:Optional[Dict[str,Any]]=None)->Dict[str,Optional[str]]:
    result:Dict[str,Optional[str]]={"artist":None,"album":None,"year":None}
    if cfg is None: cfg = {}

    # v4.1 pre-processor: apply naming pattern fixes before parsing
    cleaned, meta = apply_v41_folder_pre_processor(folder_name, cfg)
    # Propagate year extracted by pre-processor
    if meta.get("year"):
        result["year"] = meta["year"]

    # v4.3: apply Smart Title Case to all-lowercase folder names before parsing
    cleaned = smart_title_case(cleaned, cfg)

    name=normalize_unicode(cleaned.strip())
    name=re.sub(r"\s*\[[A-Z0-9\.\s]+\]\s*$","",name,flags=re.IGNORECASE).strip()
    name=re.sub(r"_-_"," - ",name).replace("_"," ")
    name=re.sub(r"\s+"," ",name).strip()
    ym=re.search(r"\b(19\d{2}|20\d{2})\b",name)
    if ym and not result["year"]: result["year"]=ym.group(1)
    name_ny=re.sub(r"[\(\[]\s*(19\d{2}|20\d{2})\s*[\)\]]","",name).strip()
    name_ny=re.sub(r"\b(19\d{2}|20\d{2})\b","",name_ny).strip()
    name_ny=re.sub(r"\s+"," ",name_ny).strip().rstrip("-–").strip()
    m=re.match(r"^[\[\(]?(19\d{2}|20\d{2})[\]\)]?\s*[-–]\s*(.+?)\s*[-–]\s*(.+)$",name)
    if m:
        if not result["year"]: result["year"]=m.group(1)
        result["artist"]=smart_title_case(m.group(2).strip())
        result["album"]=smart_title_case(re.sub(r"\s*[\(\[](19\d{2}|20\d{2})[\)\]]\s*$","",m.group(3)).strip()); return result
    m2=re.match(r"^(.+?)\s*[-–]\s*(.+)$",name_ny)
    if m2:
        result["artist"]=smart_title_case(m2.group(1).strip())
        result["album"]=smart_title_case(m2.group(2).strip().rstrip("-– ")); return result
    if name_ny: result["album"]=smart_title_case(name_ny)
    return result

# ─────────────────────────────────────────────
# Folder scanning + proposals
# ─────────────────────────────────────────────
@dataclasses.dataclass
class FolderStats:
    tracks_total:int; tracks_tagged:int; tracks_unreadable:int
    extensions:Dict[str,int]; format_duplicates:List[str]=dataclasses.field(default_factory=list)

@dataclasses.dataclass
class FolderProposal:
    folder_path:str; folder_name:str; proposed_folder_name:str
    target_path:str; destination:str; confidence:float
    decision:Dict[str,Any]; stats:FolderStats

def fp_from_dict(d:Dict[str,Any])->FolderProposal:
    sd=d.get("stats",{})
    stats=FolderStats(tracks_total=sd.get("tracks_total",0),tracks_tagged=sd.get("tracks_tagged",0),
                      tracks_unreadable=sd.get("tracks_unreadable",0),extensions=sd.get("extensions",{}),
                      format_duplicates=sd.get("format_duplicates",[])) if isinstance(sd,dict) else sd
    return FolderProposal(folder_path=d["folder_path"],folder_name=d["folder_name"],
                          proposed_folder_name=d["proposed_folder_name"],target_path=d["target_path"],
                          destination=d["destination"],confidence=d["confidence"],
                          decision=d["decision"],stats=stats)


# ─────────────────────────────────────────────────────────────────
# Artifact classification
# ─────────────────────────────────────────────────────────────────

def _cue_has_paired_audio(path:Path)->bool:
    """Return True if a .cue sheet has a matching .flac or .ape file alongside."""
    stem=path.stem
    for ext in (".flac",".ape",".wav"):
        if (path.parent/(stem+ext)).exists():
            return True
    return False

def classify_artifacts(folder:Path, cfg:Dict[str,Any])->Dict[str,List[Path]]:
    """
    Classify all non-audio, non-hidden files in folder into:
        keep       — moves with the album (jpg/jpeg/pdf/qualifying cue)
        quarantine — sent to Review/Artifacts/<folder_name>/
        ignore     — hidden/system files (already filtered)
        unknown    — unrecognised extensions → quarantine by default

    Returns dict with those four keys.
    """
    ac=cfg.get("artifacts",{})
    if not ac.get("enabled",True):
        return {"keep":[],"quarantine":[],"ignore":[],"unknown":[]}

    keep_exts   ={e.lower() for e in ac.get("keep_extensions",[".jpg",".jpeg",".pdf"])}
    smart_exts  ={e.lower() for e in ac.get("smart_extensions",[])}
    quar_exts   ={e.lower() for e in ac.get("quarantine_extensions",
                    [".nfo",".sfv",".txt",".url",".log",".m3u",".m3u8"])}
    min_dim     =int(ac.get("smart_png_min_dimension",800))
    quar_unknown=bool(ac.get("quarantine_unknown",True))
    audio_exts  ={".mp3",".flac",".m4a",".aiff",".wav",".ogg",".opus",".wma"}

    result:Dict[str,List[Path]]={"keep":[],"quarantine":[],"ignore":[],"unknown":[]}
    try:
        for p in folder.iterdir():
            if not p.is_file(): continue
            if is_hidden_file(p): result["ignore"].append(p); continue
            sfx=p.suffix.lower()
            if sfx in audio_exts: continue          # audio files handled elsewhere
            if sfx in keep_exts:
                # Special case: .cue — keep only if paired audio exists
                if sfx==".cue":
                    if _cue_has_paired_audio(p): result["keep"].append(p)
                    else: result["quarantine"].append(p)
                else:
                    result["keep"].append(p)
            elif sfx in smart_exts:
                # .png — always quarantine (policy: let go of all PNGs)
                result["quarantine"].append(p)
            elif sfx in quar_exts:
                result["quarantine"].append(p)
            else:
                if quar_unknown: result["quarantine"].append(p)
                else: result["unknown"].append(p)
    except PermissionError as e:
        out(f"  {C.DIM}Permission denied scanning artifacts: {e}{C.RESET}",level=VERBOSE)
    return result

def move_artifacts_to_quarantine(
    artifacts:Dict[str,List[Path]],
    folder_name:str,
    cfg:Dict[str,Any],
    profile_obj:Dict[str,Any],
    source_root:Path,
    dry_run:bool=False,
    session_id:str=""
)->int:
    """Move quarantine files to Review/Artifacts/<folder_name>/. Returns count moved."""
    files=artifacts.get("quarantine",[])
    if not files: return 0
    ac=cfg.get("artifacts",{})
    qfolder_rel=ac.get("quarantine_folder","Review/Artifacts")
    roots=ensure_roots(profile_obj,source_root)
    qfolder=roots["review_root"]/Path(qfolder_rel).parts[-1]/folder_name
    moved=0
    for f in files:
        if not f.exists(): continue
        if dry_run:
            out(f"    {C.DIM}[artifact dry-run]{C.RESET} quarantine: {f.name}")
            continue
        try:
            ensure_dir(qfolder)
            dst=qfolder/f.name
            if dst.exists(): dst=qfolder/(f.stem+"_"+uuid.uuid4().hex[:6]+f.suffix)
            shutil.move(str(f),str(dst))
            moved+=1
        except Exception as e:
            err(f"    artifact move failed ({f.name}): {e}")
    return moved

def list_audio_files(folder:Path,exts:List[str],follow_symlinks:bool=False)->List[Path]:
    out_f:List[Path]=[]
    try:
        for p in folder.iterdir():
            if not follow_symlinks and p.is_symlink(): continue
            if p.is_file() and p.suffix.lower() in exts and not is_hidden_file(p): out_f.append(p)
    except PermissionError: pass
    return out_f

def detect_format_dupes(files:List[Path])->List[str]:
    by_stem:Dict[str,List[str]]={}
    for f in files: by_stem.setdefault(normalize_unicode(f.stem.lower()),[]).append(f.suffix.lower())
    return [f"{s}: {', '.join(sorted(set(e)))}" for s,e in by_stem.items() if len(set(e))>1]

def compute_dominant(counter:Counter)->Tuple[Optional[str],float,int]:
    if not counter: return None,0.0,0
    total=sum(counter.values()); key,cnt=counter.most_common(1)[0]
    return key,(cnt/total if total else 0.0),cnt

def recover_display(norm_key:Optional[str],raw_by_norm:Dict[str,Counter])->Optional[str]:
    if not norm_key: return None
    rc=raw_by_norm.get(norm_key)
    return rc.most_common(1)[0][0] if rc else norm_key

def detect_va(aa_norm: str, track_artists: Any, cfg: Dict[str, Any]) -> bool:
    """
    Return True if this folder looks like a Various Artists release.

    v6.1 PHILOSOPHY: Default to album, not VA.  Only flag VA when:
      (a) albumartist tag explicitly says "Various Artists" or similar, OR
      (b) heuristic ratio is HIGH (≥0.75) AND there's no dominant artist.
    The previous 0.50 threshold caused massive false positives on real albums
    where featured artists, remixers, or tag variations inflated the unique count.

    track_artists may be:
      - a Counter[norm_str → int]  (preferred — one count per track)
      - a list[str]                (legacy — interpreted as per-track list)
    """
    vc = cfg.get("various_artists", {})
    matches = {m.lower() for m in vc.get("albumartist_matches", [])}

    # Explicit albumartist tag says Various Artists — this is definitive
    if aa_norm and aa_norm in matches:
        return True

    if not vc.get("enable_heuristics", True):
        return False

    # If albumartist is present and NOT a VA keyword, this is almost
    # certainly a single-artist album — don't even run the heuristic.
    if aa_norm and aa_norm not in matches:
        return False

    # Resolve to (unique_artists, total_tracks)
    if isinstance(track_artists, Counter):
        non_empty = {k: v for k, v in track_artists.items() if k}
        if not non_empty:
            return False
        total = sum(non_empty.values())
        unique = len(non_empty)
    else:
        non_empty = [a for a in track_artists if a]
        if not non_empty:
            return False
        total = len(non_empty)
        unique = len(set(non_empty))

    # v6.1: Raised from 0.50 → 0.75 — require strong evidence of VA.
    # At 0.50, a 6-track album with 3 "feat." variations triggers VA.
    # At 0.75, you need 6 out of 8 tracks to have genuinely different artists.
    threshold = float(vc.get("unique_artist_ratio_above", 0.75))
    if (unique / total) < threshold:
        return False

    # Extra guard: if there's a dominant primary artist covering ≥40% of tracks,
    # this is likely a single-artist album with guests, not true VA.
    if isinstance(track_artists, Counter) and non_empty:
        top_count = max(non_empty.values())
        if top_count / total >= 0.40:
            return False

    return True

def pick_year(year_counts:Counter,tracks_with_year:int,total:int,cfg:Dict[str,Any])->Tuple[Optional[int],Dict[str,Any]]:
    yc=cfg.get("year",{})
    if not yc.get("enabled",True) or total==0: return None,{"included":False}
    pres=tracks_with_year/total
    if pres<float(yc.get("require_presence_ratio",0.50)): return None,{"included":False,"reason":"presence_ratio_low"}
    yv,ys,_=compute_dominant(year_counts)
    if yv is None: return None,{"included":False,"reason":"no_year_votes"}
    if ys<float(yc.get("agreement_threshold",0.70)): return None,{"included":False,"reason":"agreement_low"}
    try: y=int(yv)
    except Exception: return None,{"included":False,"reason":"year_not_int"}
    amin=int(yc.get("allowed_range",{}).get("min",1900)); amax=int(yc.get("allowed_range",{}).get("max",2100))
    if not(amin<=y<=amax): return None,{"included":False,"reason":"year_out_of_range"}
    return y,{"included":True,"presence_ratio":pres,"agreement":ys}

def build_folder_proposal(folder:Path,audio_files:List[Path],source_root:Path,profile:Dict[str,Any],cfg:Dict[str,Any],force_album:bool=False)->Optional[FolderProposal]:
    # ── per-folder override ──────────────────────────────────────────────
    override=load_folder_override(folder)
    if override and override.get("skip"): return None

    albums_norm:Counter=Counter(); albumartists_norm:Counter=Counter(); track_artists_norm:Counter=Counter(); years:Counter=Counter()
    primary_artists_norm:Counter=Counter()  # v5.5.1: artist before feat/ft — for VA detection
    genres:Counter=Counter(); labels:Counter=Counter(); bpms:List[float]=[]; keys_raw:Counter=Counter()
    albums_raw:Dict[str,Counter]={}; albumartists_raw:Dict[str,Counter]={}; track_artists_raw:Dict[str,Counter]={}
    tracks_with_year=0; tagged=0; unreadable=0
    extensions:Counter=Counter(p.suffix.lower() for p in audio_files)
    all_tags:List[Dict[str,Optional[str]]]=[]

    for f in audio_files:
        tags=read_audio_tags(f,cfg)
        all_tags.append(tags)
        if all(v is None for v in tags.values()): unreadable+=1; continue
        alb_r=(tags.get("album") or "").strip(); aa_r=(tags.get("albumartist") or "").strip()
        art_r=(tags.get("artist") or "").strip(); yr_r=(tags.get("year") or "").strip()
        genre_r=(tags.get("genre") or "").strip()
        label_r=(tags.get("label") or "").strip()
        bpm_r=(tags.get("bpm") or "").strip()
        key_r=(tags.get("key") or "").strip()

        # v6.2: discard garbage tag values (URLs, domains, musical keys in artist field)
        if _is_garbage_tag_value(aa_r):  aa_r=""
        if _is_garbage_tag_value(art_r): art_r=""
        if _is_garbage_tag_value(alb_r): alb_r=""

        # Strip display noise and disc indicators from album for voting
        alb_r_clean=strip_disc_indicator(strip_display_noise(alb_r)) if alb_r else alb_r

        alb_n=normalize_for_vote(alb_r_clean,cfg) if alb_r_clean else ""
        aa_n =normalize_for_vote(aa_r,cfg)         if aa_r         else ""
        art_n=normalize_for_vote(art_r,cfg)        if art_r        else ""
        if alb_n: albums_norm[alb_n]+=1; albums_raw.setdefault(alb_n,Counter())[alb_r_clean or alb_r]+=1
        if aa_n:  albumartists_norm[aa_n]+=1; albumartists_raw.setdefault(aa_n,Counter())[aa_r]+=1
        if art_n:
            track_artists_norm[art_n]+=1; track_artists_raw.setdefault(art_n,Counter())[art_r]+=1
            # v5.5.1: extract primary artist (before feat/ft/featuring/&/vs) for VA detection
            _prim = re.split(r'\s+(?:feat\.?|ft\.?|featuring|&|and|vs\.?|x)\s+', art_n, maxsplit=1, flags=re.I)[0].strip()
            if _prim: primary_artists_norm[_prim]+=1
        if genre_r: genres[genre_r]+=1
        if label_r: labels[label_r]+=1
        if key_r: keys_raw[key_r]+=1
        if bpm_r:
            try: bpms.append(float(re.sub(r"[^\d.]","",bpm_r)))
            except ValueError: pass
        if yr_r:
            m=re.search(r"(\d{4})",yr_r)
            if m: years[m.group(1)]+=1; tracks_with_year+=1
        tagged+=1

    total=len(audio_files)
    dom_alb_n,alb_share,_=compute_dominant(albums_norm)
    dom_aa_n,aa_share,_  =compute_dominant(albumartists_norm)
    dom_art_n,art_share,_=compute_dominant(track_artists_norm)
    dom_alb=recover_display(dom_alb_n,albums_raw); dom_aa=recover_display(dom_aa_n,albumartists_raw)

    # Always parse folder name heuristic — needed for compound artist recovery (&/and joins)
    _h_parsed=parse_folder_name_heuristic(folder.name,cfg)
    _h_album=_h_parsed.get("album"); _h_artist=_h_parsed.get("artist"); _h_year=_h_parsed.get("year")

    used_heuristic=False
    if not dom_alb:
        if _h_album:
            dom_alb=_h_album; alb_share=0.50
        # Only take artist from folder name when we have no tags at all
        if tagged==0:
            dom_aa=dom_aa or _h_artist
            aa_share=aa_share or (0.50 if _h_artist else 0.0)
        used_heuristic=True
        if not tracks_with_year and _h_year: years[_h_year]=1; tracks_with_year=1

    # ── override: force name / artist ───────────────────────────────────
    if override:
        if override.get("album"):  dom_alb=str(override["album"])
        if override.get("artist") or override.get("albumartist"):
            dom_aa=str(override.get("albumartist") or override.get("artist"))
        if override.get("year"):
            try: years[str(int(override["year"]))]=total; tracks_with_year=total
            except (TypeError, ValueError): pass

    va_label=cfg.get("various_artists",{}).get("label","VA")
    # ── VA detection ──────────────────────────────────────────────────────
    # CRITICAL: pass the Counter (not .keys()) so ratio = unique/total tracks.
    # Passing list(Counter.keys()) gives a 1-element list for any single-artist
    # album, yielding ratio=1/1=1.0 ≥ 0.5 → false VA for every real album.
    # v5.5.1: use primary_artists_norm (feat/ft stripped) for VA detection —
    # prevents "Artist feat. X", "Artist feat. Y" from each counting as unique.
    is_va = detect_va(dom_aa_n or "", primary_artists_norm, cfg)

    # v6.2: folder-name VA prefix override — if folder explicitly starts with
    # "VA -", "VA_", "va -" etc., trust that signal and force VA detection.
    # This rescues cases where tags have garbage/wrong albumartist but the
    # user (or uploader) named the folder correctly.
    _fn_stripped = folder.name.strip()
    _fn_va_prefix = re.match(r'^(?:va|v\.?a\.?)\s*[-_–—]', _fn_stripped, re.I)
    _va_forced_by_folder = False
    if _fn_va_prefix and not force_album:
        is_va = True
        _va_forced_by_folder = True

    # v6.1: force_album override — user says this is not VA
    if force_album:
        is_va = False
        # If albumartist was a VA keyword, derive artist from track-level tags instead
        _va_kws_early = {m.lower() for m in cfg.get("various_artists",{}).get("albumartist_matches",[])}
        if dom_aa_n and dom_aa_n in _va_kws_early and dom_art_n:
            dom_aa   = recover_display(dom_art_n, {}) or dom_art_n
            dom_aa_n = dom_art_n
            aa_share = art_share

    # Pre-compute the set of known VA keywords for safeguard checks
    _va_kws = {m.lower() for m in cfg.get("various_artists",{}).get("albumartist_matches",[])}

    # v6.2: Safeguards A–I can un-flag VA, but NEVER when VA was forced by
    # an explicit "VA -" folder-name prefix — that's the strongest signal we have.
    # Safeguard A: Strong single-artist signal overrides VA heuristic.
    # If albumartist AND artist tags BOTH point to the same name at ≥75%
    # dominance, this is definitively a single-artist album — not VA.
    # Guard: only fires when the albumartist is NOT itself a VA keyword.
    if is_va and not _va_forced_by_folder and dom_aa_n and dom_aa_n not in _va_kws and dom_art_n:
        if dom_aa_n == dom_art_n and aa_share >= 0.75 and art_share >= 0.75:
            is_va = False

    # Safeguard A2: albumartist alone at very high confidence with no
    # counter-evidence — but ONLY for non-VA albumartist tags.
    if is_va and not _va_forced_by_folder and dom_aa_n and dom_aa_n not in _va_kws and aa_share >= 0.90:
        if dom_art_n and art_share >= 0.75 and dom_art_n == dom_aa_n:
            is_va = False
        elif not dom_art_n:
            # albumartist present & non-VA, artist tag absent → trust albumartist
            is_va = False

    # Safeguard B: Folder name contains the dominant albumartist name.
    # "Artist - Album" or "Year - Artist - Album" folder + AA tag = single artist.
    # Guard: albumartist must not be a VA keyword.
    # v6.1: lowered threshold from 0.80→0.60, added contains check (not just startswith).
    if is_va and not _va_forced_by_folder and dom_aa_n and dom_aa_n not in _va_kws and aa_share >= 0.60:
        fn_norm = normalize_unicode(folder.name.strip().lower())
        aa_norm_str = normalize_unicode((dom_aa or dom_aa_n or "").strip().lower())
        if aa_norm_str and len(aa_norm_str) >= 3 and aa_norm_str in fn_norm:
            is_va = False

    # Safeguard C: No albumartist tag, but artist tag is dominant and
    # folder name contains that artist.
    # v6.1: lowered threshold from 0.90→0.70, added contains check.
    if is_va and not _va_forced_by_folder and not dom_aa_n and dom_art_n and art_share >= 0.70:
        fn_norm = normalize_unicode(folder.name.strip().lower())
        art_norm_str = normalize_unicode(dom_art_n.strip().lower())
        if art_norm_str and len(art_norm_str) >= 3 and art_norm_str in fn_norm:
            is_va = False

    # Safeguard E (v5.5.1): No albumartist tag, but dominant artist covers
    # all tracks — regardless of folder naming.  If every single track
    # credits the SAME primary artist, this is definitively single-artist.
    # This rescues "Year - Album" or album-only folder names where
    # Safeguard C cannot match on folder prefix.
    if is_va and not _va_forced_by_folder and not dom_aa_n and dom_art_n and art_share >= 0.95:
        is_va = False

    # Safeguard F (v5.5.1): albumartist present, not a VA keyword,
    # dominant artist present and matches — even without folder alignment.
    # Covers cases where albumartist and artist agree but aren't identical
    # strings (e.g. "artist feat. X" vs "artist").
    if is_va and not _va_forced_by_folder and dom_aa_n and dom_aa_n not in _va_kws and dom_art_n:
        # Extract primary artist (before feat/ft/featuring) for comparison
        _prim_aa = re.split(r'\s+(?:feat\.?|ft\.?|featuring)\s+', dom_aa_n, maxsplit=1, flags=re.I)[0].strip()
        _prim_art = re.split(r'\s+(?:feat\.?|ft\.?|featuring)\s+', dom_art_n, maxsplit=1, flags=re.I)[0].strip()
        if _prim_aa == _prim_art and aa_share >= 0.70 and art_share >= 0.70:
            is_va = False

    # Safeguard G (v6.1): Parent folder name contains the dominant artist.
    # Structure like "Artist Name/2024 - Album" or "Artist Name/Album Title"
    # means the parent is an artist folder — this is not VA.
    if is_va and not _va_forced_by_folder and (dom_aa_n or dom_art_n):
        _parent_name = normalize_unicode(folder.parent.name.strip().lower())
        _check_artist = dom_aa_n if (dom_aa_n and dom_aa_n not in _va_kws) else dom_art_n
        _check_share = aa_share if (dom_aa_n and dom_aa_n not in _va_kws) else art_share
        if _check_artist and _parent_name and _check_share >= 0.60:
            _art_norm = normalize_unicode(_check_artist.strip().lower())
            if _art_norm and (_parent_name == _art_norm or _parent_name.startswith(_art_norm)):
                is_va = False

    # Safeguard H (v6.1): Sibling folders share the same dominant artist.
    # If the parent contains multiple album subfolders and sibling folder names
    # contain the same artist, this is an artist discography — not VA.
    if is_va and not _va_forced_by_folder and (dom_aa_n or dom_art_n):
        _check_artist_h = dom_aa_n if (dom_aa_n and dom_aa_n not in _va_kws) else dom_art_n
        _check_share_h = aa_share if (dom_aa_n and dom_aa_n not in _va_kws) else art_share
        if _check_artist_h and _check_share_h >= 0.60:
            _art_norm_h = normalize_unicode(_check_artist_h.strip().lower())
            try:
                _siblings = [d.name for d in folder.parent.iterdir() if d.is_dir() and d != folder]
                if _siblings and _art_norm_h:
                    _sibling_match = sum(1 for s in _siblings if _art_norm_h in normalize_unicode(s.strip().lower()))
                    if _sibling_match >= 1:
                        is_va = False
            except OSError as e:
                out(f"  {C.DIM}Could not scan siblings: {e}{C.RESET}",level=VERBOSE)

    # Safeguard I (v6.1): Remix/remixed album — track artists are remixers,
    # not evidence of VA.  If the album name or folder name contains remix
    # indicators, the albumartist (or folder-derived artist) is the real artist.
    # Remixers inflate the unique artist ratio but don't make it a compilation.
    if is_va and not _va_forced_by_folder:
        _remix_pat = re.compile(r'\b(remix(?:ed|es)?|rmx|reworks?)\b', re.I)
        _alb_is_remix = bool(dom_alb and _remix_pat.search(dom_alb))
        _fn_is_remix = bool(_remix_pat.search(folder.name))
        if _alb_is_remix or _fn_is_remix:
            # If we have an albumartist that isn't a VA keyword, trust it
            if dom_aa_n and dom_aa_n not in _va_kws:
                is_va = False
            # Or if we can parse the artist from the folder name
            elif not dom_aa_n:
                _parsed_fn = parse_folder_name_heuristic(folder.name)
                _fn_artist = _parsed_fn.get("artist")
                if _fn_artist:
                    dom_aa = _fn_artist
                    dom_aa_n = normalize_for_vote(_fn_artist, cfg)
                    aa_share = 0.60
                    is_va = False

    # Safeguard D (v4.3): albumartist tag is a label name, not an artist.
    # Pattern confirmed in 37 folders (3.7%) in session 2.
    # Re-parse using dominant track artist instead.
    if not _va_forced_by_folder and dom_aa_n and _detect_label_as_albumartist(dom_aa or dom_aa_n or ""):
        # Folder has pattern "Label - Artist - Album" or "Label - Album"
        # Check if track artist gives us a cleaner single-artist signal
        if dom_art_n and art_share >= 0.70:
            dom_aa   = recover_display(dom_art_n, {}) or dom_art_n
            dom_aa_n = dom_art_n
            aa_share = art_share
            is_va    = detect_va(dom_aa_n, primary_artists_norm, cfg)

    # ── folder content type ─────────────────────────────────────────────
    folder_type=classify_folder_content(audio_files,folder.name,all_tags,cfg)
    is_mix=(folder_type=="mix")
    is_ep =(folder_type=="ep")
    if is_va and folder_type not in ("mix",): folder_type="va"

    _feat_pat=re.compile(r'\s+(?:feat\.?|ft\.?|featuring)\s+.+$',re.I)
    if is_va:
        artist_for_folder:Optional[str]=va_label
    elif dom_aa:
        # Strip feat. collaborator from albumartist — main artist owns the folder,
        # feat. credit stays in the album name if it appears there.
        _prim_aa=_feat_pat.sub("",dom_aa).strip() or dom_aa
        artist_for_folder=normalize_artist_name(_prim_aa,cfg)
    elif cfg.get("decision",{}).get("allow_artist_fallback",True):
        raw_art=recover_display(dom_art_n,track_artists_raw) or dom_art_n
        _prim_art=_feat_pat.sub("",raw_art or "").strip() or (raw_art or "")
        artist_for_folder=normalize_artist_name(_prim_art,cfg) or None
        # Compound artist recovery: if folder name has "A and B" / "A & B" and tag artist
        # is one of the parts (tags split credits per track), prefer the compound name.
        if artist_for_folder and _h_artist:
            _split_pat=re.compile(r'\s+(?:and|&)\s+',re.I)
            _h_parts=_split_pat.split(_h_artist)
            if len(_h_parts)>=2:
                _art_low=normalize_unicode(artist_for_folder.lower().strip())
                _matches=[normalize_unicode(p.lower().strip())==_art_low or _art_low in normalize_unicode(p.lower()) for p in _h_parts]
                if any(_matches):
                    # One of the compound parts matches the tag artist → use full compound name
                    # Normalise "and" → "&" for cleaner folder names
                    _compound=_split_pat.sub(" & ",_h_artist)
                    artist_for_folder=normalize_artist_name(_compound,cfg) or artist_for_folder
    else:
        artist_for_folder=None

    if not dom_alb or not artist_for_folder: return None

    # ── apply title-case fix to album name ──────────────────────────────
    dom_alb=apply_title_case(dom_alb,cfg)
    # Strip residual display noise from voted album name
    dom_alb=strip_display_noise(dom_alb)
    # Strip garbage bracket stack from album name
    garbage=detect_garbage_name(dom_alb)
    if garbage: dom_alb=strip_bracket_stack(dom_alb)

    # ── strip URL/domain that leaked into voted album name ───────────────
    dom_alb=strip_trailing_domains(dom_alb)
    # If the album name IS a bare domain/URL (nothing left after stripping), keep original
    if not dom_alb.strip():
        dom_alb=apply_title_case(recover_display(dom_alb_n,albums_raw) or dom_alb_n or "",cfg)

    year_val,year_meta=pick_year(years,tracks_with_year,max(total,1) if used_heuristic else total,cfg)

    # v7.0: If year came only from heuristic (no tag agreement) and was low confidence,
    # drop it — better to have no year than a wrong year in the folder name.
    if year_val and used_heuristic and year_meta.get("agreement",1.0)<0.70:
        year_val=None; year_meta={"included":False,"reason":"heuristic_low_agreement"}

    fmt=cfg.get("format",{})
    # Strip trailing EP/E.P./LP suffixes from album name before adding [EP] label
    # to prevent doubling like "Ashen EP [EP]"
    _ep_stripped_alb = dom_alb
    if is_ep and fmt.get("label_eps",True):
        # Strip both bare "EP"/"E.P." and bracketed "[EP]"/"(EP)" from album name end
        _ep_stripped_alb = re.sub(r'\s*[\[\(]\s*E\.?P\.?\s*[\]\)]\s*$|\s+E\.?P\.?\s*$', '', dom_alb, flags=re.I).strip()
    ep_suffix=" [EP]" if is_ep and fmt.get("label_eps",True) else ""
    # v7.0: Strip year from album name entirely, then place it at the end via pattern.
    # This prevents years appearing in the wrong position inside the album name
    # (e.g. "2023 Album Name" → "Artist - 2023 Album Name" instead of "Artist - Album Name (2023)").
    _alb_for_fmt = _ep_stripped_alb + ep_suffix
    if year_val:
        # Remove year in any position: "(2023)", "[2023]", bare "2023", leading "2023 - "
        _yr_str = str(year_val)
        _alb_for_fmt = re.sub(r'\s*[\(\[]\s*' + _yr_str + r'\s*[\)\]]\s*', ' ', _alb_for_fmt).strip()
        _alb_for_fmt = re.sub(r'(?:^|\s+)' + _yr_str + r'(?:\s+|$)', ' ', _alb_for_fmt).strip()
        _alb_for_fmt = re.sub(r'^\s*' + _yr_str + r'\s*[-–—]\s*', '', _alb_for_fmt).strip()
        _alb_for_fmt = _alb_for_fmt.strip(' -–—')
        if not _alb_for_fmt:
            _alb_for_fmt = _ep_stripped_alb + ep_suffix  # safety: don't blank out the name
    pat_key = "pattern_with_year" if year_val else "pattern_no_year"
    pat=fmt.get(pat_key,"{albumartist} - {album}")
    proposed=pat.format(albumartist=artist_for_folder,album=_alb_for_fmt,year=year_val or "")
    proposed=sanitize_name(proposed,repl=fmt.get("replace_illegal_chars_with"," - "))
    proposed=sanitize_windows_reserved(proposed)

    proposed=_apply_format_suffix(proposed,cfg,dict(extensions))

    is_flac_only=set(extensions.keys())=={".flac"}
    # Genre/label: plurality vote (raw display form — normalization happens in resolve_library_path)
    dom_genre,_,_=compute_dominant(genres)
    dom_label,_,_=compute_dominant(labels)
    dom_key,_,_=compute_dominant(keys_raw)
    # BPM: use median of all tracks (more robust than mode for continuous values)
    avg_bpm:Optional[float]=None
    if bpms:
        sorted_bpms=sorted(bpms)
        mid=len(sorted_bpms)//2
        avg_bpm=sorted_bpms[mid] if len(sorted_bpms)%2 else (sorted_bpms[mid-1]+sorted_bpms[mid])/2
    clean_albums=derive_clean_albums_root(profile,source_root)
    target_dir=resolve_library_path(clean_albums,artist_for_folder,dom_alb,year_val,
                                     is_flac_only,is_va,False,is_mix,cfg,
                                     profile=profile,genre=dom_genre,
                                     bpm=avg_bpm,key=dom_key,label=dom_label)

    # v7.0: Multi-disc grouping — if source folder name contains a disc indicator
    # (CD1, CD2, Disc 1, etc.), nest it as a subfolder under the album folder.
    # This groups CD1 + CD2 under one parent: Artist/Album/CD1/, Artist/Album/CD2/
    _disc_folder_pat=re.compile(r'(?:disc|cd|disk)\s*(\d+)',re.I)
    _disc_m=_disc_folder_pat.search(folder.name)
    if _disc_m:
        disc_label=f"CD{_disc_m.group(1)}"
        target_dir=target_dir/disc_label
        decision_extra_disc=disc_label
    else:
        decision_extra_disc=None

    # ── named confidence factors ─────────────────────────────────────────
    conf_factors=compute_confidence_factors(
        audio_files,tagged,alb_share,aa_share,art_share,
        used_heuristic,folder.name,proposed,all_tags,cfg)

    # v6.2: VA-prefix scene-named folders get penalised on folder_alignment
    # because scene naming ("VA-Title-CAT-WEB-2023-GRP") never matches the
    # proposed clean name. Boost alignment to 0.5 minimum for confirmed VA.
    if is_va and _fn_va_prefix and conf_factors.get("folder_alignment",0) < 0.5:
        conf_factors["folder_alignment"] = 0.5

    confidence=confidence_from_factors(conf_factors,used_heuristic)

    # Override confidence boost from .raagdosa
    if override and override.get("confidence_boost"):
        confidence=min(1.0,confidence+float(override["confidence_boost"]))

    fmt_dupes=detect_format_dupes(audio_files)
    decision={
        "dominant_album":dom_alb_n,"dominant_album_display":dom_alb,"dominant_album_share":alb_share,
        "dominant_albumartist":dom_aa_n,"dominant_albumartist_display":dom_aa,"dominant_albumartist_share":aa_share,
        "dominant_artist":dom_art_n,"dominant_artist_share":art_share,
        "is_va":is_va,"is_mix":is_mix,"is_ep":is_ep,"folder_type":folder_type,
        "albumartist_display":artist_for_folder,"year":year_val,"year_meta":year_meta,
        "genre":dom_genre,"label":dom_label,"bpm":avg_bpm,"key":dom_key,
        "unreadable_ratio":(unreadable/total) if total else 0.0,
        "used_heuristic":used_heuristic,"is_flac_only":is_flac_only,
        "garbage_reasons":garbage,"confidence_factors":conf_factors,
        "disc_subfolder":decision_extra_disc,
    }
    stats=FolderStats(tracks_total=total,tracks_tagged=tagged,tracks_unreadable=unreadable,extensions=dict(extensions),format_duplicates=fmt_dupes)
    return FolderProposal(folder_path=str(folder),folder_name=folder.name,proposed_folder_name=proposed,
                          target_path=str(target_dir),destination="clean",confidence=float(confidence),decision=decision,stats=stats)

# ─────────────────────────────────────────────
# v7.0 — Review summary & sidecar
# ─────────────────────────────────────────────
_REASON_DESCRIPTIONS:Dict[str,str]={
    "low_confidence":"Confidence score below threshold",
    "generic_folder_name":"Folder name is too generic to classify",
    "unreadable_ratio_high":"Too many tracks have unreadable tags",
    "heuristic_fallback":"Name derived from heuristics (no usable tags)",
    "ep":"Detected as EP release",
    "mix_folder":"Detected as DJ mix or chart compilation",
}

_FACTOR_DESCRIPTIONS:Dict[str,str]={
    "tag_coverage":"Tag readability",
    "dominance":"Album/artist vote consensus",
    "title_quality":"Song title confidence",
    "filename_consistency":"Filename ↔ tag alignment",
    "completeness":"Track numbering completeness",
    "aa_consistency":"Album-artist consistency",
    "folder_alignment":"Folder name ↔ proposed name",
}

def _build_review_summary(reasons:List[str],factors:Dict[str,float],confidence:float)->str:
    """Build a human-readable summary explaining why a folder was routed the way it was."""
    parts=[]
    for r in reasons:
        base=r.split(":")[0]  # handle "duplicate_in_run:FolderName"
        if base in _REASON_DESCRIPTIONS:
            parts.append(_REASON_DESCRIPTIONS[base])
        elif r.startswith("duplicate"):
            parts.append(f"Duplicate: {r.split(':',1)[1] if ':' in r else 'name collision'}")
        elif r.startswith("already_in_clean"):
            parts.append("Already exists in Clean library")
        else:
            parts.append(r.replace("_"," ").capitalize())
    # Add weak factor details
    weak=[f"{_FACTOR_DESCRIPTIONS.get(k,k)} ({v:.0%})" for k,v in factors.items()
          if isinstance(v,float) and v<0.5 and k in _FACTOR_DESCRIPTIONS]
    if weak:
        parts.append("Weak: "+", ".join(weak))
    return ". ".join(parts)

def _write_review_sidecar(folder_path:Path,proposal,session_id:str)->None:
    """Write .raagdosa_review.json inside a Review folder with routing rationale."""
    sidecar=folder_path/".raagdosa_review.json"
    data={
        "session_id":session_id,
        "timestamp":now_iso(),
        "original_folder":proposal.folder_name,
        "proposed_name":proposal.proposed_folder_name,
        "confidence":proposal.confidence,
        "destination":proposal.destination,
        "route_reasons":proposal.decision.get("route_reasons",[]),
        "review_summary":proposal.decision.get("review_summary",""),
        "confidence_factors":{k:round(v,3) for k,v in proposal.decision.get("confidence_factors",{}).items()},
        "artist":proposal.decision.get("albumartist_display",""),
        "album":proposal.decision.get("dominant_album_display",""),
        "is_va":proposal.decision.get("is_va",False),
        "user_notes":[],
    }
    try: write_json(sidecar,data)
    except Exception as e:
        out(f"  {C.DIM}Could not write review sidecar: {e}{C.RESET}",level=VERBOSE)

# ─────────────────────────────────────────────
# v7.0 — Structured review reasons
# ─────────────────────────────────────────────
REVIEW_REASON_PRESETS=[
    "va-misclass",        # VA detection was wrong
    "wrong-artist",       # Artist name incorrect
    "bad-tags",           # Tags are unreliable
    "incomplete-release", # Missing tracks or partial download
    "duplicate",          # Already have this release elsewhere
    "not-music",          # Not an album (samples, stems, etc.)
    "wrong-genre",        # Genre classification wrong
    "needs-research",     # Need to check Discogs/MusicBrainz
]

# v6.2: generic/vague folder names that are not album names — force review
_GENERIC_FOLDER_NAMES = {
    "a","music","complete","down tempo","downtempo","warm up","warm-up",
    "underground house","haifa club","estray","rare tracks","new","misc",
    "unsorted","temp","incoming","downloads","stuff","tracks","songs",
    "playlist","mix","mixes","cd1","cd2","cd3","disc 1","disc 2",
    "electronica-downtempo","world electro - ethno disco - global groove",
    "jazz house piano bar","master pieces of electro","remixes and other tracks",
}

def _apply_format_suffix(name:str,cfg:Dict[str,Any],extensions:Optional[Dict[str,int]])->str:
    """Append format tag (e.g. [FLAC]) to folder name based on config."""
    sfx=cfg.get("format_suffix",{})
    if not sfx.get("enabled",True) or not extensions:
        return name
    if sfx.get("only_if_all_same_extension",True) and len(extensions)==1:
        ext1=next(iter(extensions.keys()))
        if ext1 and ext1!=lower(sfx.get("ignore_extension",".mp3")) and sfx.get("style","brackets_upper")=="brackets_upper":
            name=f"{name} [{ext1.lstrip('.').upper()}]"
    return name

def _route_proposal(p:FolderProposal,cfg:Dict[str,Any],
                    seen_names:Counter,existing_clean:Set[str],
                    manifest_entries:Set[str],
                    review_albums:Path,dup_root:Path,mixes_root:Path,
                    all_proposals:Optional[List]=None)->FolderProposal:
    """Route a single proposal to clean/review/duplicate. Mutates p in place."""
    rr=cfg.get("review_rules",{}); min_conf=float(rr.get("min_confidence_for_clean",0.85))
    sc=cfg.get("scan",{}); max_unread=float(sc.get("max_unreadable_track_ratio",0.25))
    reasons:List[str]=[]; dest="clean"
    if rr.get("route_questionable_to_review",True) and p.confidence<min_conf:
        dest="review"; reasons.append("low_confidence")
    _fn_norm = p.folder_name.strip().strip("_- ").lower()
    if _fn_norm in _GENERIC_FOLDER_NAMES:
        if dest == "clean": dest = "review"
        reasons.append("generic_folder_name")
        p.confidence = min(p.confidence, min_conf - 0.05)
    seen_names[p.proposed_folder_name]+=1
    if rr.get("route_duplicates",True) and seen_names[p.proposed_folder_name]>1:
        dest="duplicate"
        if all_proposals:
            colliders=[q.folder_name for q in all_proposals if q.proposed_folder_name==p.proposed_folder_name and q is not p]
            reasons.append(f"duplicate_in_run:{colliders[0][:40] if colliders else '?'}")
        else:
            reasons.append("duplicate_in_run")
    norm_prop=normalize_unicode(p.proposed_folder_name)
    if rr.get("route_cross_run_duplicates",True) and (norm_prop in existing_clean or norm_prop in manifest_entries):
        dest="duplicate"; reasons.append("already_in_clean")
    if p.decision.get("unreadable_ratio",0.0)>max_unread:
        dest="review"; reasons.append("unreadable_ratio_high")
    if p.decision.get("used_heuristic",False):
        if dest=="clean": dest="review"
        reasons.append("heuristic_fallback")
    if p.stats.format_duplicates: reasons.append(f"format_dupes({len(p.stats.format_duplicates)})")
    if p.decision.get("is_ep"): reasons.append("ep")
    if p.decision.get("is_mix") and dest=="clean":
        reasons.append("mix_folder"); ensure_dir(mixes_root)
        p.target_path=str(mixes_root/p.proposed_folder_name)
    p.destination=dest; p.decision["route_reasons"]=reasons
    if reasons:
        p.decision["review_summary"]=_build_review_summary(reasons,p.decision.get("confidence_factors",{}),p.confidence)
    if dest=="review":    p.target_path=str(review_albums/p.proposed_folder_name)
    elif dest=="duplicate": p.target_path=str(dup_root/p.proposed_folder_name)
    return p

def scan_folders(cfg:Dict[str,Any],profile_name:str,since:Optional[dt.datetime]=None,genre_roots:Optional[List[str]]=None,itunes_mode:bool=False,session_name:str="")->Tuple[str,Path,List[FolderProposal]]:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    if not source_root.exists(): raise FileNotFoundError(f"source_root missing: {source_root}")

    # v5.5 — resolve log paths and skip-sets from config
    setup_logging_paths(cfg, profile, source_root)
    _init_skip_sets(cfg)

    # v4.1 — resolve effective genre roots (CLI flag + config)
    effective_genre_roots = _resolve_genre_roots(cfg, genre_roots)

    roots=ensure_roots(profile,source_root)
    clean_albums=roots["clean_albums"]; review_albums=roots["review_albums"]; dup_root=roots["duplicates"]
    # Also skip the wrapper folder itself during walk (it contains Clean/, Review/, logs/)
    wrapper_root_str=str(derive_wrapper_root(profile,source_root).resolve())+os.sep
    clean_root_str =str(derive_clean_root(profile,source_root).resolve())+os.sep
    review_root_str=str(derive_review_root(profile,source_root).resolve())+os.sep

    session_id=make_session_id(profile_name, str(source_root), session_name=session_name)
    # Deduplicate if same-minute session already exists
    session_base=Path(cfg["logging"]["session_dir"])/session_id
    if session_base.exists():
        for i in range(2,20):
            candidate=Path(cfg["logging"]["session_dir"])/f"{session_id}_{i}"
            if not candidate.exists():
                session_id=f"{session_id}_{i}"; session_base=candidate; break
    session_dir=session_base; ensure_dir(session_dir)

    sc=cfg.get("scan",{}); exts=[e.lower() for e in sc.get("audio_extensions",[".mp3",".flac",".m4a"])]
    min_tracks=int(sc.get("min_tracks",3)); follow_sym=bool(sc.get("follow_symlinks",False))
    leaf_only=bool(sc.get("leaf_folders_only",True))
    ignore_patterns:List[str]=list(cfg.get("ignore",{}).get("ignore_folder_names",[]) or [])
    # v5.5: skip sets now fully populated by _init_skip_sets (also covers config overrides)
    skip_folder_names = set(_SKIP_FOLDER_NAMES)
    skip_exts = set(_SKIP_AUDIO_EXTENSIONS)

    # Initialise tag cache for this scan run
    _get_tag_cache(cfg)

    # Collect candidates with progress
    candidates:List[Path]=[]
    for root,dirs,files in os.walk(source_root,followlinks=follow_sym):
        rp=Path(root); rp_str=str(rp.resolve())+os.sep
        # Skip the entire wrapper folder (contains Clean/, Review/, logs/)
        if rp_str.startswith(wrapper_root_str): dirs[:]=[] ; continue
        if rp_str.startswith(clean_root_str) or rp_str.startswith(review_root_str): dirs[:]=[] ; continue
        # v4.1: skip __MACOSX and other noise folders
        dirs[:] = [d for d in dirs if d not in skip_folder_names]
        # v4.1: genre root protection — skip renaming them but recurse inside
        if effective_genre_roots and rp.name in effective_genre_roots and rp != source_root:
            continue  # don't process genre root itself as a candidate
        if leaf_only and dirs: continue
        audio_count = sum(1 for f in files
                          if Path(f).suffix.lower() in exts
                          and Path(f).suffix.lower() not in skip_exts
                          and not f.startswith("._"))
        if audio_count >= min_tracks: candidates.append(rp)

    if since:
        candidates=[f for f in candidates if dt.datetime.fromtimestamp(folder_mtime(f))>=since]
        out(f"  --since: {len(candidates)} folders modified after {since.strftime('%Y-%m-%d %H:%M')}",level=VERBOSE)

    # Filter ignores before submitting to workers
    candidates=[rp for rp in candidates if not folder_matches_ignore(rp.name,ignore_patterns)]

    # ── Parallel scan ────────────────────────────────────────────────────
    # Tag reading (mutagen) is I/O-bound → threads are the right tool.
    # Each worker is independent — they share the tag cache (thread-safe)
    # but build their own local vote counters.
    workers=int(sc.get("workers", min(8, (os.cpu_count() or 4))))
    out(f"  Scanning {len(candidates)} folder(s) with {workers} worker(s)…",level=VERBOSE)

    proposals_raw:List[FolderProposal]=[]; proposals_lock=Lock()
    prog=Progress(len(candidates),"Scanning")

    def _scan_one(rp:Path)->Optional[FolderProposal]:
        if should_stop(): return None
        audio_files=list_audio_files(rp,exts,follow_sym)
        if len(audio_files)<min_tracks:
            prog.tick(rp.name); return None
        prog.tick(rp.name)
        return build_folder_proposal(rp,audio_files,source_root,profile,cfg)

    if workers<=1 or len(candidates)<=4:
        # Single-threaded path (avoids thread overhead for small runs)
        for rp in candidates:
            if should_stop(): out(f"\n{C.YELLOW}Stopped after scanning {len(proposals_raw)} folders.{C.RESET}"); break
            prop=_scan_one(rp)
            if prop: proposals_raw.append(prop)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures={pool.submit(_scan_one,rp):rp for rp in candidates}
            for fut in as_completed(futures):
                if should_stop():
                    # Cancel pending (best-effort — running tasks will complete)
                    for f in futures:
                        if not f.done(): f.cancel()
                    out(f"\n{C.YELLOW}Stop requested — waiting for in-flight tasks…{C.RESET}")
                    break
                try:
                    prop=fut.result()
                    if prop:
                        with proposals_lock: proposals_raw.append(prop)
                except Exception as e:
                    rp=futures[fut]; err(f"  scan error in {rp.name}: {e}")

    prog.done()

    # Preserve input order (as_completed returns in completion order)
    # Sort by original path for deterministic proposals/reports
    proposals=sorted(proposals_raw,key=lambda p:p.folder_path)

    # Save tag cache to disk after scan completes
    if _tag_cache is not None:
        _tag_cache.save()
        out(f"  Tag cache: {_tag_cache.size} entries",level=VERBOSE)

    # Routing
    existing_clean:Set[str]=set()
    if clean_albums.exists():
        try:
            for item in clean_albums.rglob("*"):
                if item.is_dir(): existing_clean.add(normalize_unicode(item.name))
        except PermissionError as e:
            out(f"  {C.DIM}Permission denied scanning clean library: {e}{C.RESET}",level=VERBOSE)
    manifest_entries:Set[str]=set(read_manifest(cfg).get("entries",{}).keys())
    lib=cfg.get("library",{}); mixes_folder=lib.get("mixes_folder","_Mixes")
    mixes_root=clean_albums.parent/mixes_folder
    seen_names:Counter=Counter()

    for p in proposals:
        _route_proposal(p,cfg,seen_names,existing_clean,manifest_entries,
                        review_albums,dup_root,mixes_root,all_proposals=proposals)

    payload={"app":cfg.get("app",{}),"session_id":session_id,"timestamp":now_iso(),"profile":profile_name,
             "source_root":str(source_root),"since":since.isoformat() if since else None,
             "folder_proposals":[dataclasses.asdict(p) for p in proposals]}
    write_json(session_dir/"proposals.json",payload)
    _write_session_reports(session_id,profile_name,source_root,proposals,session_dir,cfg)

    clean_n=sum(1 for p in proposals if p.destination=="clean")
    rev_n  =sum(1 for p in proposals if p.destination=="review")
    dup_n  =sum(1 for p in proposals if p.destination=="duplicate")
    since_note=f"  (since {since.strftime('%Y-%m-%d %H:%M')})" if since else ""
    # Scan summary: total tracks, size, format breakdown
    total_tracks=sum(p.stats.tracks_total for p in proposals)
    total_bytes=sum(_folder_size(Path(p.folder_path)) for p in proposals if Path(p.folder_path).exists())
    fmt_totals:Counter=Counter()
    for p in proposals:
        for ext,cnt in (p.stats.extensions or {}).items():
            fmt_totals[ext.upper().lstrip(".")]+=cnt
    fmt_str=" · ".join(f"{fmt}: {cnt}" for fmt,cnt in fmt_totals.most_common()) if fmt_totals else ""
    out(f"\n{C.BOLD}Session:{C.RESET}   {session_id}")
    out(f"{C.BOLD}Found:{C.RESET}     {len(proposals)} folders, {total_tracks} tracks ({_human_size(total_bytes)}){since_note}")
    if fmt_str:
        out(f"           {C.DIM}{fmt_str}{C.RESET}")
    out(f"{C.BOLD}Routing:{C.RESET}   {C.GREEN}Clean: {clean_n}{C.RESET} | {C.YELLOW}Review: {rev_n}{C.RESET} | {C.RED}Dupes: {dup_n}{C.RESET}")

    # Tag coverage summary — shows how well populated the tags are for the active template
    lib=_resolve_lib_cfg(profile,cfg)
    tpl=lib.get("template","{artist}/{album}")
    # Detect which tokens the template uses
    tpl_tokens=set(re.findall(r"\{(\w+)\}",tpl))
    # Map tokens to the tag field needed
    _TOKEN_TAG_MAP={"genre":"genre","decade":"year","bpm_range":"bpm","camelot_key":"key","label":"label"}
    needed={tok:_TOKEN_TAG_MAP[tok] for tok in tpl_tokens if tok in _TOKEN_TAG_MAP}
    if needed and proposals:
        total_folders=len(proposals)
        out(f"\n{C.BOLD}Tag coverage{C.RESET} (for template: {tpl})")
        for tok,tag_key in sorted(needed.items()):
            has_count=sum(1 for p in proposals if p.decision.get(tag_key))
            pct=int(has_count/total_folders*100) if total_folders else 0
            color=C.GREEN if pct>=80 else (C.YELLOW if pct>=50 else C.RED)
            out(f"  {tok:<14} {color}{has_count}/{total_folders} folders ({pct}%){C.RESET}")
        missing_count=sum(1 for p in proposals for tok,tag_key in needed.items() if not p.decision.get(tag_key))
        if missing_count:
            out(f"  {C.DIM}Folders with missing tags will be placed under fallback folders (_Unsorted, etc.){C.RESET}")

    out(f"{C.DIM}Reports:   {session_dir}/report.{{txt,csv,html}}{C.RESET}")
    return session_id,session_dir,proposals

# ─────────────────────────────────────────────
# Session reports: TXT + CSV + HTML
# ─────────────────────────────────────────────
def _write_session_reports(session_id:str,profile_name:str,source_root:Path,proposals:List[FolderProposal],session_dir:Path,cfg:Dict[str,Any])->None:
    if not cfg.get("logging",{}).get("write_human_report",True): return
    cn=sum(1 for p in proposals if p.destination=="clean")
    rn=sum(1 for p in proposals if p.destination=="review")
    dn=sum(1 for p in proposals if p.destination=="duplicate")

    # TXT
    lines=[f"RaagDosa v{APP_VERSION} — Session Report","="*70,
           f"Session:  {session_id}",f"Profile:  {profile_name}",f"Source:   {source_root}",f"Date:     {now_iso()}","",
           f"SUMMARY:  {len(proposals)} total | CLEAN {cn} | REVIEW {rn} | DUPES {dn}","",
           f"{'Status':<9} {'Conf':>6}  {'Original Folder':<45}  Proposed Name","-"*110]
    for p in proposals:
        rr=", ".join(p.decision.get("route_reasons",[]))
        tag={"clean":"CLEAN","review":"REVIEW","duplicate":"DUPE"}.get(p.destination,p.destination.upper())
        heur=" [heuristic]" if p.decision.get("used_heuristic") else ""
        va_flag=" [VA]" if p.decision.get("is_va") else ""
        art_info=f"  artist={p.decision.get('albumartist_display','')}" if p.decision.get("albumartist_display") else ""
        lines.append(f"{tag:<9} {p.confidence:>6.2f}  {p.folder_name[:45]:<45}  → {p.proposed_folder_name}{heur}{va_flag}{art_info}{('  ['+rr+']') if rr else ''}")
        for fd in (p.stats.format_duplicates or []):
            lines.append(f"{'':>20}  ⚠ format dupe: {fd}")
    (session_dir/"report.txt").write_text("\n".join(lines),encoding="utf-8")

    # CSV
    with (session_dir/"report.csv").open("w",newline="",encoding="utf-8") as f:
        w=csv.writer(f)
        w.writerow(["status","confidence","original_folder","proposed_name","target_path","track_count",
                    "tagged_count","unreadable_count","extensions","route_reasons","heuristic","format_duplicates"])
        for p in proposals:
            w.writerow([p.destination,f"{p.confidence:.4f}",p.folder_name,p.proposed_folder_name,p.target_path,
                        p.stats.tracks_total,p.stats.tracks_tagged,p.stats.tracks_unreadable,
                        "|".join(sorted(p.stats.extensions.keys())),"|".join(p.decision.get("route_reasons",[])),
                        "yes" if p.decision.get("used_heuristic") else "no","|".join(p.stats.format_duplicates or [])])

    # HTML
    rows=""
    for p in proposals:
        rr=", ".join(p.decision.get("route_reasons",[])); heur=" <span class='h'>[heuristic]</span>" if p.decision.get("used_heuristic") else ""
        dc={"clean":"c","review":"r","duplicate":"d"}.get(p.destination,"")
        cc="hi" if p.confidence>=0.90 else ("mi" if p.confidence>=0.75 else "lo")
        rows+=(f'<tr class="{dc}"><td class="st">{p.destination.upper()}</td>'
               f'<td class="cf {cc}">{p.confidence:.2f}</td><td>{html.escape(p.folder_name)}</td>'
               f'<td>{html.escape(p.proposed_folder_name)}{heur}</td><td>{p.stats.tracks_total}</td>'
               f'<td>{html.escape("|".join(sorted(p.stats.extensions.keys())))}</td>'
               f'<td class="rr">{html.escape(rr)}</td></tr>\n')
    html_out=f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<title>RaagDosa {html.escape(session_id)}</title><style>
body{{font-family:'JetBrains Mono','Courier New',monospace;background:#1a1a1a;color:#e0e0e0;padding:2rem}}
h1{{color:#88c0d0}}h2{{color:#81a1c1;border-bottom:1px solid #3b4252;padding-bottom:.3rem}}
.summary{{background:#2e3440;border-radius:6px;padding:1rem;margin:1rem 0}}
.summary span{{margin-right:2rem;font-weight:bold}}.sc{{color:#a3be8c}}.sr{{color:#ebcb8b}}.sd{{color:#bf616a}}
table{{width:100%;border-collapse:collapse;margin-top:1rem;font-size:.85em}}
th{{background:#3b4252;color:#88c0d0;padding:.5rem;text-align:left}}
tr:nth-child(even){{background:#252b37}}
tr.c td.st{{color:#a3be8c}}tr.r td.st{{color:#ebcb8b}}tr.d td.st{{color:#bf616a}}
.hi{{color:#a3be8c}}.mi{{color:#ebcb8b}}.lo{{color:#bf616a}}
td{{padding:.4rem .5rem;border-bottom:1px solid #2e3440}}.rr{{color:#88c0d0;font-size:.8em}}.h{{color:#ebcb8b}}
</style></head><body>
<h1>🍛 RaagDosa v{APP_VERSION} — Session Report</h1>
<div class="summary"><span>Session: {html.escape(session_id)}</span><span>Profile: {html.escape(profile_name)}</span><span>Source: {html.escape(str(source_root))}</span></div>
<div class="summary"><span>Total: {len(proposals)}</span><span class="sc">✓ Clean: {cn}</span><span class="sr">⚠ Review: {rn}</span><span class="sd">✗ Dupes: {dn}</span></div>
<h2>Proposals</h2><table><thead><tr><th>Status</th><th>Conf</th><th>Original Folder</th><th>Proposed Name</th><th>Tracks</th><th>Format(s)</th><th>Reasons</th></tr></thead><tbody>{rows}</tbody></table>
</body></html>"""
    (session_dir/"report.html").write_text(html_out,encoding="utf-8")

# ─────────────────────────────────────────────
# Apply folder moves
# ─────────────────────────────────────────────
def collision_resolve(dst:Path,policy:str,suffix_fmt:str)->Optional[Path]:
    if not dst.exists(): return dst
    if policy=="skip": return None
    n=1
    while True:
        cand=Path(str(dst)+suffix_fmt.format(n=n))
        if not cand.exists(): return cand
        n+=1


# ─────────────────────────────────────────────────────────────────
# Intelligent duplicate resolution
# ─────────────────────────────────────────────────────────────────
def _norm_title(s:str)->str:
    """Normalise a title for fuzzy comparison: lowercase, strip numbers/punctuation."""
    s=(s or "").lower().strip()
    s=re.sub(r'^\d+[.\-_\s]+','',s)   # strip leading track number
    s=re.sub(r'[^\w\s]','',s)
    return re.sub(r'\s+',' ',s).strip()

def _title_similarity(a:str,b:str)->float:
    """Simple Jaccard word-set similarity for title matching."""
    wa=set(_norm_title(a).split()); wb=set(_norm_title(b).split())
    if not wa or not wb: return 0.0
    return len(wa&wb)/len(wa|wb)

def compare_with_existing(
    incoming_path:Path,
    existing_path:Path,
    incoming_tags:List[Dict],
    existing_tags:List[Dict],
    cfg:Dict[str,Any],
)->Dict[str,Any]:
    """
    Compare incoming folder against existing Clean folder.

    Returns a result dict:
        outcome:  exact_duplicate | missing_tracks | format_upgrade |
                  lower_quality_mp3 | partial_overlap | unknown
        missing_in_existing:  List[Path]  (files in incoming not in existing)
        matched:  int
        unmatched_existing: int
        incoming_formats: set
        existing_formats: set
    """
    dc=cfg.get("duplicates",{})
    title_thresh=float(dc.get("title_match_threshold",0.90))
    size_tol    =float(dc.get("size_match_tolerance",0.01))

    inc_audio=[p for p in incoming_path.iterdir()
               if p.is_file() and p.suffix.lower() in {".mp3",".flac",".m4a",".aiff",".wav",".ogg",".opus"}]
    ex_audio =[p for p in existing_path.iterdir()
               if p.is_file() and p.suffix.lower() in {".mp3",".flac",".m4a",".aiff",".wav",".ogg",".opus"}]

    inc_formats={p.suffix.lower() for p in inc_audio}
    ex_formats ={p.suffix.lower() for p in ex_audio}

    # Build title→path maps from tags
    def tag_map(files,tags_list):
        m={}
        for f,t in zip(files,tags_list or [{}]*len(files)):
            title=(t or {}).get("title") or _norm_title(f.stem)
            m[_norm_title(title)]=f
        return m

    inc_map=tag_map(inc_audio,incoming_tags)
    ex_map =tag_map(ex_audio, existing_tags)

    # Find tracks in incoming not matched in existing
    missing_in_existing:List[Path]=[]
    for inc_title,inc_file in inc_map.items():
        best=max(((_title_similarity(inc_title,ex_t),ex_t) for ex_t in ex_map), default=(0,""))
        if best[0]<title_thresh:
            missing_in_existing.append(inc_file)

    matched=len(inc_map)-len(missing_in_existing)

    # Determine outcome
    # Format upgrade: incoming is FLAC, existing is MP3 only
    if ".flac" in inc_formats and ".flac" not in ex_formats and matched>=max(1,len(inc_map)-1):
        outcome="format_upgrade"
    # Lower quality: incoming is MP3, existing has FLAC
    elif ".flac" in ex_formats and ".flac" not in inc_formats and matched>=max(1,len(inc_map)-1):
        outcome="lower_quality_mp3"
    # Exact duplicate: all tracks match, similar sizes
    elif not missing_in_existing and inc_formats==ex_formats:
        # Check size similarity
        inc_total=sum(f.stat().st_size for f in inc_audio)
        ex_total =sum(f.stat().st_size for f in ex_audio)
        size_ok=abs(inc_total-ex_total)/max(ex_total,1)<=size_tol if ex_total else True
        outcome="exact_duplicate" if size_ok else "partial_overlap"
    elif missing_in_existing and matched>0:
        outcome="missing_tracks"
    else:
        outcome="partial_overlap"

    return {
        "outcome":outcome,
        "missing_in_existing":missing_in_existing,
        "matched":matched,
        "unmatched_existing":len(ex_map)-matched,
        "incoming_formats":inc_formats,
        "existing_formats":ex_formats,
        "incoming_count":len(inc_audio),
        "existing_count":len(ex_audio),
    }

def merge_missing_tracks(
    missing_files:List[Path],
    existing_path:Path,
    cfg:Dict[str,Any],
    session_id:str,
    dry_run:bool=False
)->List[str]:
    """
    Copy missing tracks into existing_path and re-run track rename on the folder.
    Returns list of copied filenames.
    """
    copied=[]
    hist_path=Path(cfg.get("logging",{}).get("track_history_log","logs/track_history.jsonl"))
    for f in missing_files:
        dst=existing_path/f.name
        if dst.exists():
            # avoid collision
            dst=existing_path/(f.stem+"_merged"+f.suffix)
        if dry_run:
            out(f"    {C.DIM}[merge dry-run]{C.RESET} would copy: {f.name}")
            copied.append(f.name); continue
        try:
            shutil.copy2(str(f),str(dst))
            copied.append(f.name)
            append_jsonl(hist_path,{"action_id":uuid.uuid4().hex[:10],"timestamp":now_iso(),
                "session_id":session_id,"type":"track_merge","src":str(f),"dst":str(dst)})
        except Exception as e:
            err(f"    merge copy failed ({f.name}): {e}")
    return copied

# ─────────────────────────────────────────────────────────────────
# v7.0 — Interactive folder-by-folder review mode
# ─────────────────────────────────────────────────────────────────

def _conf_bar(c:float,width:int=20)->str:
    """Render a confidence bar: ████████░░░░ 0.75"""
    filled=int(c*width); empty=width-filled
    if c>=0.80: color=C.GREEN
    elif c>=0.50: color=C.YELLOW
    else: color=C.RED
    return f"{color}{'█'*filled}{'░'*empty}{C.RESET}  {c:.2f}"

def _factor_bar(name:str,val:float,width:int=20)->str:
    """Render a single factor bar line."""
    desc=_FACTOR_DESCRIPTIONS.get(name,name)
    filled=int(val*width); empty=width-filled
    if val>=0.80: color=C.GREEN
    elif val>=0.50: color=C.YELLOW
    else: color=C.RED
    return f"    {desc:28s} {color}{'█'*filled}{'░'*empty}{C.RESET}  {val:.2f}"

def _display_folder_card(idx:int,total:int,p:FolderProposal)->None:
    """Print the interactive review card for one folder."""
    d=p.decision; factors=d.get("confidence_factors",{})
    src_name=p.folder_name; dst_name=p.proposed_folder_name
    dest_upper=p.destination.upper()
    dest_color=C.GREEN if p.destination=="clean" else (C.YELLOW if p.destination=="review" else C.RED)

    # ── Header ────────────────────────────────────────────────
    print(f"\n{'━'*70}  [{idx}/{total}]")

    # ── Confidence + route (top line, instant gut read) ───────
    print(f"  {_conf_bar(p.confidence)}   {dest_color}{dest_upper}{C.RESET}")

    # ── Warning flags (only if problems exist) ────────────────
    reasons=d.get("route_reasons",[])
    if reasons:
        flags=" · ".join(reasons)
        flag_color=C.RED if p.confidence<0.60 else C.YELLOW
        print(f"  {flag_color}▸ {flags}{C.RESET}")

    # ── FROM / TO (the decision point) ────────────────────────
    print()
    print(f"  {C.DIM}FROM  {src_name}{C.RESET}")
    if src_name!=dst_name:
        print(f"    {C.BOLD}→ {dst_name}{C.RESET}")
    else:
        print(f"    {C.DIM}→ (unchanged){C.RESET}")

    # ── Metadata (compact, glanceable) ────────────────────────
    artist=d.get("albumartist_display","--")
    album=d.get("dominant_album_display","")
    year=d.get("year","")
    va_flag=f"  {C.CYAN}VA{C.RESET}" if d.get("is_va") else ""
    mix_flag=f"  {C.MAGENTA}MIX{C.RESET}" if d.get("is_mix") else ""
    ep_flag=f"  {C.CYAN}EP{C.RESET}" if d.get("is_ep") else ""
    ext_str=", ".join(f"{k.upper()} {v}" for k,v in (p.stats.extensions or {}).items()) or "--"
    year_str=f"  {C.DIM}({year}){C.RESET}" if year else ""
    tagged=d.get("tagged_count",p.stats.tracks_total); total_t=p.stats.tracks_total
    tag_ratio=f"{tagged}/{total_t}" if tagged!=total_t else str(total_t)
    genre=d.get("genre","")
    genre_str=f"  {C.DIM}{genre}{C.RESET}" if genre else ""
    album_str=f"  {C.DIM}-  {album}{C.RESET}" if album else ""
    # Per-folder size
    folder_bytes=_folder_size(Path(p.folder_path)) if Path(p.folder_path).exists() else 0
    size_str=f"  ·  {_human_size(folder_bytes)}" if folder_bytes else ""
    print(f"\n  {C.BOLD}{artist}{C.RESET}{album_str}{ep_flag}{va_flag}{mix_flag}{year_str}")
    print(f"  {C.DIM}{tag_ratio} tracks  ·  {ext_str}{size_str}{genre_str}{C.RESET}")

    # ── Album vs VA barometer ─────────────────────────────────
    dom_art_share=float(d.get("dominant_artist_share",1.0))
    is_va=bool(d.get("is_va",False))
    _BAR=16
    def _va_bar(share:float)->str:
        # share = dominant artist share (high = album, low = VA)
        album_fill=int(share*_BAR); va_fill=_BAR-album_fill
        album_col=C.GREEN if share>=0.75 else (C.YELLOW if share>=0.40 else C.RED)
        va_col=C.RED if share>=0.75 else (C.YELLOW if share>=0.40 else C.GREEN)
        album_bar=f"{album_col}{'█'*album_fill}{C.RESET}{C.DIM}{'░'*va_fill}{C.RESET}"
        va_bar=f"{C.DIM}{'░'*album_fill}{C.RESET}{va_col}{'█'*va_fill}{C.RESET}"
        return (f"  {C.BOLD}ALBUM{C.RESET} {album_bar} {share:.0%}  ·  "
                f"{C.BOLD}VA{C.RESET} {va_bar} {1-share:.0%}  "
                f"{C.DIM}(current: {'VA' if is_va else 'Album'}  ·  v to toggle){C.RESET}")
    print(f"\n{_va_bar(dom_art_share)}")

    # ── Factor bars (compact) ─────────────────────────────────
    display_factors=["tag_coverage","dominance","title_quality","filename_consistency",
                     "completeness","aa_consistency","folder_alignment"]
    shown=[f for f in display_factors if f in factors and isinstance(factors[f],float)]
    if shown:
        print()
        for f in shown:
            print(_factor_bar(f,factors[f]))

    # ── Review summary ────────────────────────────────────────
    summary=d.get("review_summary","")
    if summary:
        print(f"\n  {C.DIM}{summary}{C.RESET}")

    print(f"{'━'*70}")

def _diff_old_highlight(old:str, new:str)->str:
    """Return old string with removed/changed portions highlighted in red, rest dimmed.
    Used to make the 'was:' line scannable at a glance."""
    sm=difflib.SequenceMatcher(None, old, new, autojunk=False)
    out=C.DIM
    for op,i1,i2,_j1,_j2 in sm.get_opcodes():
        chunk=old[i1:i2]
        if op=="equal":
            out+=chunk
        elif op in ("replace","delete"):
            # Un-dim and color the removed portion so it pops against the dim baseline
            out+=C.RESET+C.RED+chunk+C.RESET+C.DIM
    out+=C.RESET
    return out

def _display_tracks(p:FolderProposal,cfg:Optional[Dict[str,Any]]=None)->Optional[str]:
    """Show track listing with rename preview for current folder.
    For folders with >30 tracks, shows pages of 20 with n/p navigation.
    Returns 'approve' if user presses b to batch-approve from within the view."""
    folder=Path(p.folder_path)
    if not folder.exists():
        print(f"  {C.RED}Folder not found on disk.{C.RESET}")
        return
    exts={".mp3",".flac",".m4a",".aiff",".wav",".ogg",".opus",".wma"}
    files=sorted([f for f in folder.iterdir() if f.suffix.lower() in exts],key=lambda f:f.name)
    if not files:
        print(f"  {C.DIM}No audio files found.{C.RESET}")
        return

    # If cfg is available, compute rename previews
    rename_map:Dict[str,str]={}
    classification=""
    if cfg is not None:
        try:
            classification=classify_folder_for_tracks(p.decision,cfg)
            disc_multi=folder_is_multidisc(files,cfg)
            for f in files:
                tags=read_audio_tags(f,cfg)
                new_name,conf,reason,meta=build_track_filename(classification,tags,f,cfg,p.decision,disc_multi,total_tracks=len(files))
                if new_name and normalize_unicode(new_name)!=normalize_unicode(f.name):
                    rename_map[f.name]=new_name
        except Exception:
            pass  # fall back to simple listing

    type_label=f"  {C.DIM}[{classification}]{C.RESET}" if classification else ""
    changes=len(rename_map)
    unchanged=len(files)-changes

    def _print_track(i:int, f:Path)->None:
        new_name=rename_map.get(f.name)
        if new_name:
            # New name is the main event — left-aligned, normal weight
            print(f"   {i+1:>2}  {new_name}")
            # Old name below: dim with removed portions highlighted red
            old_hl=_diff_old_highlight(f.name, new_name)
            print(f"       {C.DIM}was:{C.RESET} {old_hl}")
        else:
            print(f"   {C.DIM}{i+1:>2}  {f.name}{C.RESET}")

    BATCH=20
    if len(files)>30:
        # Large folder — paginate in batches of 20; b to batch-approve
        total_pages=(len(files)+BATCH-1)//BATCH
        page_idx=0
        while True:
            start=page_idx*BATCH; end=min(start+BATCH,len(files))
            print(f"\n  TRACKS ({len(files)} files){type_label}  ·  {C.GREEN}{changes} rename(s){C.RESET}  {C.DIM}{unchanged} unchanged  [Batch {page_idx+1}/{total_pages}]{C.RESET}")
            print(f"  {'─'*60}")
            for i,f in enumerate(files[start:end],start=start):
                _print_track(i,f)
            print(f"  {'─'*60}")
            print(f"  n:next-batch  p:prev-batch  {C.GREEN}b:approve-folder{C.RESET}  Enter:back")
            try:
                resp=input(f"  > ").strip().lower()
            except (KeyboardInterrupt,EOFError):
                break
            if resp=="n":
                page_idx=min(page_idx+1,total_pages-1)
            elif resp=="p":
                page_idx=max(page_idx-1,0)
            elif resp=="b":
                return "approve"
            else:
                break
    else:
        print(f"\n  TRACKS ({len(files)} files){type_label}  ·  {C.GREEN}{changes} rename(s){C.RESET}  {C.DIM}{unchanged} unchanged{C.RESET}")
        print(f"  {'─'*60}")
        page=30
        shown=min(len(files),page)
        for i,f in enumerate(files[:shown]):
            _print_track(i,f)
        if len(files)>shown:
            remaining=len(files)-shown
            try:
                resp=input(f"   {C.DIM}... and {remaining} more  [Enter to see all, any key to skip]{C.RESET} ").strip()
                if not resp:
                    for i,f in enumerate(files[shown:],start=shown):
                        _print_track(i,f)
            except (KeyboardInterrupt,EOFError):
                print()
        print(f"  {'─'*60}")
    return None

def _edit_track_title(f:Path,cfg:Dict[str,Any])->Optional[str]:
    """Edit the title tag of a single audio file interactively.
    Returns the new title if saved, None if cancelled."""
    if MutagenFile is None:
        print(f"  {C.RED}mutagen not available — cannot edit tags.{C.RESET}")
        return None
    tags=read_audio_tags(f,cfg)
    current=tags.get("title") or ""
    print(f"\n  {'─'*50}")
    print(f"  File:  {f.name}")
    print(f"  Title: {C.BOLD}{current or '(none)'}{C.RESET}")
    try:
        new_title=input(f"  New title (Enter to cancel): ").strip()
    except (KeyboardInterrupt,EOFError):
        print(); return None
    if not new_title:
        print(f"  {C.DIM}Cancelled.{C.RESET}"); return None
    try:
        mf=MutagenFile(str(f),easy=True)
        if mf is None:
            print(f"  {C.RED}Cannot open file for writing.{C.RESET}"); return None
        if mf.tags is None:
            mf.add_tags()
        mf.tags["title"]=[new_title]
        mf.save()
        # Update tag cache so rename preview reflects the change immediately
        cache=_tag_cache
        if cache is not None:
            updated=dict(tags); updated["title"]=new_title
            cache.set(f,updated)
        print(f"  {C.GREEN}✓ Title saved: {new_title}{C.RESET}")
        return new_title
    except Exception as ex:
        print(f"  {C.RED}Write failed: {ex}{C.RESET}"); return None

def _interactive_action_help()->None:
    """Print the action key reference."""
    print(f"\n  {'─'*50}")
    print(f"  INTERACTIVE REVIEW — KEYS")
    print(f"  {'─'*50}")
    print(f"  {C.BOLD}── Decide ──{C.RESET}")
    print(f"  z  or  Enter    Move to Clean/")
    print(f"  x               Reject → Review/ (with reason)")
    print(f"  c               Skip (leave in source)")
    print(f"  f               Flag (review again at end)")
    print(f"  {C.BOLD}── Fix ──{C.RESET}")
    print(f"  e               Edit album title  (renames the folder)")
    print(f"  e<N>            Edit track N title tag  (e.g. e3, e12)  — separate from album")
    print(f"  a               Set artist name")
    print(f"  v               Toggle VA status")
    print(f"  {C.BOLD}── Inspect ──{C.RESET}")
    print(f"  o               Open folder in Finder")
    print(f"  R               Rescan folder (after manual changes in Finder)")
    print(f"  space / b       Track rename preview  (b within view to approve folder)")
    print(f"  u               Undo a move made this session")
    print(f"  ?               This help")
    print(f"  {'─'*50}")
    print(f"  q               Quit")
    print(f"  {'─'*50}")

def _prompt_review_note()->str:
    """Prompt for a review reason. Shows presets, allows custom text or blank."""
    print(f"\n  {C.DIM}Reason?{C.RESET}")
    for i,r in enumerate(REVIEW_REASON_PRESETS,1):
        print(f"    {i}. {r}")
    print(f"    {len(REVIEW_REASON_PRESETS)+1}. Custom note")
    print(f"    {C.DIM}Enter = no reason{C.RESET}")
    while True:
        choice=input(f"  Reason (1-{len(REVIEW_REASON_PRESETS)+1}, text, or Enter): ").strip()
        if not choice: return ""  # allow blank — no reason
        try:
            idx=int(choice)
            if 1<=idx<=len(REVIEW_REASON_PRESETS):
                return REVIEW_REASON_PRESETS[idx-1]
            elif idx==len(REVIEW_REASON_PRESETS)+1:
                note=input("  Custom note: ").strip()
                if note: return note
                continue
        except ValueError:
            return choice  # free text

def interactive_review(cfg:Dict[str,Any],proposals:List[FolderProposal],
                       session_id:str,dry_run:bool=False,
                       threshold:Optional[float]=None,
                       source_root:Optional[Path]=None)->List[Dict[str,Any]]:
    """
    v7.0: Interactive folder-by-folder review mode.
    Sort by confidence ascending (worst first), display card, prompt for action.
    Returns list of applied move entries (same format as apply_folder_moves).
    """
    mc=cfg.get("move",{})
    policy=mc.get("on_collision","suffix"); sfmt=mc.get("suffix_format"," ({n})")
    use_cs=bool(mc.get("use_checksum",False))
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    hist_path=Path(cfg["logging"]["history_log"]); skip_path=Path(cfg["logging"]["skipped_log"])

    # Filter by threshold if specified
    if threshold is not None:
        proposals=[p for p in proposals if p.confidence<threshold]
        if not proposals:
            out(f"\n  {C.GREEN}All folders above threshold {threshold:.2f}. Nothing to review.{C.RESET}")
            return []

    # Sort by confidence ascending — hardest decisions first
    proposals.sort(key=lambda p:p.confidence)

    total=len(proposals)
    applied:List[Dict[str,Any]]=[]
    skipped=0; sent_to_review=0; overrides=0

    # Session header
    clean_n=sum(1 for p in proposals if p.destination=="clean")
    rev_n=sum(1 for p in proposals if p.destination=="review")
    # Pre-compute per-folder sizes for progress tracking
    _review_sizes:Dict[str,int]={}
    for p in proposals:
        _review_sizes[p.folder_path]=_folder_size(Path(p.folder_path)) if Path(p.folder_path).exists() else 0
    _review_total_bytes=sum(_review_sizes.values())
    _review_bytes_done=0
    readchar_note=f"  {C.DIM}(single-keypress mode){C.RESET}" if _HAS_READCHAR and _IS_TTY else ""
    print(f"\n{'═'*66}")
    print(f"  {C.BOLD}RAAGDOSA v{APP_VERSION}  ·  Interactive Review  ·  Session {session_id[:12]}{C.RESET}")
    print(f"{'─'*66}")
    print(f"  Folders: {total} ({_human_size(_review_total_bytes)})  ·  {C.GREEN}Clean: {clean_n}{C.RESET}  ·  {C.YELLOW}Review: {rev_n}{C.RESET}")
    print(f"  Sorted by confidence (lowest first)")
    if dry_run: print(f"  {C.YELLOW}DRY RUN — nothing will be moved{C.RESET}")
    print(f"  Press ? for help at any prompt.{readchar_note}")
    print(f"{'═'*66}")

    # Session notes file for learning
    session_notes_path=Path(cfg["logging"]["session_dir"])/session_id/"review_notes.jsonl"

    for idx,p in enumerate(proposals,1):
        if should_stop():
            out(f"\n{C.YELLOW}Stopped. {len(applied)}/{total} applied.{C.RESET}")
            break

        src=Path(p.folder_path); dst=Path(p.target_path)
        if not src.exists():
            _review_bytes_done+=_review_sizes.get(p.folder_path,0); continue

        _display_folder_card(idx,total,p)
        _display_tracks(p,cfg)

        # Build track file list for e<N> edit shortcut
        _tf_exts={".mp3",".flac",".m4a",".aiff",".wav",".ogg",".opus",".wma"}
        track_files=sorted([f for f in src.iterdir() if f.suffix.lower() in _tf_exts],key=lambda f:f.name) if src.exists() else []

        # Progress line — size-weighted percentage
        _cur_bytes=_review_sizes.get(p.folder_path,0)
        size_pct=int(_review_bytes_done/_review_total_bytes*100) if _review_total_bytes else 0
        dest_color=C.GREEN if p.destination=="clean" else C.YELLOW
        print(f"  {C.DIM}[{idx}/{total}] {size_pct}% by size{C.RESET}  ·  {p.proposed_folder_name}  ({conf_color(p.confidence)} → {dest_color}{p.destination.title()}{C.RESET})")

        # Action loop — stays on this folder until an action advances
        while True:
            if _HAS_READCHAR and _IS_TTY:
                action_labels=f"  z:move  x:reject  c:skip  b:tracks  e:edit  v:va  o:finder  R:rescan  q:quit  ?:help"
            else:
                action_labels=f"  z:move  x:reject  c:skip  space/b:tracks  e:album-title  e<N>:track-title  a:artist  v:va  o:finder  R:rescan  q:quit  ?:help"
            print(action_labels)
            choice=_read_key("  > ")

            if choice=="":
                # Empty enter — require explicit choice
                print(f"  {C.YELLOW}Press z to move, x to reject, c to skip, ? for help{C.RESET}")
                continue
            if choice in ("z","y","yes"):
                # Approve — proceed with the proposed move
                dst2=collision_resolve(dst,policy,sfmt)
                if dst2 is None:
                    warn(f"Collision skip: {dst.name}")
                    append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"collision_skip","src":str(src)})
                    break
                if dry_run:
                    out(f"  {C.DIM}[dry-run]{C.RESET} {src.name}  →  {dst2.name}  {status_tag(p.destination)}")
                    append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"dry_run","src":str(src),"dst":str(dst2)})
                    break
                ensure_dir(dst2.parent)
                try:
                    move_method,move_elapsed=safe_move_folder(src,dst2,use_checksum=use_cs)
                except RuntimeError as e:
                    err(f"  Move failed: {e}")
                    append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":f"move_failed:{e}","src":str(src)})
                    break
                action_id=uuid.uuid4().hex[:10]
                entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,"type":"folder",
                       "original_path":str(src),"original_parent":str(src.parent),"original_folder_name":src.name,
                       "target_path":str(dst2),"target_parent":str(dst2.parent),"target_folder_name":dst2.name,
                       "destination":p.destination,"confidence":p.confidence,"decision":p.decision,
                       "move_method":move_method,"interactive_action":"approved"}
                append_jsonl(hist_path,entry); applied.append(entry)
                if p.destination=="clean": manifest_add(cfg,dst2.name,{"original_path":str(src),"confidence":p.confidence,"session_id":session_id})
                elif p.destination=="review": _write_review_sidecar(dst2,p,session_id)
                out(f"  {C.GREEN}✓ Approved{C.RESET} → {dst2.name}")
                if source_root:
                    try: cleanup_empty_parents(src,source_root)
                    except Exception: pass
                break

            elif choice in ("c","k","s"):
                # Skip — leave in source
                reason_text=input(f"  Reason (Enter to skip): ").strip()
                skip_entry={"timestamp":now_iso(),"session_id":session_id,"type":"folder",
                            "reason":"user_skipped","src":str(src),"interactive_action":"skipped"}
                if reason_text:
                    skip_entry["user_note"]=reason_text
                    ensure_dir(session_notes_path.parent)
                    append_jsonl(session_notes_path,{"folder":src.name,"action":"skipped","note":reason_text,"timestamp":now_iso(),"confidence":p.confidence})
                append_jsonl(skip_path,skip_entry)
                skipped+=1
                out(f"  {C.DIM}→ Skipped{C.RESET}")
                break

            elif choice in ("x","d","r"):
                # Send to Review with required note
                note=_prompt_review_note()
                # Re-route to review
                profile_name=cfg.get("active_profile","incoming")
                profiles=cfg.get("profiles",{})
                profile_obj=profiles.get(profile_name,{})
                sr=source_root or Path(profile_obj.get("source_root","")).expanduser()
                review_albums=derive_review_albums_root(profile_obj,sr)
                review_dst=review_albums/p.proposed_folder_name
                review_dst2=collision_resolve(review_dst,policy,sfmt)
                if review_dst2 is None:
                    warn(f"Review collision skip: {review_dst.name}")
                    break
                p.destination="review"; p.decision["route_reasons"]=p.decision.get("route_reasons",[])+["user_review"]
                p.decision["user_review_note"]=note
                if dry_run:
                    out(f"  {C.DIM}[dry-run]{C.RESET} → Review: {note}")
                    break
                ensure_dir(review_dst2.parent)
                try:
                    move_method,move_elapsed=safe_move_folder(src,review_dst2,use_checksum=use_cs)
                except RuntimeError as e:
                    err(f"  Move failed: {e}"); break
                action_id=uuid.uuid4().hex[:10]
                entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,"type":"folder",
                       "original_path":str(src),"original_parent":str(src.parent),"original_folder_name":src.name,
                       "target_path":str(review_dst2),"target_parent":str(review_dst2.parent),"target_folder_name":review_dst2.name,
                       "destination":"review","confidence":p.confidence,"decision":p.decision,
                       "move_method":move_method,"interactive_action":"sent_to_review","user_note":note}
                append_jsonl(hist_path,entry); applied.append(entry)
                _write_review_sidecar(review_dst2,p,session_id)
                # Log note for learning
                ensure_dir(session_notes_path.parent)
                append_jsonl(session_notes_path,{"folder":src.name,"action":"sent_to_review","note":note,"timestamp":now_iso(),"confidence":p.confidence})
                sent_to_review+=1
                out(f"  {C.YELLOW}→ Review:{C.RESET} {note}")
                if source_root:
                    try: cleanup_empty_parents(src,source_root)
                    except Exception: pass
                break

            elif choice=="a":
                # Set artist override
                current=p.decision.get("albumartist_display","--")
                print(f"  Current artist: {current}")
                new_artist=input(f"  New artist name: ").strip()
                if not new_artist: continue
                # Update proposal
                p.decision["albumartist_display"]=new_artist
                p.decision["is_va"]=False
                p.decision["override_type"]="set_artist"
                p.decision["override_original"]=current
                # Re-derive destination path
                profile_name=cfg.get("active_profile","incoming")
                profiles=cfg.get("profiles",{})
                profile_obj=profiles.get(profile_name,{})
                sr=source_root or Path(profile_obj.get("source_root","")).expanduser()
                clean_albums=derive_clean_albums_root(profile_obj,sr)
                new_target=resolve_library_path(clean_albums,new_artist,p.decision.get("dominant_album_display",""),
                                               p.decision.get("year"),p.decision.get("is_flac_only",False),
                                               False,False,p.decision.get("is_mix",False),cfg,profile_obj,
                                               genre=p.decision.get("genre"),bpm=p.decision.get("bpm"),
                                               key=p.decision.get("key"),label=p.decision.get("label"))
                p.proposed_folder_name=_apply_format_suffix(new_target.name,cfg,p.stats.extensions if p.stats else None)
                p.target_path=str(new_target); p.destination="clean"
                overrides+=1
                print(f"\n  {C.CYAN}─ UPDATED ─{C.RESET}")
                print(f"  Artist: {C.BOLD}{new_artist}{C.RESET}  (was: {current})   VA: No")
                print(f"    TO:   {new_target.name}")
                print(f"  ROUTE: {C.GREEN}CLEAN{C.RESET}")
                # Stay in action loop — user can approve or adjust further

            elif choice=="v":
                # Toggle VA status
                was_va=p.decision.get("is_va",False)
                if was_va:
                    # VA → single artist: need artist name
                    new_artist=input(f"  Artist name: ").strip()
                    if not new_artist: continue
                    p.decision["is_va"]=False
                    p.decision["albumartist_display"]=new_artist
                    p.decision["override_type"]="unmark_va"
                    overrides+=1
                    # Re-derive path
                    profile_name=cfg.get("active_profile","incoming")
                    profiles=cfg.get("profiles",{})
                    profile_obj=profiles.get(profile_name,{})
                    sr=source_root or Path(profile_obj.get("source_root","")).expanduser()
                    clean_albums=derive_clean_albums_root(profile_obj,sr)
                    new_target=resolve_library_path(clean_albums,new_artist,p.decision.get("dominant_album_display",""),
                                                   p.decision.get("year"),p.decision.get("is_flac_only",False),
                                                   False,False,p.decision.get("is_mix",False),cfg,profile_obj,
                                                   genre=p.decision.get("genre"),bpm=p.decision.get("bpm"),
                                                   key=p.decision.get("key"),label=p.decision.get("label"))
                    p.proposed_folder_name=_apply_format_suffix(new_target.name,cfg,p.stats.extensions if p.stats else None)
                    p.target_path=str(new_target); p.destination="clean"
                    print(f"\n  {C.CYAN}─ UPDATED ─{C.RESET}")
                    print(f"  Artist: {C.BOLD}{new_artist}{C.RESET}   VA→Album  (tracks will rename as: NN - Title)")
                    print(f"    TO:   {p.proposed_folder_name}")
                    print(f"  {C.DIM}Press space/b to preview updated track renames.{C.RESET}")
                else:
                    # Single artist → VA
                    old_artist=p.decision.get("albumartist_display","--")
                    p.decision["is_va"]=True
                    p.decision["albumartist_display"]="Various Artists"
                    p.decision["override_type"]="mark_va"
                    overrides+=1
                    profile_name=cfg.get("active_profile","incoming")
                    profiles=cfg.get("profiles",{})
                    profile_obj=profiles.get(profile_name,{})
                    sr=source_root or Path(profile_obj.get("source_root","")).expanduser()
                    clean_albums=derive_clean_albums_root(profile_obj,sr)
                    new_target=resolve_library_path(clean_albums,"Various Artists",p.decision.get("dominant_album_display",""),
                                                   p.decision.get("year"),p.decision.get("is_flac_only",False),
                                                   True,False,p.decision.get("is_mix",False),cfg,profile_obj,
                                                   genre=p.decision.get("genre"),bpm=p.decision.get("bpm"),
                                                   key=p.decision.get("key"),label=p.decision.get("label"))
                    p.proposed_folder_name=_apply_format_suffix(new_target.name,cfg,p.stats.extensions if p.stats else None)
                    p.target_path=str(new_target)
                    print(f"\n  {C.CYAN}─ UPDATED ─{C.RESET}")
                    print(f"  Album→VA  (was: {old_artist})  tracks will rename as: NN - Artist - Title")
                    print(f"    TO:   {new_target.name}")
                    print(f"  {C.DIM}Press space/b to preview updated track renames.{C.RESET}")

            elif choice=="e":
                # Edit the album/folder title (e = album edit, e<N> = track title edit)
                current_album=p.decision.get("dominant_album_display","")
                print(f"  {C.DIM}Current album:{C.RESET} {current_album}")
                new_album=input(f"  New album title (Enter to cancel): ").strip()
                if not new_album: continue
                p.decision["dominant_album_display"]=new_album
                p.decision["override_type"]=p.decision.get("override_type","")+"edit_title"
                profile_name=cfg.get("active_profile","incoming")
                profiles=cfg.get("profiles",{}); profile_obj=profiles.get(profile_name,{})
                sr=source_root or Path(profile_obj.get("source_root","")).expanduser()
                clean_albums=derive_clean_albums_root(profile_obj,sr)
                new_target=resolve_library_path(clean_albums,p.decision.get("albumartist_display",""),new_album,
                                               p.decision.get("year"),p.decision.get("is_flac_only",False),
                                               p.decision.get("is_va",False),False,p.decision.get("is_mix",False),cfg,profile_obj,
                                               genre=p.decision.get("genre"),bpm=p.decision.get("bpm"),
                                               key=p.decision.get("key"),label=p.decision.get("label"))
                p.proposed_folder_name=_apply_format_suffix(new_target.name,cfg,p.stats.extensions if p.stats else None)
                p.target_path=str(new_target); p.destination="clean"
                overrides+=1
                print(f"\n  {C.CYAN}─ UPDATED ─{C.RESET}")
                print(f"  Album: {C.BOLD}{new_album}{C.RESET}  (was: {current_album})")
                print(f"    TO:   {new_target.name}")
                print(f"  ROUTE: {C.GREEN}CLEAN{C.RESET}")

            elif choice.startswith("e") and len(choice)>1 and choice[1:].strip().isdigit():
                # e<N> — edit individual track title tag (separate from album title)
                track_num=int(choice[1:].strip())
                if 1<=track_num<=len(track_files):
                    _edit_track_title(track_files[track_num-1],cfg)
                    print(f"  {C.DIM}Press space/b to refresh track view.{C.RESET}")
                else:
                    print(f"  {C.DIM}Track {track_num} not found (folder has {len(track_files)} tracks).{C.RESET}")

            elif choice in (" ","b"):
                trk_result=_display_tracks(p,cfg)
                if trk_result=="approve":
                    # User approved from batch track view — execute move inline
                    dst2=collision_resolve(dst,policy,sfmt)
                    if dst2 is None:
                        warn(f"Collision skip: {dst.name}")
                        append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"collision_skip","src":str(src)})
                        break
                    if dry_run:
                        out(f"  {C.DIM}[dry-run]{C.RESET} {src.name}  →  {dst2.name}  {status_tag(p.destination)}")
                        append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"dry_run","src":str(src),"dst":str(dst2)})
                        break
                    ensure_dir(dst2.parent)
                    try:
                        move_method,move_elapsed=safe_move_folder(src,dst2,use_checksum=use_cs)
                    except RuntimeError as e:
                        err(f"  Move failed: {e}")
                        append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":f"move_failed:{e}","src":str(src)})
                        break
                    action_id=uuid.uuid4().hex[:10]
                    entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,"type":"folder",
                           "original_path":str(src),"original_parent":str(src.parent),"original_folder_name":src.name,
                           "target_path":str(dst2),"target_parent":str(dst2.parent),"target_folder_name":dst2.name,
                           "destination":p.destination,"confidence":p.confidence,"decision":p.decision,
                           "move_method":move_method,"interactive_action":"approved"}
                    append_jsonl(hist_path,entry); applied.append(entry)
                    if p.destination=="clean": manifest_add(cfg,dst2.name,{"original_path":str(src),"confidence":p.confidence,"session_id":session_id})
                    elif p.destination=="review": _write_review_sidecar(dst2,p,session_id)
                    out(f"  {C.GREEN}✓ Batch-approved{C.RESET} → {dst2.name}")
                    if source_root:
                        try: cleanup_empty_parents(src,source_root)
                        except Exception: pass
                    break

            elif choice=="o":
                _open_in_finder(src)

            elif choice in ("R",) or (not _HAS_READCHAR and choice in ("rescan",)):
                # Rescan folder after manual changes (e.g. moved tracks in Finder)
                if not src.exists():
                    print(f"  {C.RED}Folder no longer exists.{C.RESET}"); break
                print(f"  {C.CYAN}Rescanning...{C.RESET}")
                new_audio=list_audio_files(src,exts,False)
                min_trk=int(cfg.get("scan",{}).get("min_tracks",3))
                if len(new_audio)<min_trk:
                    print(f"  {C.YELLOW}Only {len(new_audio)} audio file(s) remain — below minimum ({min_trk}).{C.RESET}")
                    print(f"  {C.DIM}Skip this folder? [y/N]{C.RESET}")
                    skip_confirm=input(f"  > ").strip().lower()
                    if skip_confirm in ("y","yes"):
                        skipped+=1; break
                    continue
                profile_name_r=cfg.get("active_profile","incoming")
                profile_obj_r=cfg.get("profiles",{}).get(profile_name_r,{})
                sr_r=source_root or Path(profile_obj_r.get("source_root","")).expanduser()
                new_p=build_folder_proposal(src,new_audio,sr_r,profile_obj_r,cfg)
                if new_p is None:
                    print(f"  {C.YELLOW}Folder no longer qualifies after rescan.{C.RESET}"); break
                # Determine routing based on confidence
                rr=cfg.get("review_rules",{}); min_conf=float(rr.get("min_confidence_for_clean",0.85))
                if new_p.confidence<min_conf:
                    new_p.destination="review"
                    review_albums_r=derive_review_albums_root(profile_obj_r,sr_r)
                    new_p.target_path=str(review_albums_r/new_p.proposed_folder_name)
                # Replace the current proposal in-place
                p.stats=new_p.stats; p.confidence=new_p.confidence; p.decision=new_p.decision
                p.proposed_folder_name=new_p.proposed_folder_name; p.target_path=new_p.target_path
                p.destination=new_p.destination
                dst=Path(p.target_path)
                # Rebuild track file list
                track_files=sorted([f for f in src.iterdir() if f.suffix.lower() in _tf_exts],key=lambda f:f.name) if src.exists() else []
                # Re-display
                _display_folder_card(idx,total,p)
                _display_tracks(p,cfg)
                print(f"  {C.CYAN}─ RESCANNED ─{C.RESET}  {len(new_audio)} tracks  ·  conf {conf_color(p.confidence)}  → {p.destination.title()}")

            elif choice=="q":
                print(f"\n{'═'*66}")
                print(f"  Stop processing?")
                print(f"  Completed: {len(applied)} approved · {skipped} skipped · {sent_to_review} to review")
                print(f"  Remaining: {total-idx} folders will stay in source.")
                print(f"  Moves already made will NOT be undone.")
                print(f"{'─'*66}")
                confirm=input(f"  Confirm stop? [y/N]: ").strip().lower()
                if confirm in ("y","yes"):
                    out(f"\n  Session ended. {len(applied)} moved · {skipped} skipped · {total-idx} remaining.")
                    out(f"  Undo: raagdosa undo --session {session_id}")
                    return applied
                # Otherwise continue with this folder

            elif choice=="?":
                _interactive_action_help()

            else:
                print(f"  {C.DIM}Unknown action. Press ? for help.{C.RESET}")

        # Track bytes for size-weighted progress
        _review_bytes_done+=_cur_bytes

    # Session summary
    print(f"\n{'═'*66}")
    print(f"  {C.BOLD}SESSION COMPLETE{C.RESET}")
    print(f"{'─'*66}")
    approved_clean=sum(1 for a in applied if a.get("destination")=="clean")
    approved_review=sum(1 for a in applied if a.get("destination")=="review")
    print(f"  Approved to Clean:   {C.GREEN}{approved_clean}{C.RESET} folders")
    print(f"  Sent to Review:      {C.YELLOW}{approved_review}{C.RESET} folders")
    print(f"  Skipped:             {skipped} folders")
    if overrides: print(f"  Overrides applied:   {overrides}")
    print(f"{'─'*66}")
    print(f"  Undo:  raagdosa undo --session {session_id}")
    print(f"  Log:   raagdosa history --session {session_id}")
    print(f"{'═'*66}\n")

    return applied

def apply_folder_moves(cfg:Dict[str,Any],proposals:List[FolderProposal],interactive:bool,
                       auto_above:Optional[float],dry_run:bool,session_id:str,
                       source_root:Optional[Path]=None)->List[Dict[str,Any]]:
    mc=cfg.get("move",{})
    if not mc.get("enabled",True): return []
    policy=mc.get("on_collision","suffix"); sfmt=mc.get("suffix_format"," ({n})"); use_cs=bool(mc.get("use_checksum",False))
    dc=cfg.get("decision",{}); req_confirm=bool(dc.get("require_confirmation",True)); interactive_below=float(dc.get("interactive_below",0.92))
    dj=cfg.get("dj_safety",{}); halt_dj=bool(dj.get("halt_on_dj_databases",False)); warn_dj=bool(dj.get("warn_on_dj_databases",True))
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    hist_path=Path(cfg["logging"]["history_log"]); skip_path=Path(cfg["logging"]["skipped_log"])

    if not dry_run and proposals:
        try:
            dst_parent=Path(proposals[0].target_path).parent.parent
            total_size=sum(get_folder_size(Path(p.folder_path)) for p in proposals if Path(p.folder_path).exists())
            free=shutil.disk_usage(str(dst_parent)).free
            if free<int(total_size*1.10):
                err(f"⛔ Disk: need ~{total_size/1024/1024:.0f} MB, only {free/1024/1024:.0f} MB free. Aborting."); sys.exit(1)
        except Exception: pass

    applied:List[Dict[str,Any]]=[]
    # Size-aware progress bar — compute per-folder sizes up front
    _folder_sizes:Dict[str,int]={}
    for p in proposals:
        sp=Path(p.folder_path)
        _folder_sizes[p.folder_path]=get_folder_size(sp) if sp.exists() else 0
    _total_bytes=sum(_folder_sizes.values())
    prog=SizeProgress(_total_bytes,len(proposals),"Moving")

    for p in proposals:
        if should_stop(): out(f"\n{C.YELLOW}Stopped. {len(applied)}/{len(proposals)} applied.{C.RESET}"); break
        src=Path(p.folder_path); dst=Path(p.target_path)
        _cur_size=_folder_sizes.get(p.folder_path,0)
        if not src.exists():
            prog.tick(_cur_size,src.name)
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"missing_source","src":str(src)}); continue
        if not check_path_length(dst): warn(f"Path length >{260}: {dst}")
        locked=check_folder_locked(src,exts)
        if locked:
            warn(f"Locked files in {src.name}: {[lf.name for lf in locked[:3]]}")
            if interactive and input(f"  Skip '{src.name}'? [Y/n] ").strip().lower()!="n":
                prog.tick(_cur_size,src.name)
                append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"locked_files","src":str(src)}); continue
        if warn_dj:
            dj_dbs=find_dj_databases(src)
            if dj_dbs:
                warn(f"DJ databases in {src.name}: {', '.join(dj_dbs)}")
                if halt_dj:
                    prog.tick(_cur_size,src.name)
                    append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"dj_halt","src":str(src)}); continue
        dst2=collision_resolve(dst,policy,sfmt)
        if dst2 is None:
            warn(f"Collision skip: {dst.name}")
            prog.tick(_cur_size,src.name)
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"collision_skip","src":str(src)}); continue
        elif dst2!=dst: warn(f"Collision — renamed to: {dst2.name}")
        should_prompt=(interactive or (req_confirm and p.confidence<interactive_below)) and not(auto_above is not None and p.confidence>=auto_above)
        if should_prompt:
            # v6.1: Show richer context for review decisions
            _art_info=f"  artist: {p.decision.get('albumartist_display','?')}" if p.decision.get("albumartist_display") else ""
            _va_info=f"  {C.YELLOW}[VA]{C.RESET}" if p.decision.get("is_va") else ""
            _reasons=", ".join(p.decision.get("route_reasons",[]))
            print(f"\n  {C.DIM}FROM:{C.RESET} {src.name}")
            print(f"  {C.DIM}  TO:{C.RESET} {dst2.name}  {status_tag(p.destination)}  conf={conf_color(p.confidence)}{_va_info}")
            if _art_info or _reasons:
                print(f"  {C.DIM}    {_art_info}{'  |  ' if _art_info and _reasons else ''}{_reasons}{C.RESET}")
            if p.decision.get("used_heuristic"): warn("name from heuristic — no tags")
            if input("  Apply? [y/N] ").strip().lower()!="y":
                prog.tick(_cur_size,src.name)
                append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"user_skipped","src":str(src)}); continue
        if dry_run:
            same=_same_device(src,dst2)
            method_note=f"{C.DIM}[rename]{C.RESET}" if same else f"{C.DIM}[copy]{C.RESET}"
            out(f"  {C.DIM}[dry-run]{C.RESET} {method_note} {src.name}  →  {dst2.name}  {status_tag(p.destination)}  conf={conf_color(p.confidence)}")
            prog.tick(_cur_size,src.name)
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"dry_run","src":str(src),"dst":str(dst2)}); continue
        ensure_dir(dst2.parent)
        try: move_method,move_elapsed=safe_move_folder(src,dst2,use_checksum=use_cs)
        except RuntimeError as e:
            err(f"⛔ Move failed ({src.name}): {e}")
            prog.tick(_cur_size,src.name)
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":f"move_failed:{e}","src":str(src)}); continue
        action_id=uuid.uuid4().hex[:10]
        entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,"type":"folder",
               "original_path":str(src),"original_parent":str(src.parent),"original_folder_name":src.name,
               "target_path":str(dst2),"target_parent":str(dst2.parent),"target_folder_name":dst2.name,
               "destination":p.destination,"confidence":p.confidence,"decision":p.decision,
               "move_method":move_method}
        append_jsonl(hist_path,entry); applied.append(entry)
        if p.destination=="clean": manifest_add(cfg,dst2.name,{"original_path":str(src),"confidence":p.confidence,"session_id":session_id})
        elif p.destination=="review": _write_review_sidecar(dst2,p,session_id)
        method_tag=f"  {C.DIM}[{move_method} {move_elapsed*1000:.0f}ms]{C.RESET}" if _verbosity>=VERBOSE else ""
        out(f"  MOVED {status_tag(p.destination)} {C.DIM}{src.name}{C.RESET}  →  {dst2.name}  conf={conf_color(p.confidence)}{method_tag}")
        prog.tick(_cur_size,dst2.name)
        # Clean up empty parent directories left behind in source
        if source_root:
            try: cleanup_empty_parents(src,source_root)
            except Exception: pass
    prog.done()
    # Summary: show rename vs copy counts so user knows which path was taken
    if applied:
        renames=sum(1 for a in applied if a.get("move_method")=="rename")
        copies =sum(1 for a in applied if a.get("move_method")=="copy")
        if renames and copies:
            out(f"  {C.DIM}Move methods: {renames} instant rename + {copies} cross-device copy{C.RESET}")
        elif renames:
            out(f"  {C.DIM}All {renames} folder(s) moved via instant rename (same filesystem){C.RESET}")
        elif copies:
            out(f"  {C.DIM}All {copies} folder(s) copied across devices{C.RESET}")
    return applied

# ─────────────────────────────────────────────
# Track renaming
# ─────────────────────────────────────────────
_TLDS="com|net|org|info|biz|co|io|me|fm|tv|cc|us|uk|de|fr|es|it|nl|ru|br|mx|in|jp|cn|au|ca|ch|se|no|fi|dk|pl|cz"

# v6.2: URL/domain garbage detection for tag values
_GARBAGE_URL_RE=re.compile(
    r"(^https?://)"                        # starts with http(s)://
    r"|(^www\.)"                            # starts with www.
    rf"|(\b[\w\-]+\.({_TLDS})\b)",          # contains domain.tld
    re.IGNORECASE
)
# Musical key names that get mis-tagged as artist (e.g. key=Em stored in artist field)
_MUSICAL_KEY_GARBAGE=frozenset({
    "a","ab","am","b","bb","bbm","bm","c","cb","cm","d","db","dm","dbm",
    "e","eb","ebm","em","f","fb","fm","g","gb","gbm","gm",
    "abm","c#","c#m","d#","d#m","f#","f#m","g#","g#m","a#","a#m",
    "smg",  # scene-release group, not an artist
})

def _is_garbage_tag_value(val:str)->bool:
    """Return True if a tag value looks like a URL/domain or musical key — not a real artist/album."""
    if not val: return False
    v=val.strip()
    if _GARBAGE_URL_RE.search(v): return True
    if v.lower() in _MUSICAL_KEY_GARBAGE and len(v)<=4: return True
    return False

def strip_trailing_domains(s:str)->str:
    s=re.sub(rf"(\s*[\(\[]\s*(https?:\/\/)?(www\.)?[\w\-]+(\.[\w\-]+)+\.({_TLDS})\s*[\)\]]\s*)$","",s.strip(),flags=re.IGNORECASE)
    s=re.sub(rf"(\s*[-–—]\s*)?(https?:\/\/)?(www\.)?[\w\-]+(\.[\w\-]+)+\.({_TLDS})\s*$","",s,flags=re.IGNORECASE)
    return re.sub(r"(\s*[-–—]\s*)?www\s*$","",s,flags=re.IGNORECASE).strip()

def cleanup_title(title:str,cfg:Dict[str,Any])->str:
    tc=cfg.get("title_cleanup",{})
    if not tc.get("enabled",True): return title.strip()
    o=title.strip().lstrip("\ufeff\u200b\u200c\u200d\u00a0")
    n=tc.get("normalize",{})
    if n.get("replace_underscores",True): o=o.replace("_"," ")
    if tc.get("strip_trailing_domains",True): o=strip_trailing_domains(o)
    if tc.get("strip_trailing_handles",True): o=re.sub(r"(\s*[-–—]\s*)?@[\w\.\-]+\s*$","",o.strip())
    # Strip Soundcloud/archive track ID + optional channel name: "Title 959134033 - Channel" → "Title"
    # 9+ digit numbers are SC-style IDs; regular track numbers are ≤ 4 digits
    o=re.sub(r'[\s_]+\d{9,}(?:\s*[-–—]\s*\S[^-–—]*)?$','',o).strip()
    phrases=[ph.lower() for ph in (tc.get("strip_trailing_phrases",[]) or [])]
    keep=[k.lower() for k in (tc.get("keep_parenthetical_if_contains",[]) or [])]
    def prot(seg:str)->bool: return any(k in seg.lower() for k in keep)
    changed=True
    while changed:
        changed=False
        m=re.search(r"(\s*[\(\[]([^)\]]+)[\)\]]\s*)$",o)
        if m and not prot(m.group(2)) and any(ph in m.group(2).lower() for ph in phrases):
            o=o[:m.start()].strip(); changed=True; continue
        m2=re.search(r"(\s*[-–—]\s*([^-–—]+)\s*)$",o)
        if m2 and any(ph in m2.group(2).lower() for ph in phrases) and not prot(m2.group(2)):
            o=o[:m2.start()].strip(); changed=True; continue
    if n.get("collapse_whitespace",True): o=re.sub(r"\s+"," ",o).strip()
    if n.get("trim_dots_spaces",True): o=o.rstrip(". ").strip()
    # Strip any lone trailing dash left behind by phrase stripping
    o=re.sub(r"\s*[-–—]+\s*$","",o).strip()
    # Apply smart title case to all-lowercase titles (mirrors artist normalization behaviour)
    _alpha=[w for w in o.split() if any(c.isalpha() for c in w)]
    if _alpha and all(w.islower() for w in _alpha):
        o=_smart_title_case_v43(o,cfg)
    return o

def parse_artist_title_from_fn(stem: str, folder_name: str = "", cfg: Optional[Dict[str, Any]] = None) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse artist and title from a filename stem.
    v4.2: Handles Beatport inverted format (Title - Artist1, Artist2) and
    leading-space Beatport artefact.  Also handles disc-track compound prefix
    (1-01 Title) and vinyl side lettering (A1 - Title).
    """
    if cfg is None: cfg = {}
    beatport_aware = cfg.get("beatport_aware", True)
    keep_all = cfg.get("beatport_keep_all_artists", False)

    # Strip leading space first (always safe — Beatport artefact)
    raw_stem = stem
    stem = stem.lstrip()

    # Strip disc-track compound prefix from stem if present: "1-01 Title"
    m_disc = _TRACK_DISC_COMPOUND.match(stem)
    if m_disc:
        stem = stem[m_disc.end():]

    # Strip vinyl side prefix from stem: "A1 - Title" or "b2 Title"
    m_vinyl = _TRACK_VINYL_STEM.match(stem)
    if m_vinyl:
        stem = stem[m_vinyl.end():]

    # Beatport inversion
    if beatport_aware and _is_beatport_format(raw_stem, folder_name):
        artist, title, _ = _invert_beatport_filename(raw_stem, keep_all_artists=keep_all)
        if artist and title:
            return artist.strip(), title.strip()

    # Standard parse
    s = normalize_unicode(re.sub(r"\s+", " ", stem.strip()))
    if detect_bpm_dj_encoding(s): return None, None
    s = re.sub(r"_-_", " - ", s).replace("_", " ")
    s = re.sub(r"\s+", " ", s).strip()
    # Strip Soundcloud/archive track ID + optional channel name before parsing
    s = re.sub(r'[\s_]+\d{9,}(?:\s*[-–—]\s*\S[^-–—]*)?$', '', s).strip()
    s = re.sub(r"^[\[\(][^\]\)]+[\]\)]\s*", "", s)
    s2 = re.sub(r"^\(?\d{1,3}\)?\s*[-–—\.]\s*", "", s)
    s2 = re.sub(r"^\d{1,3}\s+", "", s2)
    # Prefer splitting on spaced dashes ( - ) to avoid splitting compound names like Heavy-K.
    # Fall back to any dash if spaced-dash split yields fewer than 2 parts.
    parts = [p.strip() for p in re.split(r"\s+[-–—]\s+", s2) if p.strip()]
    if len(parts) < 2:
        parts = [p.strip() for p in re.split(r"\s*[-–—]\s*", s2) if p.strip()]
    # Strip trailing label/distributor noise codes: short all-lowercase alphabetic tokens
    # like "ftd", "cdm", "web" appended to scene/batch releases.
    # Only fires when there are 3+ parts (artist + title + code) so the title is preserved.
    # Guard: known musical terms (mix, vip, dub, rmx, etc.) are never stripped.
    _MUSIC_TERMS = {"mix","rmx","vip","dub","edit","live","demo","ost","dj","va","lp"}
    if len(parts) >= 3:
        _last = parts[-1]
        if re.match(r'^[a-z]{2,5}$', _last) and _last not in _MUSIC_TERMS:
            parts = parts[:-1]
    if len(parts) >= 2:
        return parts[0], " - ".join(parts[1:])
    # Single-part stem (no artist–title separator): return as title only.
    # This lets the feat. supplement step in build_track_filename pick up
    # collaborator info from the filename even when the tag title lacks it.
    if parts:
        return None, parts[0]
    return None, None

def extract_mix_suffix(title:str,cfg:Dict[str,Any])->Tuple[str,str]:
    mc=cfg.get("mix_info",{}); kw=[k.lower() for k in (mc.get("detect_keywords",[]) or [])]
    if not mc.get("enabled",True) or not kw: return title,""
    m=re.search(r"\s*(\(([^)]+)\))\s*$",title)
    if m and any(k in m.group(2).lower() for k in kw): return title[:m.start()].strip(),f" {m.group(1)}"
    m2=re.search(r"\s*[-–—]\s*([^-–—]+)\s*$",title)
    if m2 and any(k in m2.group(1).lower() for k in kw):
        candidate=m2.group(1).strip()
        # Guard: skip if candidate looks like a list of artists (has comma) or is very long —
        # prevents "- Artist1, Artist2 (Original Mix)" being swallowed as a mix suffix
        if "," not in candidate and len(candidate)<=60:
            clean=title[:m2.start()].strip(); sty=mc.get("style","parenthetical")
            return (clean,f" - {candidate}") if sty=="dash" else (clean,f" ({candidate})")
    return title,""

def classify_folder_for_tracks(decision:Dict[str,Any],cfg:Dict[str,Any])->str:
    # Check VA first — VA compilations should be "various" even with low album dominance.
    # Otherwise low-dominance VA folders fall through to "mixed" and lose track numbers.
    if bool(decision.get("is_va",False)): return "various"
    if float(decision.get("dominant_album_share",0.0))<float(cfg.get("decision",{}).get("album_dominance_threshold",0.75)): return "mixed"
    # Non-VA but track artists are diverse (remix albums, edits compilations) — use "various"
    # so individual track artists are preserved in filenames.
    # Exception: if the dominant track artist starts with the albumartist, this is a feat. album
    # (e.g., "Artist feat. Guest" variants) — keep "album" so we don't repeat the main artist name.
    dom_art_share=float(decision.get("dominant_artist_share",1.0))
    if dom_art_share<0.50:
        dom_aa=(decision.get("dominant_albumartist") or "").strip().lower()
        dom_art=(decision.get("dominant_artist") or "").strip().lower()
        # Feat. album: most tracks are "Main Artist" or "Main Artist feat. X" — albumartist is the prefix
        if dom_aa and dom_art and dom_art.startswith(dom_aa):
            return "album"
        return "various"
    return "album"

def build_track_filename(classification:str,tags:Dict[str,Optional[str]],src:Path,cfg:Dict[str,Any],decision:Dict[str,Any],disc_multi:bool,total_tracks:int=0)->Tuple[Optional[str],float,str,Dict[str,Any]]:
    trc=cfg.get("track_rename",{}); pat=trc.get("patterns",{}); ext=src.suffix.lower()
    tag_title=(tags.get("title") or "").strip(); tag_artist=(tags.get("artist") or "").strip()
    track_raw=(tags.get("tracknumber") or "").strip(); disc_raw=(tags.get("discnumber") or "").strip()
    folder_name = src.parent.name
    meta:Dict[str,Any]={}

    # v6.1: Smart tag vs filename priority.
    # Parse filename only when needed — if tags are clean, trust them fully.
    _tag_title_clean = tag_title and len(tag_title) >= 2 and not detect_garbage_name(tag_title) and not detect_mojibake(tag_title)
    _tag_artist_clean = tag_artist and len(tag_artist) >= 2 and not detect_garbage_name(tag_artist) and not detect_mojibake(tag_artist)

    # Always parse filename — needed for feat. supplement even when tag title is clean
    fn_artist, fn_title = parse_artist_title_from_fn(src.stem, folder_name=folder_name, cfg=cfg)

    # Title: prefer clean tag, fall back to filename
    if _tag_title_clean:
        title = tag_title; meta["title_src"] = "tag"
        # Supplement with feat. info from filename if tag title lacks it.
        # e.g. tag="Too Shy To Dance", fn_title="Too Shy To Dance (feat. Astrid Van Peeterssen)"
        if not re.search(r'(?<![a-zA-Z])(?:feat\.?|ft\.?|featuring)(?![a-zA-Z])', title, re.I):
            # Try fn_title first, then fn_artist — feat. info may live in either place.
            # Filename: "NN Artist feat. Guest - Title" → feat. in fn_artist, not fn_title.
            _feat_search_strs=[fn_title,fn_artist] if fn_title else ([fn_artist] if fn_artist else [])
            _feat_pat_re=re.compile(r'(?:^|[,\s\(\[])(?:feat\.?|ft\.?|featuring)(?![a-zA-Z])\s+([^\)\],\-]+)',re.I)
            _collab=None
            for _fs in _feat_search_strs:
                if not _fs: continue
                _feat_m=_feat_pat_re.search(_fs)
                if _feat_m:
                    _collab=_feat_m.group(1).strip().rstrip(')], ').strip()
                    if _collab: break
            if _collab:
                title=f"{title} (feat. {_collab})"
                meta["title_src"]="tag+feat_from_fn"
    elif fn_title:
        title = fn_title; meta["title_src"] = "filename"
    else:
        title = tag_title  # last resort: use whatever tag had even if short
        meta["title_src"] = "tag_fallback" if title else "none"

    if title: title=cleanup_title(title,cfg)
    if not title: return None,0.0,"missing_title",{}
    title_c,mix_suf=extract_mix_suffix(title,cfg); title_c=cleanup_title(title_c,cfg)
    if not title_c: return None,0.0,"title_cleaned_empty",{}

    # Artist: prefer clean tag, fall back to filename
    if _tag_artist_clean:
        artist = tag_artist; meta["artist_src"] = "tag"
    elif fn_artist:
        artist = fn_artist; meta["artist_src"] = "filename"
    else:
        artist = tag_artist  # last resort
        meta["artist_src"] = "tag_fallback" if artist else "none"
    if artist and cfg.get("artists",{}).get("feature_handling",{}).get("normalize_tokens",True):
        # Use lookahead (?=\s|,|$) to avoid matching "feat" without the period and doubling it
        artist=re.sub(r"\b(featuring|feat\.?|ft\.?)(?=\s|,|$)","feat.",artist,flags=re.IGNORECASE)
        artist=re.sub(r"\s+"," ",artist).strip()
    # For various/compilation classification: detect tag confusion where the tagger has
    # put a subtitle/track-name into the artist field. If fn_artist (from a 3-part filename)
    # is available, the fn_title starts with or equals the tag_artist, and they differ,
    # the filename is more authoritative than the tag.
    if classification=="various" and fn_artist and fn_title and _tag_artist_clean:
        _ta_n = normalize_unicode(tag_artist.lower().strip())
        _fn_t_n = normalize_unicode(fn_title.lower().strip())
        _fn_t_first = _fn_t_n.split(" - ")[0].strip()
        if _fn_t_first == _ta_n and normalize_unicode(fn_artist.lower().strip()) != _ta_n:
            # Tag artist looks like part of the filename title — use filename artist
            artist = fn_artist
            meta["artist_src"] = "filename_confusion_override"
    # Try vinyl track notation first (A1, B2 etc.)
    vinyl=parse_vinyl_track(track_raw.split("/")[0].strip()) if track_raw else None
    if vinyl:
        track_n=vinyl[2]; meta["vinyl_side"]=vinyl[0]; meta["track_src"]="vinyl_notation"
    else:
        track_n=parse_int_prefix(track_raw) if track_raw else None
    if track_n is None:
        # Try to extract track number from filename (leading digits)
        om=re.match(r"^(\d{1,3})",src.stem.strip())
        if om:
            raw_num=int(om.group(1))
            # Detect disc-compound format: 101→disc1/track01, 213→disc2/track13
            if 100<=raw_num<=999 and raw_num%100>=1:
                track_n=raw_num%100; meta["track_src"]="filename_disc_compound"; meta["fn_disc_n"]=raw_num//100
            else:
                track_n=raw_num; meta["track_src"]="filename_order"
    else:
        # Sanity-check tag track number: if it's way too high for this folder, use filename instead.
        # Catches cases where tracknumber is the position on a bigger compilation (e.g. tag=32, folder has 15 tracks).
        if total_tracks>0 and track_n>total_tracks and meta.get("track_src") not in ("filename_order","filename_disc_compound","vinyl_notation"):
            om=re.match(r"^(\d{1,3})",src.stem.strip())
            if om:
                raw_num=int(om.group(1))
                fn_n2=raw_num%100 if 100<=raw_num<=999 else raw_num
                if 1<=fn_n2<=total_tracks:
                    if 100<=raw_num<=999 and raw_num%100>=1:
                        track_n=fn_n2; meta["fn_disc_n"]=raw_num//100; meta["track_src"]="filename_disc_compound"
                    else:
                        track_n=fn_n2; meta["track_src"]="filename_order_sanity"
    if track_n is None:
        if classification=="album" and trc.get("track_numbers",{}).get("required_for_album",True):
            return None,0.0,"missing_track_number",{}
        elif classification=="various":
            # No track number — fall back to mixed pattern (Artist - Title) rather than skip.
            # Handles DJ download folders (e.g. [MONADA] compilations) with no track numbers.
            if not artist: return None,0.0,"missing_artist",meta
            tmpl=pat.get("mixed","{artist} - {title}{mix_suffix}{ext}")
            return sanitize_name(tmpl.format(artist=artist,title=title_c,mix_suffix=mix_suf,ext=ext,disc_prefix="",track=0)),0.85,"ok_no_tracknum",meta

    # Guard: strip leading "NN - " from title when the number matches the filename's leading digits.
    # Catches: (a) filename-inferred track number baked into title, (b) old filename pasted into title tag.
    if title_c:
        _stem_m=re.match(r"^(\d{1,3})",src.stem.strip())
        if _stem_m:
            _fn_num=int(_stem_m.group(1))
            _fn_num2=_fn_num%100 if 100<=_fn_num<=999 else _fn_num
            _num_m=re.match(r'^(\d{1,3})\s*[-–—]\s*(.+)$',title_c)
            if _num_m and int(_num_m.group(1))==_fn_num2:
                title_c=_num_m.group(2).strip() or title_c

    # Guard: strip leading "ArtistName - " from title if it matches the albumartist (album)
    # or the per-track artist (various). Prevents duplication in output filenames when the
    # filename-parsed title already contains the artist name that the template will prepend.
    import unicodedata as _ud
    _fold=lambda s:_ud.normalize('NFKD',normalize_unicode(s)).encode("ascii","ignore").decode("ascii").lower()
    if classification=="album" and decision and title_c:
        _aa=(decision.get("albumartist_display") or "").strip()
        if _aa:
            _aa_f=_fold(_aa)
            if len(_aa_f.split())==len(_aa.split()) and _aa_f:
                _sep_m=re.match(re.escape(_aa_f)+r'\s*[-–—]\s*',_fold(title_c))
                if _sep_m and _sep_m.end()<len(_fold(title_c)):
                    title_c=title_c[_sep_m.end():].strip() or title_c
    if classification=="various" and artist and title_c:
        _art_f=_fold(artist)
        if len(_art_f.split())==len(artist.split()) and _art_f:
            _sep_m=re.match(re.escape(_art_f)+r'\s*[-–—]\s*',_fold(title_c))
            if _sep_m and _sep_m.end()<len(_fold(title_c)):
                title_c=title_c[_sep_m.end():].strip() or title_c

    # Strip trailing [LabelName] bracket from track titles.
    # Square brackets in DJ download filenames are almost always label/distributor codes —
    # not part of the song title. Strip any trailing [TEXT] unless TEXT contains a music term
    # (mix, edit, version, feat, etc.) that should be preserved.
    if title_c:
        _tc_bracket_m=re.search(r'\s*\[([^\]]+)\]\s*$',title_c)
        if _tc_bracket_m:
            _bracket_content=_tc_bracket_m.group(1).strip()
            _keep_terms=set(cfg.get("title_cleanup",{}).get("keep_parenthetical_if_contains",[]))
            _is_music=any(k in _bracket_content.lower() for k in _keep_terms) if _keep_terms else False
            if not _is_music and _bracket_content:
                title_c=title_c[:_tc_bracket_m.start()].strip() or title_c

    disc_prefix=""
    if trc.get("disc",{}).get("enabled",True):
        disc_n=parse_int_prefix(disc_raw) if disc_raw else None
        # Also use disc number derived from filename compound (101→disc1, 201→disc2)
        if disc_n is None: disc_n=meta.get("fn_disc_n")
        # Disc-compound filenames are inherently multi-disc even without a discnumber tag
        use_disc=disc_multi or bool(meta.get("fn_disc_n"))
        if use_disc and disc_n: disc_prefix=trc.get("disc",{}).get("format","{disc}-").format(disc=disc_n)
    if classification=="album":
        if track_n is None: return None,0.0,"missing_track_number",{}
        tmpl=pat.get("album","{disc_prefix}{track:02d} - {title}{mix_suffix}{ext}")
        return sanitize_name(tmpl.format(disc_prefix=disc_prefix,track=int(track_n),title=title_c,mix_suffix=mix_suf,ext=ext)),0.95,"ok",meta
    if classification=="various":
        if not artist: return None,0.0,"missing_artist",meta
        if track_n is None: return None,0.0,"missing_track_number",meta
        tmpl=pat.get("various","{disc_prefix}{track:02d} - {artist} - {title}{mix_suffix}{ext}")
        fname=tmpl.format(disc_prefix=disc_prefix,track=int(track_n),artist=artist,title=title_c,mix_suffix=mix_suf,ext=ext)
        return sanitize_name(fname),0.92,"ok",meta
    if not artist: return None,0.0,"missing_artist",meta
    # Mixed classification with a known track number: preserve track numbering in output.
    if track_n is not None:
        tmpl=pat.get("various","{disc_prefix}{track:02d} - {artist} - {title}{mix_suffix}{ext}")
        fname=tmpl.format(disc_prefix=disc_prefix,track=int(track_n),artist=artist,title=title_c,mix_suffix=mix_suf,ext=ext)
        return sanitize_name(fname),0.90,"ok",meta
    tmpl=pat.get("mixed","{artist} - {title}{mix_suffix}{ext}")
    return sanitize_name(tmpl.format(artist=artist,title=title_c,mix_suffix=mix_suf,ext=ext,disc_prefix="",track=0)),0.90,"ok",meta

def folder_is_multidisc(files:List[Path],cfg:Dict[str,Any])->bool:
    discs:set=set()
    for f in files:
        dn=(read_audio_tags(f,cfg).get("discnumber") or ""); d=parse_int_prefix(dn) if dn else None
        if d:
            discs.add(d)
        else:
            # Detect disc-compound filename prefix: 101→disc1, 213→disc2 track13
            m=re.match(r"^(\d{3})",f.stem.strip())
            if m:
                raw=int(m.group(1))
                if 100<=raw<=999 and raw%100>=1:
                    discs.add(raw//100)
    return len(discs)>1

def rename_tracks_in_clean_folder(cfg:Dict[str,Any],folder:Path,folder_decision:Dict[str,Any],interactive:bool,dry_run:bool,session_id:str)->List[Dict[str,Any]]:
    trc=cfg.get("track_rename",{})
    if not trc.get("enabled",True): return []
    allowed={e.lower() for e in trc.get("allowed_extensions",[".mp3",".flac",".m4a"])}
    skip_exts={e.lower() for e in trc.get("skip_extensions",[])}
    files=[p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in allowed and p.suffix.lower() not in skip_exts and not is_hidden_file(p)]
    if not files: return []
    classification=classify_folder_for_tracks(folder_decision,cfg); disc_multi=folder_is_multidisc(files,cfg)
    thist=Path(cfg["logging"]["track_history_log"]); tskip=Path(cfg["logging"]["track_skipped_log"])
    applied:List[Dict[str,Any]]=[]; skip_count=0
    for f in sorted(files):
        # Extension case normalisation: .MP3 → .mp3
        ext_fixed=normalise_extension(f)
        if ext_fixed and not dry_run:
            try: f.rename(ext_fixed); f=ext_fixed
            except Exception: pass
        tags=read_audio_tags(f,cfg); new_name,conf,reason,meta=build_track_filename(classification,tags,f,cfg,folder_decision,disc_multi,total_tracks=len(files))
        if not new_name:
            out(f"    ↷ SKIP {f.name}  ({reason})",level=VERBOSE); skip_count+=1
            append_jsonl(tskip,{"timestamp":now_iso(),"session_id":session_id,"type":"track","reason":reason,"file":str(f),"meta":meta}); continue
        dst=f.with_name(new_name)
        if normalize_unicode(dst.name)==normalize_unicode(f.name):
            out(f"    ✓ {f.name}  {C.DIM}(already clean){C.RESET}",level=VERBOSE)
            continue
        if dst.exists():
            n=1
            while True:
                cand=dst.with_name(dst.stem+f" ({n})"+dst.suffix)
                if not cand.exists(): dst=cand; break
                n+=1
        if dry_run:
            out(f"    {C.DIM}[dry-run]{C.RESET} {f.name}\n          → {dst.name}  [{classification}]  conf={conf_color(conf)}")
            append_jsonl(tskip,{"timestamp":now_iso(),"session_id":session_id,"type":"track","reason":"dry_run","file":str(f),"dst":str(dst)}); continue
        if interactive:
            print(f"\n    {f.name}\n    → {dst.name}  conf={conf_color(conf)}")
            if input("    Rename? [y/N] ").strip().lower()!="y":
                append_jsonl(tskip,{"timestamp":now_iso(),"session_id":session_id,"type":"track","reason":"user_skipped","file":str(f)}); continue
        f.rename(dst)
        action_id=uuid.uuid4().hex[:10]
        entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,"type":"track","folder":str(folder),
               "original_path":str(f),"target_path":str(dst),"confidence":conf,"classification":classification,"meta":meta}
        append_jsonl(thist,entry); applied.append(entry)
        out(f"    RENAMED: {f.name}  →  {dst.name}",level=VERBOSE)
    if skip_count: out(f"    ↷ {skip_count} track(s) skipped in {folder.name}")
    elif applied:  out(f"    ✓ {len(applied)} track(s) renamed in {folder.name}")
    return applied

# ─────────────────────────────────────────────
# show — single folder debug
# ─────────────────────────────────────────────
def cmd_show(cfg:Dict[str,Any],folder_path:str,profile_name:str,show_tracks:bool=False)->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    folder=Path(folder_path).expanduser().resolve()
    if not folder.exists(): err(f"Folder not found: {folder}"); sys.exit(1)
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    audio_files=list_audio_files(folder,exts)
    out(f"\n{C.BOLD}{'═'*60}{C.RESET}")
    out(f"{C.BOLD}raagdosa show: {folder}{C.RESET}\n{'═'*60}")
    out(f"Audio files: {len(audio_files)}")
    if not audio_files: out("  None found."); return

    # Check .raagdosa override
    ov=load_folder_override(folder)
    if ov: out(f"\n{C.CYAN}Override file (.raagdosa):{C.RESET} {ov}")

    out(f"\n{C.BOLD}Tags (first 8 files):{C.RESET}")
    for f in sorted(audio_files)[:8]:
        tags=read_audio_tags(f,cfg); has=any(v for k,v in tags.items() if k not in ("bpm","key") and v)
        out(f"  {C.DIM}{f.name}{C.RESET}")
        if has: out(f"    {', '.join(f'{k}={v!r}' for k,v in tags.items() if v)}")
        else: out(f"    {C.YELLOW}(no tags){C.RESET}")
    if len(audio_files)>8: out(f"  ... and {len(audio_files)-8} more")
    prop=build_folder_proposal(folder,audio_files,source_root,profile,cfg)
    if not prop:
        out(f"\n{C.RED}⛔ Could not build a proposal — insufficient metadata and heuristic failed.{C.RESET}")
        out("  Tips:\n  • Tag files with album/albumartist (use MusicBrainz Picard or beets)\n  • Rename folder to 'Artist - Album' for heuristic parsing")
        return
    d=prop.decision
    out(f"\n{C.BOLD}Proposal:{C.RESET}")
    out(f"  Proposed name:      {C.GREEN}{prop.proposed_folder_name}{C.RESET}")
    out(f"  Target path:        {prop.target_path}")
    out(f"  Confidence:         {conf_color(prop.confidence)}")
    out(f"  Folder type:        {d.get('folder_type','album')}{'  [EP]' if d.get('is_ep') else ''}{'  [MIX]' if d.get('is_mix') else ''}")
    out(f"  album tag:          '{d.get('dominant_album_display')}' ({d.get('dominant_album_share',0):.0%} dominance)")
    out(f"  albumartist tag:    '{d.get('dominant_albumartist_display')}' ({d.get('dominant_albumartist_share',0):.0%} dominance)")
    out(f"  year:               {d.get('year') or 'not included'}")
    out(f"  VA:                 {'yes' if d.get('is_va') else 'no'}")
    out(f"  FLAC only:          {'yes' if d.get('is_flac_only') else 'no'}")
    out(f"  heuristic:          {'yes (no tags found)' if d.get('used_heuristic') else 'no'}")

    # Confidence factor breakdown
    cf=d.get("confidence_factors",{})
    if cf:
        out(f"\n{C.BOLD}Confidence factors:{C.RESET}")
        factor_labels={"dominance":"Vote dominance","tag_coverage":"Tag coverage",
                       "title_quality":"Title quality","filename_consistency":"Filename/tag consistency",
                       "completeness":"Track completeness","aa_consistency":"Albumartist consistency",
                       "folder_alignment":"Folder name alignment"}
        for key,label in factor_labels.items():
            v=cf.get(key,0.0)
            bar="█"*int(v*20)+"░"*(20-int(v*20))
            col=C.GREEN if v>=0.85 else (C.YELLOW if v>=0.65 else C.RED)
            out(f"  {label:<30} {col}[{bar}] {v:.2f}{C.RESET}")
        gaps=int(cf.get("track_gaps",0)); dupes=int(cf.get("track_dupes",0))
        if gaps:  warn(f"Track number gaps: {gaps} missing")
        if dupes: warn(f"Duplicate track numbers: {dupes}")

    if d.get("garbage_reasons"): warn(f"Garbage flags on album name: {', '.join(d['garbage_reasons'])}")
    if prop.stats.format_duplicates: warn(f"Format duplicates: {', '.join(prop.stats.format_duplicates)}")
    rr_cfg=cfg.get("review_rules",{}); min_conf=float(rr_cfg.get("min_confidence_for_clean",0.85))
    in_mf=manifest_has(cfg,prop.proposed_folder_name)
    reasons:List[str]=[]; route="clean"
    if prop.confidence<min_conf: route="review"; reasons.append(f"low_confidence ({prop.confidence:.2f}<{min_conf})")
    if in_mf: route="duplicate"; reasons.append("already_in_manifest")
    out(f"\n{C.BOLD}Routing:{C.RESET}  {status_tag(route)}  {', '.join(reasons) or 'all good'}")
    if route=="review":
        out(f"\n{C.YELLOW}Tips to reach Clean:{C.RESET}")
        out(f"  • Ensure consistent album/albumartist tags across all tracks")
        out(f"  • Or lower review_rules.min_confidence_for_clean below {prop.confidence:.2f} in config")

    # --tracks: show per-track proposed renames
    if show_tracks:
        out(f"\n{C.BOLD}Track rename preview:{C.RESET}")
        allowed={e.lower() for e in cfg.get("track_rename",{}).get("allowed_extensions",[".mp3",".flac",".m4a"])}
        tr_files=[p for p in sorted(audio_files) if p.suffix.lower() in allowed]
        if not tr_files: out("  No renamable tracks found.")
        else:
            cls=classify_folder_for_tracks(prop.decision,cfg)
            disc_multi=folder_is_multidisc(tr_files,cfg)
            for f in tr_files:
                tags=read_audio_tags(f,cfg)
                new_name,conf,reason,meta=build_track_filename(cls,tags,f,cfg,prop.decision,disc_multi,total_tracks=len(tr_files))
                if new_name:
                    changed=normalize_unicode(new_name)!=normalize_unicode(f.name)
                    sym=f"{C.GREEN}→{C.RESET}" if changed else f"{C.DIM}={C.RESET}"
                    out(f"  {C.DIM}{f.name:<50}{C.RESET} {sym} {new_name}  {conf_color(conf)}")
                else:
                    out(f"  {C.DIM}{f.name:<50}{C.RESET} {C.YELLOW}SKIP ({reason}){C.RESET}")

# ─────────────────────────────────────────────
# verify — library audit
# ─────────────────────────────────────────────
def cmd_verify(cfg:Dict[str,Any],profile_name:str)->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    setup_logging_paths(cfg, profile, source_root)
    roots=ensure_roots(profile,source_root); clean_albums=roots["clean_albums"]
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    manifest=read_manifest(cfg); mf_entries=manifest.get("entries",{})
    issues:List[str]=[]; ok_count=0
    out(f"\n{C.BOLD}{'═'*60}{C.RESET}")
    out(f"{C.BOLD}raagdosa verify — {profile_name}{C.RESET}\n{'═'*60}")
    out(f"\nChecking {len(mf_entries)} manifest entries...")
    for name in mf_entries:
        candidates=list(clean_albums.rglob(name)) if clean_albums.exists() else []
        if not candidates: issues.append(f"MANIFEST_MISSING_ON_DISK: '{name}'")
        else: ok_count+=1
    out("Checking disk vs manifest...")
    if clean_albums.exists():
        for d in clean_albums.rglob("*"):
            if not d.is_dir(): continue
            if any(f.suffix.lower() in exts for f in d.iterdir() if f.is_file()):
                if normalize_unicode(d.name) not in mf_entries: issues.append(f"NOT_IN_MANIFEST: {d}")
    out("Checking for empty folders...")
    if clean_albums.exists():
        for d in clean_albums.rglob("*"):
            if d.is_dir() and not any(d.iterdir()): issues.append(f"EMPTY_FOLDER: {d}")
    allowed={e.lower() for e in cfg.get("track_rename",{}).get("allowed_extensions",[".mp3",".flac",".m4a"])}
    expected=re.compile(r"^\d{2,3}\s*[-–]\s*.+\.\w+$"); unclean=0
    if clean_albums.exists():
        for f in clean_albums.rglob("*"):
            if f.is_file() and f.suffix.lower() in allowed and not is_hidden_file(f) and not expected.match(f.name): unclean+=1
    out(f"\n{C.BOLD}Results:{C.RESET}")
    out(f"  Manifest OK:            {ok_count}")
    out(f"  Unclean track names:    {unclean}  {C.DIM}(run 'raagdosa tracks' to fix){C.RESET}")
    if issues:
        out(f"\n{C.YELLOW}Issues ({len(issues)}):{C.RESET}")
        for i in issues[:50]: out(f"  {C.YELLOW}⚠{C.RESET}  {i}")
        if len(issues)>50: out(f"  ... and {len(issues)-50} more")
    else: ok_msg("No issues found — library looks healthy.")
    log_root=Path(cfg.get("logging",{}).get("root_dir","logs")); ensure_dir(log_root)
    vpath=log_root/f"verify_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    vpath.write_text("\n".join([f"RaagDosa v{APP_VERSION} — Verify",f"Profile: {profile_name}",f"Date: {now_iso()}",f"Issues: {len(issues)}",""]+issues if issues else [f"RaagDosa v{APP_VERSION} — Verify",f"Profile: {profile_name}",f"Date: {now_iso()}","No issues."]),encoding="utf-8")
    out(f"\n{C.DIM}Report: {vpath}{C.RESET}")

# ─────────────────────────────────────────────
# learn — config suggestion from Review patterns
# ─────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────
# v7.0 — Reference (Musical Reference) commands
# ─────────────────────────────────────────────────────────────────

def cmd_reference(cfg_path:Path,cfg:Dict[str,Any],action:str,
                  import_path:Optional[str]=None,section:Optional[str]=None,
                  export_path:Optional[str]=None)->None:
    """Handle `raagdosa reference list|import|export` commands."""
    ref=cfg.get("reference",{})

    if action=="list":
        aliases=ref.get("artist_aliases",{}) or {}
        labels=ref.get("known_labels",[]) or []
        va_prefixes=ref.get("va_rescue_prefixes",[]) or []
        noise=ref.get("noise_patterns",[]) or []
        out(f"\n{C.BOLD}Musical Reference{C.RESET}")
        out(f"{'─'*50}")
        out(f"  Artist aliases:     {len(aliases)}")
        out(f"  Known labels:       {len(labels)}")
        out(f"  VA rescue prefixes: {len(va_prefixes)}")
        out(f"  Noise patterns:     {len(noise)}")
        if aliases:
            out(f"\n  {C.DIM}Artist aliases (first 20):{C.RESET}")
            for i,(k,v) in enumerate(sorted(aliases.items(),key=lambda x:x[1].lower())):
                if i>=20:
                    out(f"  {C.DIM}  ... and {len(aliases)-20} more{C.RESET}"); break
                out(f"    {k:30s} → {v}")
        if labels:
            out(f"\n  {C.DIM}Known labels:{C.RESET}")
            for lb in labels[:15]:
                out(f"    {lb}")
            if len(labels)>15: out(f"    {C.DIM}... and {len(labels)-15} more{C.RESET}")

    elif action=="export":
        # Export reference section to a shareable YAML file
        export_data={
            "raagdosa_reference_version":1,
            "exported_at":now_iso(),
        }
        if section:
            if section in ref:
                export_data[section]=ref[section]
            else:
                err(f"Unknown section: {section}. Available: {', '.join(ref.keys())}"); return
        else:
            export_data.update(ref)
        out_path=Path(export_path) if export_path else Path("reference_export.yaml")
        write_yaml(out_path,export_data)
        aliases_n=len(export_data.get("artist_aliases",{}))
        labels_n=len(export_data.get("known_labels",[]))
        out(f"  {C.GREEN}Exported:{C.RESET} {out_path}")
        out(f"  {aliases_n} aliases, {labels_n} labels")
        out(f"  Share this file with the community!")

    elif action=="import":
        if not import_path:
            err("Usage: raagdosa reference import <file.yaml>"); return
        imp_path=Path(import_path)
        if not imp_path.exists():
            err(f"File not found: {imp_path}"); return
        imp_data=read_yaml(imp_path)
        # Strip metadata keys
        for meta_key in ("raagdosa_reference_version","exported_at"):
            imp_data.pop(meta_key,None)
        # Merge each section
        added=0; conflicts=0; source=f"imported:{imp_path.name}:{now_iso()[:10]}"
        # Artist aliases
        if "artist_aliases" in imp_data:
            existing=ref.get("artist_aliases",{}) or {}
            incoming=imp_data["artist_aliases"] or {}
            for k,v in incoming.items():
                if k.lower() in {ek.lower() for ek in existing}:
                    # Check if canonical differs
                    existing_canonical=next((ev for ek,ev in existing.items() if ek.lower()==k.lower()),None)
                    if existing_canonical and existing_canonical!=v:
                        warn(f"Conflict: '{k}' → yours: '{existing_canonical}' vs import: '{v}'")
                        choice=input(f"    Keep yours (y) or use import (n)? [y/N]: ").strip().lower()
                        if choice!="n":
                            conflicts+=1; continue
                    else:
                        continue  # identical, skip
                existing[k]=v; added+=1
            ref["artist_aliases"]=existing
        # Known labels
        if "known_labels" in imp_data:
            existing_labels=set(ref.get("known_labels",[]) or [])
            for lb in (imp_data["known_labels"] or []):
                if lb not in existing_labels:
                    existing_labels.add(lb); added+=1
            ref["known_labels"]=sorted(existing_labels)
        # VA rescue prefixes
        if "va_rescue_prefixes" in imp_data:
            existing_va=set(ref.get("va_rescue_prefixes",[]) or [])
            for p in (imp_data["va_rescue_prefixes"] or []):
                if p not in existing_va:
                    existing_va.add(p); added+=1
            ref["va_rescue_prefixes"]=sorted(existing_va)
        # Noise patterns
        if "noise_patterns" in imp_data:
            existing_noise=ref.get("noise_patterns",[]) or []
            existing_pats={n.get("pattern","") if isinstance(n,dict) else n for n in existing_noise}
            for n in (imp_data["noise_patterns"] or []):
                pat=n.get("pattern","") if isinstance(n,dict) else n
                if pat and pat not in existing_pats:
                    existing_noise.append(n); added+=1
            ref["noise_patterns"]=existing_noise

        # Write back to config
        cfg["reference"]=ref
        write_yaml(cfg_path,cfg)
        out(f"\n  {C.GREEN}Imported:{C.RESET} {added} new entries from {imp_path.name}")
        if conflicts: out(f"  {C.YELLOW}Conflicts:{C.RESET} {conflicts} kept your version")
        out(f"  Source: {source}")
    else:
        err(f"Unknown reference action: {action}. Use: list, import, export")

def cmd_learn(cfg_path:Path,cfg:Dict[str,Any],session_id:Optional[str])->None:
    _resolve_log_paths_from_active_profile(cfg)
    sdir=Path(cfg["logging"]["session_dir"]); sessions:List[Path]=[]
    if session_id:
        sp=sdir/session_id/"proposals.json"
        if sp.exists(): sessions=[sp]
    else:
        if sdir.exists():
            all_s=sorted([p for p in sdir.iterdir() if p.is_dir()],key=lambda p:p.name,reverse=True)
            sessions=[s/"proposals.json" for s in all_s[:5] if (s/"proposals.json").exists()]
    if not sessions: out("No sessions found."); return
    review_albums:List[str]=[]; review_reasons:Counter=Counter()
    low_conf:List[str]=[]; heuristic:List[str]=[]; unclean_suffixes:Counter=Counter()
    for sp in sessions:
        try: payload=read_json(sp)
        except Exception: continue
        for fp in payload.get("folder_proposals",[]):
            if fp.get("destination")!="review": continue
            name=fp.get("folder_name",""); reasons=fp.get("decision",{}).get("route_reasons",[])
            review_albums.append(name)
            for r in reasons: review_reasons[r]+=1
            if "low_confidence" in reasons: low_conf.append(name)
            if "heuristic_fallback" in reasons: heuristic.append(name)
            for b in re.findall(r"[\(\[]([\w\s]+)[\)\]]",name):
                bl=b.strip().lower()
                if bl not in {k.lower() for k in cfg.get("normalize",{}).get("strip_common_suffixes_for_voting",[]) or []}:
                    unclean_suffixes[bl]+=1
    out(f"\n{C.BOLD}{'═'*60}{C.RESET}\n{C.BOLD}raagdosa learn{C.RESET}\n{'═'*60}")
    out(f"Sessions analysed: {len(sessions)} | Review folders: {len(review_albums)}")
    if not review_albums: ok_msg("No Review folders — config looks well-tuned."); return
    out(f"\n{C.BOLD}Review reasons:{C.RESET}")
    for reason,count in review_reasons.most_common(): out(f"  {count:3d}×  {reason}")
    suggestions:List[Tuple[str,str,Any]]=[]
    if len(low_conf)>5:
        cur=float(cfg.get("review_rules",{}).get("min_confidence_for_clean",0.85))
        prop_conf=max(0.70,cur-0.05)
        suggestions.append(("review_rules.min_confidence_for_clean",f"Lower from {cur} → {prop_conf} ({len(low_conf)} folders hit this threshold)",prop_conf))
    new_suf=[(s,c) for s,c in unclean_suffixes.most_common(10) if c>=2]
    if new_suf:
        suggestions.append(("normalize.strip_common_suffixes_for_voting",f"Add {len(new_suf)} suffix(es) seen in Review names: "+", ".join(f"'{s}'({c}×)" for s,c in new_suf[:5]),[s for s,_ in new_suf]))
    if len(heuristic)>3:
        suggestions.append(("NOTE",f"{len(heuristic)} zero-tag folders — consider running MusicBrainz Picard on them first.",None))
    if not suggestions: ok_msg("No actionable suggestions."); return
    out(f"\n{C.BOLD}Suggestions:{C.RESET}")
    for i,(key,desc,_) in enumerate(suggestions,1): out(f"\n  [{i}] {C.CYAN}{key}{C.RESET}\n      {desc}")
    out("\nApply which? (e.g. 1,2  or  all  or  none): ",level=NORMAL)
    ans=input("  → ").strip().lower()
    if ans in ("","none","n"): out("No changes."); return
    chosen=set(range(1,len(suggestions)+1)) if ans=="all" else {int(x.strip()) for x in ans.split(",") if x.strip().isdigit()}
    applied_changes:List[str]=[]
    for i,(key,_,val) in enumerate(suggestions,1):
        if i not in chosen or val is None: continue
        if key=="review_rules.min_confidence_for_clean":
            cfg.setdefault("review_rules",{})["min_confidence_for_clean"]=val; applied_changes.append(f"{key} → {val}")
        elif key=="normalize.strip_common_suffixes_for_voting":
            ex=cfg.setdefault("normalize",{}).setdefault("strip_common_suffixes_for_voting",[])
            for s in val:
                if s not in ex: ex.append(s)
            applied_changes.append(f"Added {len(val)} suffix(es) to {key}")
    if applied_changes:
        write_yaml(cfg_path,cfg); out(f"\n{C.GREEN}Config updated:{C.RESET}")
        for c in applied_changes: out(f"  ✓ {c}")
        out(f"\n{C.DIM}Saved: {cfg_path}{C.RESET}")
    else: out("No changes applied.")


# ─────────────────────────────────────────────
# dump-tree — export raw folder/file tree for training/debug
# ─────────────────────────────────────────────
def cmd_dump_tree(
    cfg: Dict[str, Any],
    profile_name: str,
    out_path: str,
    include_clean: bool = False,
    include_review: bool = False,
    include_logs: bool = False,
    folders_only: bool = False,
    files_only: bool = False,
) -> None:
    profiles = cfg.get("profiles", {})
    if profile_name not in profiles:
        raise ValueError(f"Unknown profile: {profile_name}")

    profile = profiles[profile_name]
    source_root = Path(profile["source_root"]).expanduser().resolve()
    roots = ensure_roots(profile, source_root)

    if not source_root.exists():
        raise FileNotFoundError(f"source_root missing: {source_root}")

    clean_root = roots["clean_root"].resolve()
    review_root = roots["review_root"].resolve()
    logs_cfg = Path(cfg.get("logging", {}).get("root_dir", "logs"))
    logs_root = (source_root / logs_cfg).resolve() if not logs_cfg.is_absolute() else logs_cfg.resolve()

    out_file = Path(out_path).expanduser().resolve()
    ensure_dir(out_file.parent)

    def _is_under(path: Path, parent: Path) -> bool:
        try:
            path.resolve().relative_to(parent.resolve())
            return True
        except Exception:
            return False

    lines: List[str] = []

    for root, dirs, files in os.walk(source_root):
        root_path = Path(root).resolve()

        # Prune excluded top-level generated areas
        pruned_dirs = []
        for d in dirs:
            child = (root_path / d).resolve()

            if not include_clean and _is_under(child, clean_root):
                continue
            if not include_review and _is_under(child, review_root):
                continue
            if not include_logs and _is_under(child, logs_root):
                continue

            pruned_dirs.append(d)

        dirs[:] = sorted(pruned_dirs)
        files = sorted(files)

        # Skip hidden macOS noise files in output
        rel_root = root_path.relative_to(source_root)
        depth = 0 if str(rel_root) == "." else len(rel_root.parts)

        if not files_only:
            rel_str = "." if str(rel_root) == "." else rel_root.as_posix()
            lines.append(f"DIR|depth={depth}|path={rel_str}")

        if folders_only:
            continue

        for fname in files:
            fpath = root_path / fname

            if is_hidden_file(fpath):
                continue

            if not include_clean and _is_under(fpath, clean_root):
                continue
            if not include_review and _is_under(fpath, review_root):
                continue
            if not include_logs and _is_under(fpath, logs_root):
                continue

            rel_file = fpath.relative_to(source_root).as_posix()
            file_depth = len(Path(rel_file).parts) - 1
            ext = fpath.suffix.lower() or ""
            lines.append(f"FILE|depth={file_depth}|ext={ext}|path={rel_file}")

    out_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    out(f"\n{C.BOLD}raagdosa dump-tree — {profile_name}{C.RESET}")
    out(f"Source: {source_root}")
    out(f"Wrote:  {out_file}")
    out(f"Lines:  {len(lines)}")



# ─────────────────────────────────────────────
# orphans — find loose audio files
# ─────────────────────────────────────────────
def cmd_orphans(cfg:Dict[str,Any],profile_name:str)->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    setup_logging_paths(cfg, profile, source_root)
    roots=ensure_roots(profile,source_root)
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    orphans:List[Path]=[]
    for root_key in ("clean_albums","review_albums"):
        base=roots[root_key]
        if not base.exists(): continue
        # Files directly in the Albums/ root (no subfolder)
        for p in base.iterdir():
            if p.is_file() and p.suffix.lower() in exts and not is_hidden_file(p):
                orphans.append(p)
        # Files in artist-level folder (one level deep) but no album subfolder
        for artist_dir in base.iterdir():
            if not artist_dir.is_dir(): continue
            for p in artist_dir.iterdir():
                if p.is_file() and p.suffix.lower() in exts and not is_hidden_file(p):
                    orphans.append(p)
    out(f"\n{C.BOLD}{'═'*60}{C.RESET}\n{C.BOLD}raagdosa orphans — {profile_name}{C.RESET}\n{'═'*60}")
    if not orphans:
        ok_msg("No orphan audio files found — library looks tidy.")
        return
    out(f"\n{C.YELLOW}Found {len(orphans)} orphan file(s):{C.RESET}")
    for p in sorted(orphans): out(f"  {C.DIM}{p}{C.RESET}")
    out(f"\n{C.DIM}Orphans can be moved to '{profile.get('orphans_folder_name','Orphans')}' manually or by adding them to an album folder.{C.RESET}")

# ─────────────────────────────────────────────
# artists — list/find artists in Clean
# ─────────────────────────────────────────────
def cmd_artists(cfg:Dict[str,Any],profile_name:str,list_mode:bool,find_query:Optional[str])->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    setup_logging_paths(cfg, profile, source_root)
    roots=ensure_roots(profile,source_root); clean_albums=roots["clean_albums"]
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]

    out(f"\n{C.BOLD}{'═'*60}{C.RESET}\n{C.BOLD}raagdosa artists — {profile_name}{C.RESET}\n{'═'*60}")

    if not clean_albums.exists(): out("Clean/Albums not found."); return
    lib=cfg.get("library",{}); template=lib.get("template","{artist}/{album}")

    # Collect artist directories (top-level subdirs of clean_albums, excluding _* special folders)
    if "{artist}" in template:
        # Artist folders are direct children
        artist_dirs=sorted([d for d in clean_albums.iterdir() if d.is_dir() and not d.name.startswith("_")])
    else:
        # Flat template — read albumartist from tags
        artist_dirs=[]

    if find_query:
        q=find_query.strip().lower()
        matches=[(d,string_similarity(d.name.lower(),q)) for d in artist_dirs]
        matches=sorted([(d,s) for d,s in matches if s>=0.3],key=lambda x:x[1],reverse=True)
        if not matches: out(f"No artists matching '{find_query}'."); return
        out(f"\nMatches for '{find_query}':")
        for d,score in matches[:20]:
            albums=len([x for x in d.iterdir() if x.is_dir()]) if d.is_dir() else 0
            out(f"  {C.GREEN}{d.name:<40}{C.RESET}  {score:.0%} match  {albums} album(s)  {C.DIM}{d}{C.RESET}")
    else:
        if not list_mode: out(f"\n{len(artist_dirs)} artist(s) in Clean:"); list_mode=True
        for d in artist_dirs:
            albums=len([x for x in d.iterdir() if x.is_dir()]) if d.is_dir() else 0
            tracks=sum(len(list_audio_files(x,exts)) for x in d.rglob("*") if x.is_dir())
            out(f"  {d.name:<45}  {albums:3d} album(s)  {tracks:4d} tracks")
        out(f"\n{C.DIM}Total: {len(artist_dirs)} artists{C.RESET}")

# ─────────────────────────────────────────────
# review-list — summarise Review folder
# ─────────────────────────────────────────────
def cmd_review_list(cfg:Dict[str,Any],profile_name:str,older_than_days:Optional[int])->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    setup_logging_paths(cfg, profile, source_root)
    roots=ensure_roots(profile,source_root); review_albums=roots["review_albums"]
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]

    out(f"\n{C.BOLD}{'═'*60}{C.RESET}\n{C.BOLD}raagdosa review-list — {profile_name}{C.RESET}\n{'═'*60}")
    if not review_albums.exists(): out("Review/Albums not found — empty."); return

    now=dt.datetime.now()
    cutoff=now-dt.timedelta(days=older_than_days) if older_than_days else None

    # Build a lookup from session proposals for confidence/reason/origin data
    conf_map:Dict[str,float]={}; reason_map:Dict[str,str]={}
    origin_map:Dict[str,str]={}; detail_map:Dict[str,Dict[str,Any]]={}
    sdir=Path(cfg["logging"]["session_dir"])
    if sdir.exists():
        for sd in sorted(sdir.iterdir(),key=lambda p:p.name,reverse=True)[:10]:
            pf=sd/"proposals.json"
            if not pf.exists(): continue
            try:
                pl=read_json(pf)
                for fp in pl.get("folder_proposals",[]):
                    nm=fp.get("proposed_folder_name","")
                    if nm and nm not in conf_map:
                        conf_map[nm]=float(fp.get("confidence",0.0))
                        dec=fp.get("decision",{})
                        reason_map[nm]=", ".join(dec.get("route_reasons",[]))
                        origin_map[nm]=fp.get("folder_name","")
                        detail_map[nm]={"artist":dec.get("albumartist_display",""),
                                        "is_va":dec.get("is_va",False),
                                        "folder_type":dec.get("folder_type",""),
                                        "heuristic":dec.get("used_heuristic",False)}
            except Exception: continue

    folders=sorted([d for d in review_albums.rglob("*") if d.is_dir()
                    and any(f.suffix.lower() in exts for f in d.iterdir() if f.is_file())],
                   key=lambda d:folder_mtime(d))

    if cutoff:
        folders=[d for d in folders if dt.datetime.fromtimestamp(folder_mtime(d))<=cutoff]

    if not folders: out(f"No Review folders{' older than '+str(older_than_days)+' days' if older_than_days else ''}."); return
    out("")
    for d in folders:
        mtime=dt.datetime.fromtimestamp(folder_mtime(d))
        age=(now-mtime).days
        conf=conf_map.get(d.name)
        reason=reason_map.get(d.name,"")
        origin=origin_map.get(d.name,"")
        detail=detail_map.get(d.name,{})
        conf_s=conf_color(conf) if conf else f"{C.DIM}n/a{C.RESET}"
        age_col=C.RED if age>60 else (C.YELLOW if age>14 else C.DIM)
        # v6.1: Show FROM → TO transformation so user can evaluate the proposal
        if origin and origin != d.name:
            out(f"  {C.DIM}{origin}{C.RESET}")
            out(f"  → {C.BOLD}{d.name}{C.RESET}  {conf_s}  {age_col}{age}d{C.RESET}")
        else:
            out(f"  {C.BOLD}{d.name}{C.RESET}  {conf_s}  {age_col}{age}d{C.RESET}")
        # Show key decision context
        flags=[]
        if detail.get("artist"): flags.append(f"artist={detail['artist']}")
        if detail.get("is_va"): flags.append("VA")
        if detail.get("folder_type") and detail["folder_type"] not in ("album",): flags.append(detail["folder_type"])
        if detail.get("heuristic"): flags.append("heuristic")
        if reason: flags.append(reason)
        if flags:
            out(f"    {C.DIM}{' | '.join(flags)}{C.RESET}")
        out("")
    out(f"{C.DIM}Total: {len(folders)} folder(s) in Review{C.RESET}")

# ─────────────────────────────────────────────
# review promote — force VA→album re-evaluation
# ─────────────────────────────────────────────
def cmd_review_promote(cfg:Dict[str,Any],profile_name:str,folder_query:str,dry_run:bool=False,artist_override:Optional[str]=None)->None:
    """
    Re-evaluate a Review folder as a single-artist album (force_album=True).
    If the new proposal passes confidence, move it to Clean/.
    The user can optionally provide --artist to force the artist name.
    """
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    setup_logging_paths(cfg, profile, source_root)
    roots=ensure_roots(profile,source_root)
    review_albums=roots["review_albums"]; clean_albums=roots["clean_albums"]
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]

    out(f"\n{C.BOLD}{'═'*60}{C.RESET}")
    out(f"{C.BOLD}raagdosa review promote{C.RESET}\n{'═'*60}")

    if not review_albums.exists():
        out("Review/Albums not found."); return

    # Find the folder — exact match first, then fuzzy
    folder_query_p=Path(folder_query).expanduser().resolve()
    target:Optional[Path]=None
    if folder_query_p.exists() and folder_query_p.is_dir():
        target=folder_query_p
    else:
        # Search in Review/Albums for matching name
        candidates=[d for d in review_albums.rglob("*") if d.is_dir()
                    and any(f.suffix.lower() in exts for f in d.iterdir() if f.is_file())]
        # Exact name match
        for d in candidates:
            if d.name == folder_query:
                target=d; break
        # Fuzzy substring match
        if not target:
            q_low=folder_query.lower()
            matches=[d for d in candidates if q_low in d.name.lower()]
            if len(matches)==1:
                target=matches[0]
            elif len(matches)>1:
                out(f"  Multiple matches for '{folder_query}':")
                for m in matches[:10]:
                    out(f"    {m.name}")
                out(f"  Be more specific."); return

    if not target:
        err(f"Could not find '{folder_query}' in Review."); return

    audio_files=list_audio_files(target,exts)
    if not audio_files:
        err(f"No audio files in {target.name}"); return

    # If user provides --artist, write a temporary .raagdosa override
    _tmp_override=False
    override_path=target/".raagdosa"
    if artist_override and not override_path.exists():
        try:
            override_path.write_text(f"albumartist: {artist_override}\n",encoding="utf-8")
            _tmp_override=True
        except Exception: pass

    # Build original proposal (as-is) for comparison
    prop_original=build_folder_proposal(target,audio_files,source_root,profile,cfg)

    # Build new proposal with force_album=True
    prop_new=build_folder_proposal(target,audio_files,source_root,profile,cfg,force_album=True)

    # Clean up temp override
    if _tmp_override:
        try: override_path.unlink()
        except Exception: pass

    if not prop_new:
        err(f"Could not build a proposal for {target.name}"); return

    # Show comparison
    out(f"\n{C.BOLD}Current (VA):{C.RESET}")
    if prop_original:
        out(f"  {prop_original.proposed_folder_name}  conf={conf_color(prop_original.confidence)}")
        out(f"  artist: {prop_original.decision.get('albumartist_display','?')}  VA={prop_original.decision.get('is_va')}")
    else:
        out(f"  {C.DIM}(no proposal){C.RESET}")

    out(f"\n{C.BOLD}Re-evaluated (album):{C.RESET}")
    out(f"  {C.GREEN}{prop_new.proposed_folder_name}{C.RESET}  conf={conf_color(prop_new.confidence)}")
    out(f"  artist: {prop_new.decision.get('albumartist_display','?')}  VA={prop_new.decision.get('is_va')}")

    # Route the new proposal
    rr=cfg.get("review_rules",{}); min_conf=float(rr.get("min_confidence_for_clean",0.85))
    new_dest="clean" if prop_new.confidence>=min_conf else "review"
    reasons=[]
    if prop_new.confidence<min_conf:
        reasons.append(f"low_confidence ({prop_new.confidence:.2f}<{min_conf})")
    if prop_new.decision.get("used_heuristic"):
        reasons.append("heuristic_fallback")
        if new_dest=="clean": new_dest="review"

    if new_dest=="clean":
        new_target=resolve_library_path(
            clean_albums,prop_new.decision.get("albumartist_display",""),
            prop_new.decision.get("dominant_album_display",""),
            prop_new.decision.get("year"),
            prop_new.decision.get("is_flac_only",False),False,False,
            prop_new.decision.get("is_mix",False),cfg,
            profile=profile,genre=prop_new.decision.get("genre"),
            bpm=prop_new.decision.get("bpm"),key=prop_new.decision.get("key"),
            label=prop_new.decision.get("label"))
        out(f"\n  {C.GREEN}→ Promotes to Clean:{C.RESET} {new_target}")

        if dry_run:
            out(f"  {C.DIM}[dry-run] No files moved.{C.RESET}")
        else:
            confirm=input(f"\n  Move to Clean? [y/N] ").strip().lower()
            if confirm=="y":
                ensure_dir(new_target.parent)
                dst=collision_resolve(new_target,"suffix"," ({n})")
                if dst is None:
                    err("Collision — cannot resolve target."); return
                try:
                    move_method,_=safe_move_folder(target,dst)
                    session_id=make_session_id(profile_name,str(source_root))
                    action_id=uuid.uuid4().hex[:10]
                    entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,
                           "type":"folder","subtype":"review_promote",
                           "original_path":str(target),"original_folder_name":target.name,
                           "target_path":str(dst),"target_folder_name":dst.name,
                           "destination":"clean","confidence":prop_new.confidence,
                           "decision":prop_new.decision,"move_method":move_method}
                    hist_path=Path(cfg["logging"]["history_log"])
                    append_jsonl(hist_path,entry)
                    update_manifest(cfg,dst.name,entry)
                    ok_msg(f"Promoted: {target.name} → {dst}")
                except Exception as e:
                    err(f"Move failed: {e}")
            else:
                out("  Skipped.")
    else:
        out(f"\n  {C.YELLOW}Still routes to Review:{C.RESET} {', '.join(reasons)}")
        out(f"  Tips:")
        out(f"  • Use --artist 'Artist Name' to force the artist")
        out(f"  • Or tag the files with proper albumartist/artist tags")
        out(f"  • Or lower review_rules.min_confidence_for_clean below {prop_new.confidence:.2f}")

# ─────────────────────────────────────────────
# cache — manage the tag cache
# ─────────────────────────────────────────────
def cmd_cache(cfg:Dict[str,Any],action:str)->None:
    cache_path=Path(cfg.get("logging",{}).get("root_dir","logs"))/"tag_cache.json"
    out(f"\n{C.BOLD}{'═'*50}{C.RESET}\n{C.BOLD}raagdosa cache{C.RESET}\n{'═'*50}")
    if action=="status":
        if not cache_path.exists():
            out("No tag cache found. It will be created on the next scan.")
            return
        try:
            raw=read_json(cache_path)
            entries=raw.get("entries",raw) if isinstance(raw,dict) else {}
            saved=raw.get("saved","unknown") if isinstance(raw,dict) else "unknown"
            size_kb=cache_path.stat().st_size/1024
            out(f"  Path:     {cache_path}")
            out(f"  Entries:  {len(entries):,}")
            out(f"  Size:     {size_kb:.0f} KB")
            out(f"  Saved:    {saved}")
            out(f"\n{C.DIM}Warm scans skip mutagen for cached files — near-zero tag-read cost.{C.RESET}")
        except Exception as e:
            err(f"Could not read cache: {e}")
    elif action=="clear":
        if not cache_path.exists(): out("No cache to clear."); return
        if input("Clear tag cache? This forces a full re-read on next scan. [y/N] ").strip().lower()!="y":
            out("Aborted."); return
        cache_path.unlink(); reset_tag_cache(); ok_msg("Tag cache cleared.")
    elif action=="evict":
        tc=TagCache(cache_path)
        removed=tc.evict_missing()
        tc.save()
        ok_msg(f"Evicted {removed} stale entries.")
    else:
        err(f"Unknown cache action: {action}")

# ─────────────────────────────────────────────
# clean-report — audit Clean library
# ─────────────────────────────────────────────
def cmd_clean_report(cfg:Dict[str,Any],profile_name:str)->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    setup_logging_paths(cfg, profile, source_root)
    roots=ensure_roots(profile,source_root); clean_albums=roots["clean_albums"]
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]

    out(f"\n{C.BOLD}{'═'*60}{C.RESET}\n{C.BOLD}raagdosa clean-report — {profile_name}{C.RESET}\n{'═'*60}")
    if not clean_albums.exists(): out("Clean/Albums not found."); return

    album_dirs:List[Path]=[]; total_tracks=0; ext_counts:Counter=Counter()
    tagged_count=0; untagged_count=0; issues:List[str]=[]

    for d in sorted(clean_albums.rglob("*")):
        if not d.is_dir(): continue
        files=list_audio_files(d,exts)
        if not files: continue
        album_dirs.append(d)
        total_tracks+=len(files)
        for f in files:
            ext_counts[f.suffix.lower()]+=1
            tags=read_audio_tags(f,cfg)
            if any(v for k,v in tags.items() if k not in ("bpm","key") and v): tagged_count+=1
            else: untagged_count+=1
        # Detect issues in this album folder
        track_nums:List[int]=[]
        for f in files:
            tags=read_audio_tags(f,cfg)
            vt=parse_vinyl_track((tags.get("tracknumber") or "").split("/")[0].strip())
            n=vt[2] if vt else parse_int_prefix(tags.get("tracknumber") or "")
            if n: track_nums.append(n)
        if detect_track_gaps(track_nums): issues.append(f"track_gaps: {d.name}")
        if detect_duplicate_track_numbers(track_nums): issues.append(f"duplicate_track_nums: {d.name}")

    out(f"\n{C.BOLD}Summary:{C.RESET}")
    out(f"  Album folders:       {len(album_dirs)}")
    out(f"  Total tracks:        {total_tracks}")
    out(f"  Tagged tracks:       {C.GREEN}{tagged_count}{C.RESET}")
    out(f"  Untagged tracks:     {C.RED}{untagged_count}{C.RESET}")
    out(f"\n{C.BOLD}Format breakdown:{C.RESET}")
    for ext,cnt in sorted(ext_counts.items(),key=lambda x:x[1],reverse=True):
        pct=cnt/total_tracks*100 if total_tracks else 0
        bar="█"*int(pct/2)
        out(f"  {ext:<8} {cnt:5d} tracks  {C.DIM}[{bar:<50}] {pct:.0f}%{C.RESET}")
    if issues:
        out(f"\n{C.YELLOW}Issues found ({len(issues)}):{C.RESET}")
        for i in issues[:30]: out(f"  {C.YELLOW}⚠{C.RESET}  {i}")
        if len(issues)>30: out(f"  … and {len(issues)-30} more")
    else:
        ok_msg("No issues detected in Clean library.")

# ─────────────────────────────────────────────
# extract --by-artist — split VA/mix folder
# ─────────────────────────────────────────────
def cmd_extract_by_artist(cfg:Dict[str,Any],profile_name:str,folder_path:str,dry_run:bool)->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    roots=ensure_roots(profile,source_root)
    folder=Path(folder_path).expanduser().resolve()
    if not folder.exists(): err(f"Folder not found: {folder}"); sys.exit(1)
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    audio_files=list_audio_files(folder,exts)
    if not audio_files: err("No audio files found."); sys.exit(1)

    out(f"\n{C.BOLD}{'═'*60}{C.RESET}\n{C.BOLD}raagdosa extract --by-artist: {folder.name}{C.RESET}\n{'═'*60}")

    by_artist:Dict[str,List[Path]]={}
    for f in sorted(audio_files):
        tags=read_audio_tags(f,cfg)
        artist=(tags.get("artist") or "").strip() or "_Unknown"
        artist=normalize_artist_name(artist,cfg)
        by_artist.setdefault(artist,[]).append(f)

    if len(by_artist)<=1: out("Only one artist found — nothing to extract."); return
    out(f"\nWould extract {len(by_artist)} artist group(s) into Clean/Albums:\n")
    clean_albums=roots["clean_albums"]
    for artist,files in sorted(by_artist.items()):
        dest=clean_albums/sanitize_name(artist)/"_Singles"
        out(f"  {C.GREEN}{artist:<40}{C.RESET}  {len(files):3d} track(s)  →  {dest}")
    if dry_run: out(f"\n{C.DIM}[dry-run] No files moved.{C.RESET}"); return
    if input("\nProceed? [y/N] ").strip().lower()!="y": out("Aborted."); return
    moved=0
    for artist,files in by_artist.items():
        dest=clean_albums/sanitize_name(artist)/"_Singles"; ensure_dir(dest)
        for f in files:
            target=dest/f.name
            if target.exists(): target=dest/(f.stem+" (extracted)"+f.suffix)
            try: shutil.move(str(f),str(target)); moved+=1
            except Exception as e: err(f"  Failed {f.name}: {e}")
    ok_msg(f"Extracted {moved} tracks into artist folders.")

# ─────────────────────────────────────────────
# compare --folder — diff two folders
# ─────────────────────────────────────────────
def cmd_compare_folders(cfg:Dict[str,Any],folder_a:str,folder_b:str)->None:
    fa=Path(folder_a).expanduser().resolve(); fb=Path(folder_b).expanduser().resolve()
    for p,label in ((fa,"A"),(fb,"B")):
        if not p.exists(): err(f"Folder {label} not found: {p}"); sys.exit(1)
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    fa_files={f.name for f in list_audio_files(fa,exts)}
    fb_files={f.name for f in list_audio_files(fb,exts)}
    out(f"\n{C.BOLD}{'═'*60}{C.RESET}")
    out(f"{C.BOLD}raagdosa compare{C.RESET}")
    out(f"  A: {fa}  ({len(fa_files)} tracks)")
    out(f"  B: {fb}  ({len(fb_files)} tracks)\n{'─'*60}")
    only_a=sorted(fa_files-fb_files); only_b=sorted(fb_files-fa_files); common=sorted(fa_files&fb_files)
    out(f"\n{C.GREEN}Common ({len(common)}):{C.RESET}")
    for n in common[:20]: out(f"  {n}")
    if len(common)>20: out(f"  … and {len(common)-20} more")
    if only_a:
        out(f"\n{C.YELLOW}Only in A ({len(only_a)}):{C.RESET}")
        for n in only_a: out(f"  {C.YELLOW}{n}{C.RESET}")
    if only_b:
        out(f"\n{C.CYAN}Only in B ({len(only_b)}):{C.RESET}")
        for n in only_b: out(f"  {C.CYAN}{n}{C.RESET}")
    # Tag comparison on common files
    if common:
        out(f"\n{C.BOLD}Tag comparison (first 5 common):{C.RESET}")
        for name in sorted(common)[:5]:
            ta=read_audio_tags(fa/name,cfg); tb=read_audio_tags(fb/name,cfg)
            diffs=[k for k in ("album","albumartist","artist","title","tracknumber","year") if (ta.get(k) or "")!=(tb.get(k) or "")]
            if diffs:
                out(f"  {C.DIM}{name}{C.RESET}")
                for k in diffs: out(f"    {k}: A={ta.get(k)!r}  B={tb.get(k)!r}")
            else:
                out(f"  {C.DIM}{name}{C.RESET}  {C.GREEN}tags match{C.RESET}")

# ─────────────────────────────────────────────
# diff — compare two session reports
# ─────────────────────────────────────────────
def cmd_diff(cfg:Dict[str,Any],session_a:str,session_b:str)->None:
    sdir=Path(cfg["logging"]["session_dir"])
    def _load(sid:str)->Dict[str,Any]:
        # Support "last" / "prev" shortcuts
        all_s=sorted([p for p in sdir.iterdir() if p.is_dir()],key=lambda p:p.name,reverse=True) if sdir.exists() else []
        if sid=="last" and all_s: sid=all_s[0].name
        elif sid=="prev" and len(all_s)>1: sid=all_s[1].name
        p=sdir/sid/"proposals.json"
        if not p.exists(): err(f"Session not found: {sid}"); sys.exit(1)
        pl=read_json(p); pl["_session_id"]=sid; return pl
    pa=_load(session_a); pb=_load(session_b)
    props_a={fp["proposed_folder_name"]:fp for fp in pa.get("folder_proposals",[])}
    props_b={fp["proposed_folder_name"]:fp for fp in pb.get("folder_proposals",[])}
    names_a=set(props_a); names_b=set(props_b)
    out(f"\n{C.BOLD}{'═'*60}{C.RESET}")
    out(f"{C.BOLD}raagdosa diff{C.RESET}")
    out(f"  A: {pa['_session_id']}  ({len(props_a)} proposals)")
    out(f"  B: {pb['_session_id']}  ({len(props_b)} proposals)\n{'─'*60}")
    only_a=sorted(names_a-names_b); only_b=sorted(names_b-names_a); common=sorted(names_a&names_b)
    if only_a:
        out(f"\n{C.YELLOW}Only in A ({len(only_a)}):{C.RESET}")
        for n in only_a: out(f"  {C.YELLOW}{n}{C.RESET}")
    if only_b:
        out(f"\n{C.CYAN}Only in B ({len(only_b)}):{C.RESET}")
        for n in only_b: out(f"  {C.CYAN}{n}{C.RESET}")
    changed_dest=[]; changed_conf=[]
    for n in common:
        fa=props_a[n]; fb=props_b[n]
        if fa.get("destination")!=fb.get("destination"):
            changed_dest.append((n,fa.get("destination"),fb.get("destination"),fa.get("confidence",0),fb.get("confidence",0)))
        elif abs(float(fa.get("confidence",0))-float(fb.get("confidence",0)))>0.05:
            changed_conf.append((n,float(fa.get("confidence",0)),float(fb.get("confidence",0))))
    if changed_dest:
        out(f"\n{C.BOLD}Destination changes ({len(changed_dest)}):{C.RESET}")
        for n,da,db,ca,cb in changed_dest:
            out(f"  {n[:50]:<50}  {da}→{db}  conf {ca:.2f}→{cb:.2f}")
    if changed_conf:
        out(f"\n{C.BOLD}Confidence changes >5% ({len(changed_conf)}):{C.RESET}")
        for n,ca,cb in changed_conf:
            delta=cb-ca; sym="↑" if delta>0 else "↓"
            out(f"  {n[:50]:<50}  {conf_color(ca)} {sym} {conf_color(cb)}")
    if not only_a and not only_b and not changed_dest and not changed_conf:
        ok_msg("Sessions are identical.")

# ─────────────────────────────────────────────
# report
# ─────────────────────────────────────────────
def cmd_report(cfg:Dict[str,Any],session_id:Optional[str],fmt:str)->None:
    sdir=Path(cfg["logging"]["session_dir"])
    if not session_id:
        sessions=sorted([p for p in sdir.iterdir() if p.is_dir()],key=lambda p:p.name,reverse=True) if sdir.exists() else []
        if not sessions: err("No sessions found."); sys.exit(1)
        session_id=sessions[0].name
    session_dir=sdir/session_id
    if not session_dir.exists(): err(f"Session not found: {session_id}"); sys.exit(1)
    fname={"txt":"report.txt","csv":"report.csv","html":"report.html"}.get(fmt,"report.txt")
    target=session_dir/fname
    if not target.exists():
        pp=session_dir/"proposals.json"
        if not pp.exists(): err(f"proposals.json not found for session {session_id}"); sys.exit(1)
        payload=read_json(pp); proposals=[fp_from_dict(fp) for fp in payload.get("folder_proposals",[])]
        _write_session_reports(payload.get("session_id",session_id),payload.get("profile",""),
                               Path(payload.get("source_root",".")),proposals,session_dir,cfg)
    out(f"\n{C.DIM}{target}{C.RESET}\n")
    if fmt=="txt": out(target.read_text(encoding="utf-8"))
    else: out(f"Open: {target}")

# ─────────────────────────────────────────────
# status
# ─────────────────────────────────────────────
def cmd_status(cfg:Dict[str,Any],profile_name:str)->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    setup_logging_paths(cfg, profile, source_root)
    roots=ensure_roots(profile,source_root)
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    def count_in(path:Path)->Tuple[int,int]:
        if not path.exists(): return 0,0
        folders=[p for p in path.rglob("*") if p.is_dir() and any(f.suffix.lower() in exts for f in p.iterdir() if f.is_file())]
        return len(folders),sum(len(list_audio_files(f,exts)) for f in folders)
    cf,ct=count_in(roots["clean_albums"]); rf,rt=count_in(roots["review_albums"]); df,dt_n=count_in(roots["duplicates"])
    manifest=read_manifest(cfg); committed=len(manifest.get("entries",{})); last_run=manifest.get("last_run")
    out(f"\n{C.BOLD}{'═'*50}{C.RESET}\n{C.BOLD}RaagDosa Status — {profile_name}{C.RESET}\n{'═'*50}")
    src_exists=source_root.exists()
    out(f"Source:              {source_root}  ({C.GREEN+'exists'+C.RESET if src_exists else C.RED+'MISSING'+C.RESET})")
    out(f"\n{C.GREEN}Clean/Albums:{C.RESET}        {cf:4d} folders  {ct:5d} tracks")
    out(f"{C.YELLOW}Review/Albums:{C.RESET}       {rf:4d} folders  {rt:5d} tracks")
    out(f"{C.RED}Review/Duplicates:{C.RESET}   {df:4d} folders  {dt_n:5d} tracks")
    out(f"\nManifest entries:    {committed}"); out(f"Last run:            {last_run or 'never'}")
    clean_str=str(roots["clean_root"].resolve()); review_str=str(roots["review_root"].resolve())
    min_t=int(cfg.get("scan",{}).get("min_tracks",3)); pending=0
    if source_root.exists():
        for root,dirs,files in os.walk(source_root):
            rp=str(Path(root).resolve())
            if rp.startswith(clean_str) or rp.startswith(review_str): dirs[:]=[] ; continue
            if sum(1 for f in files if Path(f).suffix.lower() in exts)>=min_t: pending+=1
    out(f"Pending in source:   {pending} folder(s)")
    lr_dt=manifest_get_last_run(cfg)
    if lr_dt and source_root.exists():
        new_since=sum(1 for root,dirs,files in os.walk(source_root)
                      if dt.datetime.fromtimestamp(folder_mtime(Path(root)))>=lr_dt
                      and sum(1 for f in files if Path(f).suffix.lower() in exts)>=min_t)
        out(f"New since last run:  {new_since} folder(s)  {C.DIM}(use --since last_run){C.RESET}")
    dj=cfg.get("dj_safety",{})
    if dj.get("detect_dj_databases",True) and source_root.exists():
        dbs=find_dj_databases(source_root)
        if dbs: warn(f"DJ databases: {', '.join(dbs)}")
        else: ok_msg("No DJ databases detected.")
    try:
        stat=shutil.disk_usage(str(roots["clean_albums"].parent))
        out(f"\nDisk (dest):         {stat.free/1024**3:.1f} GB free / {stat.total/1024**3:.1f} GB total")
    except Exception: pass
    out()

# ─────────────────────────────────────────────
# init
# ─────────────────────────────────────────────
def cmd_init(cfg_path:Path)->None:
    out(f"\n{C.BOLD}🍛 RaagDosa v{APP_VERSION} — Setup Wizard{C.RESET}\n{'='*50}")
    if cfg_path.exists():
        if input(f"\n  {cfg_path} exists. Overwrite? [y/N] ").strip().lower()!="y":
            out("Aborted."); return
    source=input("\n  Source root (your messy music folder): ").strip()
    if not source: err("Source root is required."); sys.exit(1)
    out("\n  Library template:\n    [1] artist/album  (recommended)\n    [2] album only  (flat)")
    template="{artist}/{album}" if (input("  Choose [1]: ").strip() or "1")=="1" else "{album}"
    flac_seg=input("\n  Segregate FLAC into artist/FLAC/album/? [y/N] ").strip().lower()=="y"
    out("\n  'The' prefix policy:\n    [1] keep-front (The Beatles)  [2] move-to-end (Beatles, The)  [3] strip (Beatles)")
    the_map={"1":"keep-front","2":"move-to-end","3":"strip"}
    the_policy=the_map.get(input("  Choose [1]: ").strip(),"keep-front")
    new_cfg={"app":{"name":"RaagDosa","version":APP_VERSION},
             "profiles":{"incoming":{"source_root":source,"clean_mode":"inside_root",
                "wrapper_folder_name":"raagdosa",
                "clean_folder_name":"Clean","review_folder_name":"Review",
                "clean_albums_folder_name":"Albums","clean_tracks_folder_name":"Tracks",
                "review_albums_folder_name":"Albums","duplicates_folder_name":"Duplicates","orphans_folder_name":"Orphans"}},
             "active_profile":"incoming",
             "library":{"template":template,"flac_segregation":flac_seg,"singles_folder":"_Singles","va_folder":"_Various Artists","unknown_artist_label":"_Unknown"},
             "artist_normalization":{"enabled":True,"the_prefix":the_policy,"normalize_hyphens":True,"fuzzy_dedup_threshold":0.92,"unicode_map":{},"aliases":{}},
             "scan":{"audio_extensions":[".mp3",".flac",".m4a",".aiff",".wav"],"leaf_folders_only":True,"min_tracks":3,"max_unreadable_track_ratio":0.25,"follow_symlinks":False,
                     "skip_sidecar_extensions":[".sfk",".asd",".reapeaks",".pkf",".db",".lrc"],
                     "skip_system_folders":["__MACOSX","__macosx"]},
             "ep_detection":{"enabled":True,"min_tracks":2,"max_tracks":6},
             "ignore":{"ignore_folder_names":["Singles","One-Offs","Dump","_dump","Clean","Review","raagdosa"]},
             "tags":{"album_keys":["album"],"albumartist_keys":["albumartist","album_artist","album artist"],
                     "artist_keys":["artist"],"title_keys":["title"],"tracknumber_keys":["tracknumber","track"],
                     "discnumber_keys":["discnumber","disc"],"year_keys_prefer":["originaldate","date","year"],
                     "bpm_keys":["bpm","tbpm"],"key_keys":["initialkey","key","tkey"]},
             "normalize":{"lower_case":True,"strip_whitespace":True,"collapse_whitespace":True,
                          "strip_punctuation_for_voting":True,"strip_bracketed_phrases_for_voting":True,
                          "strip_common_suffixes_for_voting":["deluxe edition","expanded edition","remaster","remastered","anniversary edition","bonus tracks","explicit","special edition"]},
             "fuzzy":{"enabled":True,"similarity_threshold":0.88,"prompt_threshold":0.75},
             "decision":{"album_dominance_threshold":0.75,"allow_artist_fallback":True,"require_confirmation":True,"auto_approve_above":0.92,"interactive_below":0.92},
             "various_artists":{"label":"VA","albumartist_matches":["various artists","various","va","v/a"],"enable_heuristics":True,"unique_artist_ratio_above":0.75},
             "year":{"enabled":True,"allowed_range":{"min":1900,"max":2030},"require_presence_ratio":0.50,"agreement_threshold":0.70},
             "format":{"pattern_no_year":"{albumartist} - {album}","pattern_with_year":"{albumartist} - {album} ({year})","replace_illegal_chars_with":" - ","trim_trailing_dots_spaces":True},
             "format_suffix":{"enabled":True,"only_if_all_same_extension":True,"ignore_extension":".mp3","style":"brackets_upper"},
             "review_rules":{"min_confidence_for_clean":0.85,"route_duplicates":True,"route_cross_run_duplicates":True,"route_questionable_to_review":True,"route_heuristic_to_review":True},
             "move":{"enabled":True,"on_collision":"suffix","suffix_format":" ({n})","use_checksum":False},
             "dj_safety":{"detect_dj_databases":True,"warn_on_dj_databases":True,"halt_on_dj_databases":False,
                          "database_patterns":["export.pdb","database2","rekordbox.xml","_Serato_","Serato Scratch","Serato DJ","PIONEER"]},
             "track_rename":{"enabled":True,"scope":"clean_only","allowed_extensions":[".mp3",".flac",".m4a"],"skip_extensions":[".wav",".aiff"],
                             "patterns":{"album":"{disc_prefix}{track:02d} - {title}{mix_suffix}{ext}","various":"{disc_prefix}{track:02d} - {artist} - {title}{mix_suffix}{ext}","mixed":"{artist} - {title}{mix_suffix}{ext}"},
                             "disc":{"enabled":True,"only_if_multi_disc":True,"format":"{disc}-"},
                             "track_numbers":{"required_for_album":True,"required_for_various":True,"pad_width":2,"fallback_to_filename_order":False}},
             "artists":{"feature_handling":{"enabled":True,"style":"keep_in_artist","normalize_tokens":True}},
             "mix_info":{"enabled":True,"detect_keywords":["remix","edit","mix","version","rework","bootleg","dub","original mix","extended mix","extended version","club mix","radio edit","instrumental","acapella","vip","flip"],"style":"parenthetical"},
             "title_cleanup":{"enabled":True,"strip_trailing_domains":True,"strip_trailing_handles":True,
                              "strip_trailing_phrases":["official video","official music video","official audio","lyrics","lyric video","free download","download","uploaded by","ripped by","encoded by","320kbps","lossless","hi-res","soundcloud","youtube","bandcamp","spotify","prod. by","prod by","clip","teaser","preview"],
                              "keep_parenthetical_if_contains":["live","remaster","edit","mix","version","instrumental","acapella","demo","mono","stereo","original","extended","club","radio","vip","dub","feat"],
                              "normalize":{"replace_underscores":True,"collapse_whitespace":True,"trim_dots_spaces":True}},
             "logging":{"root_dir":"logs","session_dir":"logs/sessions","history_log":"logs/history.jsonl",
                        "skipped_log":"logs/skipped.jsonl","track_history_log":"logs/track-history.jsonl",
                        "track_skipped_log":"logs/track-skipped.jsonl","write_human_report":True,"report_filename":"report.txt","rotate_log_max_mb":10.0},
             "undo":{"allow_undo_by_id":True,"allow_undo_by_original_path":True,"allow_undo_by_session":True,"allow_undo_by_folder":True}}
    write_yaml(cfg_path,new_cfg)
    out(f"\n{C.GREEN}✓ Config written: {cfg_path}{C.RESET}")
    out("\nNext steps:\n  1. Review config.yaml — especially artist_normalization.aliases\n  2. raagdosa doctor\n  3. raagdosa go --dry-run\n  4. raagdosa go")

# ─────────────────────────────────────────────
# High-level flows
# ─────────────────────────────────────────────
def load_last_session(cfg:Dict[str,Any])->Optional[Path]:
    sdir=Path(cfg["logging"]["session_dir"])
    if not sdir.exists(): return None
    for s in sorted([p for p in sdir.iterdir() if p.is_dir()],key=lambda p:p.name,reverse=True):
        cand=s/"proposals.json"
        if cand.exists(): return cand
    return None

def _parse_since(val:Optional[str],cfg:Dict[str,Any])->Optional[dt.datetime]:
    if not val: return None
    if val=="last_run": return manifest_get_last_run(cfg)
    try: return dt.datetime.fromisoformat(val)
    except Exception: err(f"Cannot parse --since '{val}'. Use ISO date or 'last_run'."); sys.exit(1)

def cmd_scan(cfg_path:Path,cfg:Dict[str,Any],profile:str,out_path:Optional[str],since:Optional[str],genre_roots:Optional[List[str]]=None,itunes_mode:bool=False,session_name:str="")->str:
    sid,sdir,_=scan_folders(cfg,profile,since=_parse_since(since,cfg),genre_roots=genre_roots,itunes_mode=itunes_mode,session_name=session_name)
    if out_path: shutil.copyfile(str(sdir/"proposals.json"),out_path)
    return sid

def cmd_apply(cfg:Dict[str,Any],proposals_path:Path,interactive:bool,auto_above:Optional[float],dry_run:bool)->None:
    payload=read_json(proposals_path); session_id=payload.get("session_id") or make_session_id()
    raw_props=payload.get("folder_proposals",[]); profile_name=payload.get("profile","")
    profiles=cfg.get("profiles",{})
    if profile_name in profiles:
        prof=profiles[profile_name]; source_root=Path(prof["source_root"]).expanduser()
        roots=ensure_roots(prof,source_root)
        viols=validate_proposal_paths(raw_props,[roots["clean_albums"],roots["review_albums"],roots["duplicates"]])
        if viols: err("⛔ Path validation failed:"); [err(f"  {v}") for v in viols]; sys.exit(1)
    folder_props=[fp_from_dict(fp) for fp in raw_props]
    applied=apply_folder_moves(cfg,folder_props,interactive=interactive,auto_above=auto_above,dry_run=dry_run,session_id=session_id)
    trc=cfg.get("track_rename",{})
    if trc.get("enabled",True) and trc.get("scope","clean_only") in ("clean_only","both"):
        for a in applied:
            if a.get("destination")=="clean":
                rename_tracks_in_clean_folder(cfg,Path(a["target_path"]),a.get("decision",{}),interactive=interactive,dry_run=dry_run,session_id=session_id)

def _run_core(cfg_path:Path,cfg:Dict[str,Any],profile:str,interactive:bool,dry_run:bool,
               since_str:Optional[str]=None,perf_tier:Optional[str]=None,
               genre_roots:Optional[List[str]]=None,itunes_mode:bool=False)->None:
    """
    True per-album streaming pipeline (v3.81).

    Architecture:
      - Walk source → queue of individual candidate folders
      - Scanner thread pre-reads tags for `lookahead` folders ahead
      - Main thread: for each folder: route → artifact quarantine → apply move
        → track rename → duplicate resolution → immediately continue to next
      - First album moves within seconds of starting on any library size
      - Performance tier controls workers, lookahead, and copy-path sleep
    """
    import queue as _queue, threading as _threading

    for lk in ["history_log","track_history_log"]:
        lp=Path(cfg.get("logging",{}).get(lk,""))
        if lp.name: rotate_log_if_needed(lp,float(cfg.get("logging",{}).get("rotate_log_max_mb",10.0)))
    register_stop_handler()

    profiles=cfg.get("profiles",{}); profile_obj=profiles[profile]
    source_root=Path(profile_obj["source_root"]).expanduser()
    if not source_root.exists(): raise FileNotFoundError(f"source_root missing: {source_root}")

    # v5.5 — resolve log paths into wrapper folder and merge skip-sets from config
    setup_logging_paths(cfg, profile_obj, source_root)
    _init_skip_sets(cfg)

    roots=ensure_roots(profile_obj,source_root)
    clean_albums=roots["clean_albums"]; review_albums=roots["review_albums"]; dup_root=roots["duplicates"]
    clean_root_str =str(roots["clean_root"].resolve())+os.sep
    review_root_str=str(roots["review_root"].resolve())+os.sep

    # ── Performance settings ──────────────────────────────────────
    perf=resolve_perf_settings(cfg, perf_tier)
    workers     =perf["workers"]
    lookahead   =perf["lookahead"]
    sleep_copy  =perf["sleep_copy_ms"]/1000.0

    sc=cfg.get("scan",{}); exts=[e.lower() for e in sc.get("audio_extensions",[".mp3",".flac",".m4a"])]
    min_tracks=int(sc.get("min_tracks",3)); follow_sym=bool(sc.get("follow_symlinks",False))
    leaf_only=bool(sc.get("leaf_folders_only",True))
    ignore_patterns:List[str]=list(cfg.get("ignore",{}).get("ignore_folder_names",[]) or [])
    since=_parse_since(since_str,cfg)

    rr=cfg.get("review_rules",{}); min_conf=float(rr.get("min_confidence_for_clean",0.85))
    max_unread=float(sc.get("max_unreadable_track_ratio",0.25))
    lib=cfg.get("library",{}); mixes_folder=lib.get("mixes_folder","_Mixes")
    mixes_root=clean_albums.parent/mixes_folder
    dc=cfg.get("duplicates",{}); do_dup_compare=bool(dc.get("compare_before_routing",True))
    do_merge    =bool(dc.get("merge_missing_tracks",True))
    flac_coex   =dc.get("flac_mp3_coexistence","keep_both")
    do_artifacts=bool(cfg.get("artifacts",{}).get("enabled",True))
    trc=cfg.get("track_rename",{})
    do_tracks=(trc.get("enabled",True) and trc.get("scope","clean_only") in ("clean_only","both"))

    # Session setup
    session_id=make_session_id()
    session_dir=Path(cfg["logging"]["session_dir"])/session_id; ensure_dir(session_dir)
    out(f"\n{C.BOLD}Session:{C.RESET}   {session_id}")
    tier_name=perf_tier or cfg.get("performance",{}).get("tier","medium")
    out(f"{C.DIM}Performance: {tier_name}  workers={workers}  lookahead={lookahead}  sleep={perf['sleep_copy_ms']}ms{C.RESET}",level=VERBOSE)

    # Pre-load existing Clean names for cross-run dedup
    seen_names:Counter=Counter()
    existing_clean:Set[str]=set()
    existing_clean_paths:Dict[str,Path]={}    # name → actual path (for comparison)
    if clean_albums.exists():
        try:
            for item in clean_albums.rglob("*"):
                if item.is_dir():
                    n=normalize_unicode(item.name)
                    existing_clean.add(n)
                    existing_clean_paths[n]=item
        except Exception: pass
    manifest_entries:Set[str]=set(read_manifest(cfg).get("entries",{}).keys())

    # ── Phase 1: collect candidate paths ──────────────────────────
    candidates:List[Path]=[]
    for root,dirs,files in os.walk(source_root,followlinks=follow_sym):
        rp=Path(root); rp_str=str(rp.resolve())+os.sep
        if rp_str.startswith(clean_root_str) or rp_str.startswith(review_root_str): dirs[:]=[];continue
        if leaf_only and dirs: continue
        if sum(1 for f in files if Path(f).suffix.lower() in exts)>=min_tracks: candidates.append(rp)
    if since:
        candidates=[f for f in candidates if dt.datetime.fromtimestamp(folder_mtime(f))>=since]
        out(f"  --since: {len(candidates)} folders modified after {since.strftime('%Y-%m-%d %H:%M')}",level=VERBOSE)
    candidates=[rp for rp in candidates if not folder_matches_ignore(rp.name,ignore_patterns)]
    total_n=len(candidates)
    out(f"{C.DIM}Candidates: {total_n} folder(s){C.RESET}",level=VERBOSE)

    # ── Pre-flight: confirmation gate + disk space check ──
    if total_n and not dry_run:
        total_bytes=sum(_folder_size(c) for c in candidates)
        out(f"\n  {C.BOLD}Ready to process {total_n} folder(s) ({_human_size(total_bytes)}){C.RESET}")
        try:
            dest_path=clean_albums if clean_albums.exists() else clean_albums.parent
            free_bytes=shutil.disk_usage(dest_path).free
            if total_bytes > free_bytes * 0.9:
                out(f"  {C.YELLOW}Low disk space! Need ~{_human_size(total_bytes)}, only {_human_size(free_bytes)} free.{C.RESET}")
        except OSError:
            pass
        try:
            answer=input(f"  Continue? [y/N] ").strip().lower()
        except (EOFError,KeyboardInterrupt):
            answer=""
        if answer != "y":
            out(f"  {C.DIM}Cancelled.{C.RESET}"); return

    _get_tag_cache(cfg)

    # ── Streaming pipeline ─────────────────────────────────────────
    # Queue carries individual FolderProposal|None (sentinel)
    # lookahead controls queue maxsize — scanner runs this many folders ahead
    scanner_queue:_queue.Queue=_queue.Queue(maxsize=max(1,lookahead))
    all_proposals:List[FolderProposal]=[]
    total_clean=0; total_review=0; total_dup=0; total_merged=0; total_artifacts=0; total_failed=0
    prog=Progress(total_n,"Processing")

    def _scan_one(rp:Path)->Optional[FolderProposal]:
        if should_stop(): return None
        audio_files=list_audio_files(rp,exts,follow_sym)
        if len(audio_files)<min_tracks: prog.tick(rp.name); return None
        return build_folder_proposal(rp,audio_files,source_root,profile_obj,cfg)

    def _scanner_worker():
        # Pre-read tags for each folder; push individual proposals to queue
        if workers>1 and total_n>1:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures={pool.submit(_scan_one,rp):rp for rp in candidates}
                # Submit in order; yield in completion order
                for fut in as_completed(futures):
                    if should_stop(): break
                    try:
                        prop=fut.result()
                        scanner_queue.put(prop)   # None = skip, main thread handles
                    except Exception as e:
                        err(f"  scan error: {e}"); scanner_queue.put(None)
        else:
            for rp in candidates:
                if should_stop(): scanner_queue.put(None); break
                scanner_queue.put(_scan_one(rp))
        scanner_queue.put("DONE")
        if _tag_cache is not None:
            _tag_cache.save()
            out(f"  Tag cache: {_tag_cache.size} entries saved",level=VERBOSE)

    def _route_one(p:FolderProposal)->FolderProposal:
        """Route a single proposal. Updates seen_names."""
        reasons:List[str]=[]; dest="clean"
        if rr.get("route_questionable_to_review",True) and p.confidence<min_conf:
            dest="review"; reasons.append("low_confidence")
        seen_names[p.proposed_folder_name]+=1
        if rr.get("route_duplicates",True) and seen_names[p.proposed_folder_name]>1:
            dest="duplicate"; reasons.append("duplicate_in_run")
        norm_prop=normalize_unicode(p.proposed_folder_name)
        is_existing_clean=(norm_prop in existing_clean or norm_prop in manifest_entries)
        if rr.get("route_cross_run_duplicates",True) and is_existing_clean:
            dest="duplicate"; reasons.append("already_in_clean")
        if p.decision.get("unreadable_ratio",0.0)>max_unread:
            dest="review"; reasons.append("unreadable_ratio_high")
        if p.decision.get("used_heuristic",False):
            if dest=="clean": dest="review"
            reasons.append("heuristic_fallback")
        if p.stats.format_duplicates: reasons.append(f"format_dupes({len(p.stats.format_duplicates)})")
        if p.decision.get("is_ep"): reasons.append("ep")
        if p.decision.get("is_mix") and dest=="clean":
            reasons.append("mix_folder"); ensure_dir(mixes_root)
            p.target_path=str(mixes_root/p.proposed_folder_name)
        p.destination=dest; p.decision["route_reasons"]=reasons
        if reasons:
            p.decision["review_summary"]=_build_review_summary(reasons,p.decision.get("confidence_factors",{}),p.confidence)
        if dest=="review":      p.target_path=str(review_albums/p.proposed_folder_name)
        elif dest=="duplicate": p.target_path=str(dup_root/p.proposed_folder_name)
        return p

    def _handle_duplicate(p:FolderProposal)->str:
        """
        Intelligent duplicate handling. Returns final outcome label.
        Mutates p.destination and p.decision in place.
        """
        if not do_dup_compare: return "duplicate"
        norm_prop=normalize_unicode(p.proposed_folder_name)
        ex_path=existing_clean_paths.get(norm_prop)
        if ex_path is None or not ex_path.exists(): return "duplicate"

        inc_path=Path(p.folder_path)
        # Read tags for both sides (use cache)
        inc_audio=list_audio_files(inc_path,exts,follow_sym)
        ex_audio =list_audio_files(ex_path, exts,follow_sym)
        inc_tags=[read_audio_tags(f,cfg) for f in inc_audio]
        ex_tags =[read_audio_tags(f,cfg) for f in ex_audio]

        result=compare_with_existing(inc_path,ex_path,inc_tags,ex_tags,cfg)
        outcome=result["outcome"]
        p.decision["dup_compare"]=result
        p.decision["dup_compare"]["missing_in_existing"]=[str(f) for f in result.get("missing_in_existing",[])]

        if outcome=="missing_tracks" and do_merge:
            missing=result.get("missing_in_existing",[])
            copied=merge_missing_tracks(missing,ex_path,cfg,session_id,dry_run=dry_run)
            p.decision["merged_tracks"]=copied
            p.destination="merged"; p.decision["route_reasons"].append(f"merged_{len(copied)}_tracks")
            # Re-run track rename on the now-augmented existing folder
            if do_tracks and not dry_run:
                rename_tracks_in_clean_folder(cfg,ex_path,p.decision,interactive=False,
                                               dry_run=False,session_id=session_id)
            return "merged"

        if outcome=="format_upgrade":
            if flac_coex=="keep_both" or flac_coex=="prefer_flac":
                if lib.get("flac_segregation",False):
                    # Route to FLAC sub-folder
                    artist_dir=ex_path.parent
                    flac_dest=artist_dir/"FLAC"/p.proposed_folder_name
                    p.target_path=str(flac_dest)
                    p.destination="clean"; p.decision["route_reasons"].append("flac_segregation")
                    return "flac_upgrade"
            p.decision["route_reasons"].append("format_upgrade")
            return "format_upgrade_review"

        if outcome=="lower_quality_mp3":
            p.decision["route_reasons"].append("lower_quality_mp3")
            return "lower_quality_mp3"

        p.decision["route_reasons"].append(outcome)
        return outcome

    # Scanner starts immediately
    scanner_thread=_threading.Thread(target=_scanner_worker,daemon=True)
    scanner_thread.start()

    folder_idx=0
    hist_path=Path(cfg.get("logging",{}).get("history_log","logs/history.jsonl"))

    while True:
        item=scanner_queue.get()
        if item=="DONE": break
        if item is None:
            # Either below min_tracks or scan error — just tick
            prog.tick(""); continue
        if should_stop():
            out(f"\n{C.YELLOW}Stop requested.{C.RESET}"); break

        p:FolderProposal=item
        folder_idx+=1
        prog.tick(p.folder_name)
        p=_route_one(p)
        src_path=Path(p.folder_path)

        # ── Artifact quarantine (before move) ──────────────────
        artifact_count=0
        if do_artifacts and src_path.exists():
            artifacts=classify_artifacts(src_path,cfg)
            artifact_count=move_artifacts_to_quarantine(
                artifacts,p.folder_name,cfg,profile_obj,source_root,dry_run,session_id)
            if artifact_count:
                p.decision["artifacts_quarantined"]=artifact_count
                total_artifacts+=artifact_count

        # ── Duplicate resolution ────────────────────────────────
        dup_outcome=None
        if p.destination=="duplicate":
            dup_outcome=_handle_duplicate(p)
            if dup_outcome=="merged":
                total_merged+=1
                out(f"  [{folder_idx}/{total_n}]  {C.CYAN}MERGED{C.RESET} ⟳  {p.folder_name}  "
                    f"({len(p.decision.get('merged_tracks',[]))} tracks added)")
                all_proposals.append(p); continue   # no folder move needed
            if dup_outcome=="flac_upgrade":
                p.destination="clean"   # will move to FLAC subfolder

        # ── Apply folder move ───────────────────────────────────
        if p.destination in ("clean","review","duplicate"):
            target=Path(p.target_path); ensure_dir(target.parent)
            if dry_run:
                same=_same_device(src_path,target)
                out(f"  [{folder_idx}/{total_n}]  {C.DIM}[dry-run][{'rename' if same else 'copy'}]{C.RESET}"
                    f"  {status_tag(p.destination)}  {p.folder_name}  →  {target.name}"
                    f"  conf={conf_color(p.confidence)}")
            elif src_path.exists():
                try:
                    move_method,move_elapsed=safe_move_folder(src_path,target)
                    if sleep_copy>0 and move_method=="copy":
                        import time as _time; _time.sleep(sleep_copy)
                    # Log action
                    action_id=uuid.uuid4().hex[:10]
                    entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,
                           "type":"folder","original_path":str(src_path),
                           "original_parent":str(src_path.parent),"original_folder_name":p.folder_name,
                           "target_path":str(target),"target_parent":str(target.parent),
                           "target_folder_name":target.name,"destination":p.destination,
                           "confidence":p.confidence,"decision":p.decision,
                           "move_method":move_method}
                    append_jsonl(hist_path,entry)
                    if p.destination=="clean":
                        manifest_add(cfg,target.name,{"original_path":str(src_path),
                                                       "confidence":p.confidence,"session_id":session_id})
                        existing_clean.add(normalize_unicode(target.name))
                        existing_clean_paths[normalize_unicode(target.name)]=target
                    elif p.destination=="review":
                        _write_review_sidecar(target,p,session_id)
                    method_tag=f"  {C.DIM}[{move_method} {move_elapsed*1000:.0f}ms]{C.RESET}" if _verbosity>=VERBOSE else ""
                    art_tag=f"  {C.DIM}[{artifact_count} artifacts quarantined]{C.RESET}" if artifact_count else ""
                    out(f"  [{folder_idx}/{total_n}]  MOVED {status_tag(p.destination)}"
                        f"  {C.DIM}{p.folder_name}{C.RESET}  →  {target.name}"
                        f"  conf={conf_color(p.confidence)}{method_tag}{art_tag}")
                    try: cleanup_empty_parents(src_path,source_root)
                    except Exception: pass
                    # Track rename immediately after move
                    if do_tracks and p.destination=="clean":
                        rename_tracks_in_clean_folder(cfg,target,p.decision,
                                                       interactive=interactive,dry_run=dry_run,
                                                       session_id=session_id)
                except RuntimeError as e:
                    err(f"  [{folder_idx}/{total_n}]  ⛔ FAILED  {p.folder_name}: {e}")
                    total_failed+=1

        # Tally
        if   p.destination=="clean":     total_clean+=1
        elif p.destination=="review":    total_review+=1
        elif p.destination in ("duplicate","merged"): total_dup+=1
        all_proposals.append(p)

    prog.done()
    scanner_thread.join(timeout=5)
    if _tag_cache is not None: _tag_cache.save()

    # Session report
    payload={"app":cfg.get("app",{}),"session_id":session_id,"timestamp":now_iso(),
             "profile":profile,"source_root":str(source_root),
             "since":since.isoformat() if since else None,
             "folder_proposals":[dataclasses.asdict(p) for p in all_proposals]}
    write_json(session_dir/"proposals.json",payload)
    _write_session_reports(session_id,profile,source_root,all_proposals,session_dir,cfg)

    merged_tag=f"  {C.CYAN}Merged: {total_merged}{C.RESET}" if total_merged else ""
    fail_tag=f" | {C.RED}Failed: {total_failed}{C.RESET}" if total_failed else ""
    art_tag=f"  {C.DIM}Artifacts quarantined: {total_artifacts}{C.RESET}" if total_artifacts else ""
    out(f"\n{C.BOLD}Results:{C.RESET}   {len(all_proposals)} processed | "
        f"{C.GREEN}Clean: {total_clean}{C.RESET} | {C.YELLOW}Review: {total_review}{C.RESET} | "
        f"{C.RED}Dupes: {total_dup}{C.RESET}{merged_tag}{fail_tag}")
    if total_failed:
        out(f"  {C.YELLOW}⚠ {total_failed} folder(s) failed. Successfully moved folders can be undone:{C.RESET}")
        out(f"  raagdosa undo --session {session_id}")
    if art_tag: out(art_tag)
    out(f"{C.DIM}Reports:   {session_dir}/report.{{txt,csv,html}}{C.RESET}")
    manifest_set_last_run(cfg)


def _interactive_streaming(cfg:Dict[str,Any],profile_name:str,dry_run:bool=False,
                           since_str:Optional[str]=None,genre_roots:Optional[List[str]]=None,
                           itunes_mode:bool=False,threshold:Optional[float]=None,
                           sort_by:str="name")->None:
    """
    v7.0: Streaming interactive mode — scan one folder at a time, display the review
    card, wait for user action, then move to the next folder. No parallel scanning.
    This avoids the multitask/worker clash with interactive prompts.
    """
    register_stop_handler()
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile_obj=profiles[profile_name]; source_root=Path(profile_obj["source_root"]).expanduser()
    if not source_root.exists(): raise FileNotFoundError(f"source_root missing: {source_root}")

    setup_logging_paths(cfg, profile_obj, source_root)
    _init_skip_sets(cfg)
    effective_genre_roots = _resolve_genre_roots(cfg, genre_roots)
    roots=ensure_roots(profile_obj,source_root)
    clean_albums=roots["clean_albums"]; review_albums=roots["review_albums"]; dup_root=roots["duplicates"]
    wrapper_root_str=str(derive_wrapper_root(profile_obj,source_root).resolve())+os.sep
    clean_root_str =str(derive_clean_root(profile_obj,source_root).resolve())+os.sep
    review_root_str=str(derive_review_root(profile_obj,source_root).resolve())+os.sep

    sc=cfg.get("scan",{}); exts=[e.lower() for e in sc.get("audio_extensions",[".mp3",".flac",".m4a"])]
    min_tracks=int(sc.get("min_tracks",3)); follow_sym=bool(sc.get("follow_symlinks",False))
    leaf_only=bool(sc.get("leaf_folders_only",True))
    ignore_patterns:List[str]=list(cfg.get("ignore",{}).get("ignore_folder_names",[]) or [])
    skip_folder_names = set(_SKIP_FOLDER_NAMES)
    skip_exts = set(_SKIP_AUDIO_EXTENSIONS)

    since=_parse_since(since_str,cfg)
    _get_tag_cache(cfg)

    # Session
    session_id=make_session_id(profile_name, str(source_root))
    session_dir=Path(cfg["logging"]["session_dir"])/session_id; ensure_dir(session_dir)

    # Pre-load existing Clean names for dedup
    existing_clean:Set[str]=set()
    if clean_albums.exists():
        try:
            for item in clean_albums.rglob("*"):
                if item.is_dir(): existing_clean.add(normalize_unicode(item.name))
        except Exception: pass
    manifest_entries:Set[str]=set(read_manifest(cfg).get("entries",{}).keys())
    lib=cfg.get("library",{}); mixes_folder=lib.get("mixes_folder","_Mixes")
    mixes_root=clean_albums.parent/mixes_folder
    seen_names:Counter=Counter()

    # Collect candidates (fast walk, no tag reading)
    candidates:List[Path]=[]
    for root,dirs,files in os.walk(source_root,followlinks=follow_sym):
        rp=Path(root); rp_str=str(rp.resolve())+os.sep
        if rp_str.startswith(wrapper_root_str): dirs[:]=[];continue
        if rp_str.startswith(clean_root_str) or rp_str.startswith(review_root_str): dirs[:]=[];continue
        dirs[:] = [d for d in dirs if d not in skip_folder_names]
        if effective_genre_roots and rp.name in effective_genre_roots and rp != source_root: continue
        if leaf_only and dirs: continue
        audio_count = sum(1 for f in files
                          if Path(f).suffix.lower() in exts
                          and Path(f).suffix.lower() not in skip_exts
                          and not f.startswith("._"))
        if audio_count >= min_tracks: candidates.append(rp)
    if since:
        candidates=[f for f in candidates if dt.datetime.fromtimestamp(folder_mtime(f))>=since]
    candidates=[rp for rp in candidates if not folder_matches_ignore(rp.name,ignore_patterns)]

    # Sort candidates based on user preference
    if sort_by=="date-created":
        candidates.sort(key=lambda p:p.stat().st_birthtime if hasattr(p.stat(),"st_birthtime") else p.stat().st_ctime)
        sort_label="date created"
    elif sort_by=="date-modified":
        candidates.sort(key=lambda p:p.stat().st_mtime)
        sort_label="date modified"
    else:
        # Default: name — symbols first, then numbers, then letters (natural sort)
        candidates.sort(key=lambda p:p.name.lower())
        sort_label="name"

    total=len(candidates)
    if total==0:
        out(f"\n  {C.DIM}No candidate folders found.{C.RESET}"); return

    # Session header
    print(f"\n{'═'*66}")
    print(f"  {C.BOLD}RAAGDOSA v{APP_VERSION}  ·  Interactive Review  ·  Session {session_id[:12]}{C.RESET}")
    print(f"{'─'*66}")
    print(f"  Candidates: {total} folder(s)  ·  Scanning one at a time  ·  Sort: {sort_label}")
    if threshold: print(f"  Threshold: only review folders below {threshold:.2f}")
    if dry_run: print(f"  {C.YELLOW}DRY RUN — nothing will be moved{C.RESET}")
    print(f"  Press ? for help at any prompt.")
    print(f"{'═'*66}")

    # Reuse the interactive_review action loop internals
    mc=cfg.get("move",{}); policy=mc.get("on_collision","suffix"); sfmt=mc.get("suffix_format"," ({n})")
    use_cs=bool(mc.get("use_checksum",False))
    hist_path=Path(cfg["logging"]["history_log"]); skip_path=Path(cfg["logging"]["skipped_log"])
    session_notes_path=session_dir/"review_notes.jsonl"

    # ── Tracking (moves happen immediately, these track what was done) ──
    applied:List[Dict[str,Any]]=[]   # successful move entries
    skipped_count=0
    moved_clean=0; moved_review=0
    held:List[Tuple[int,Path]]=[]    # user said [f] — review again later
    auto_approved=0; stopped=False

    def _execute_move(p_:FolderProposal,action:str="approved",reason:str="")->bool:
        """Execute a single folder move immediately. Returns True on success."""
        nonlocal moved_clean, moved_review
        if dry_run:
            dest_label=f"Review/{p_.proposed_folder_name}" if p_.destination=="review" else p_.proposed_folder_name
            out(f"  {C.DIM}[dry-run]{C.RESET} → {dest_label}  {status_tag(p_.destination)}")
            return True
        src_=Path(p_.folder_path); dst_=Path(p_.target_path)
        if not src_.exists(): return False
        dst2=collision_resolve(dst_,policy,sfmt)
        if dst2 is None:
            warn(f"Collision skip: {dst_.name}")
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"collision_skip","src":str(src_)})
            return False
        ensure_dir(dst2.parent)
        try:
            move_method,_=safe_move_folder(src_,dst2,use_checksum=use_cs)
        except RuntimeError as e:
            err(f"  Move failed: {src_.name}: {e}"); return False
        action_id=uuid.uuid4().hex[:10]
        entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,"type":"folder",
               "original_path":str(src_),"original_parent":str(src_.parent),"original_folder_name":src_.name,
               "target_path":str(dst2),"target_parent":str(dst2.parent),"target_folder_name":dst2.name,
               "destination":p_.destination,"confidence":p_.confidence,"decision":p_.decision,
               "move_method":move_method,"interactive_action":action}
        if reason: entry["user_note"]=reason
        append_jsonl(hist_path,entry); applied.append(entry)
        if p_.destination=="clean":
            manifest_add(cfg,dst2.name,{"original_path":str(src_),"confidence":p_.confidence,"session_id":session_id})
            existing_clean.add(normalize_unicode(dst2.name))
            moved_clean+=1
        elif p_.destination=="review":
            _write_review_sidecar(dst2,p_,session_id)
            moved_review+=1
        if reason:
            ensure_dir(session_notes_path.parent)
            append_jsonl(session_notes_path,{"folder":src_.name,"action":action,"note":reason,"timestamp":now_iso(),"confidence":p_.confidence})
        try: cleanup_empty_parents(src_,source_root)
        except Exception: pass
        return True

    def _re_derive_target(p_:FolderProposal)->None:
        """Re-derive target path from current decision state."""
        ca=derive_clean_albums_root(profile_obj,source_root)
        art=p_.decision.get("albumartist_display","Unknown")
        new_t=resolve_library_path(ca,art,p_.decision.get("dominant_album_display",""),
                                   p_.decision.get("year"),p_.decision.get("is_flac_only",False),
                                   p_.decision.get("is_va",False),False,p_.decision.get("is_mix",False),
                                   cfg,profile_obj,genre=p_.decision.get("genre"),
                                   bpm=p_.decision.get("bpm"),key=p_.decision.get("key"),
                                   label=p_.decision.get("label"))
        # Re-apply format suffix (e.g. [FLAC]) — resolve_library_path doesn't add it
        folder_name=_apply_format_suffix(new_t.name,cfg,p_.stats.extensions if p_.stats else None)
        # Re-apply disc subfolder if present
        disc_sub=p_.decision.get("disc_subfolder")
        if disc_sub:
            new_t=new_t/disc_sub
        p_.proposed_folder_name=folder_name; p_.target_path=str(new_t); p_.destination="clean"

    def _run_review_pass(items:List[Tuple[int,Path]],pass_label:str="Review"):
        """Run the interactive review loop over a list of (idx, candidate_path) tuples."""
        nonlocal auto_approved, stopped, skipped_count
        for idx,rp in items:
            if should_stop() or stopped:
                out(f"\n{C.YELLOW}Stopped.{C.RESET}"); stopped=True; break

            # ── Scan this one folder ──────────────────────────────
            audio_files=list_audio_files(rp,exts,follow_sym)
            if len(audio_files)<min_tracks: continue
            p=build_folder_proposal(rp,audio_files,source_root,profile_obj,cfg)
            if p is None: continue

            # ── Route it ──────────────────────────────────────────
            _route_proposal(p,cfg,seen_names,existing_clean,manifest_entries,
                            review_albums,dup_root,mixes_root)

            # ── Threshold filter: auto-accept high-confidence folders ──
            if threshold is not None and p.confidence>=threshold:
                _execute_move(p,"auto_approved")
                auto_approved+=1; continue

            # ── Display card + track preview by default ─────────
            _display_folder_card(idx,total,p)
            _display_tracks(p,cfg)

            # Build track file list for e<N> track-title editing
            _tf_exts={".mp3",".flac",".m4a",".aiff",".wav",".ogg",".opus",".wma"}
            _stream_track_files=sorted([f for f in rp.iterdir() if f.suffix.lower() in _tf_exts],key=lambda f:f.name) if rp.exists() else []

            # Running tally
            tally=f"{C.DIM}{moved_clean} moved · {moved_review} review · {skipped_count} skipped{C.RESET}"

            while True:
                if p.confidence<0.50:
                    print(f"  {C.DIM}z:move  x:reject  c:skip  space:tracks  e:edit  o:finder  R:rescan  ?:more  ({tally}){C.RESET}")
                else:
                    print(f"  z:move  x:reject  c:skip  space:tracks  e:edit  o:finder  R:rescan  ?:more  ({tally})")
                try:
                    _raw=input(f"  > ").strip()
                    choice=_raw if _raw in ("R",) else _raw.lower()
                except (EOFError,KeyboardInterrupt):
                    choice="q"

                if choice in ("","z","y"):
                    # Confidence gate — warn but always allow explicit user override
                    if p.confidence<0.50:
                        confirm=input(f"  {C.RED}Very low confidence ({p.confidence:.2f}).{C.RESET} Force move anyway? [y/N]: ").strip().lower()
                        if confirm not in ("y","yes","z"): continue
                    elif p.confidence<0.70:
                        confirm=input(f"  {C.YELLOW}Low confidence ({p.confidence:.2f}).{C.RESET} Move anyway? [y/n]: ").strip().lower()
                        if confirm not in ("y","yes","z"): continue
                    if _execute_move(p):
                        out(f"  {C.GREEN}✓ Moved{C.RESET} → {p.proposed_folder_name}")
                    break

                elif choice in ("x","d","r"):
                    # Reject — move to Review/ immediately with optional reason
                    reason=_prompt_review_note()
                    p.destination="review"
                    p.decision["route_reasons"]=p.decision.get("route_reasons",[])+["user_rejected"]
                    p.decision["user_review_note"]=reason
                    review_target=review_albums/p.proposed_folder_name
                    p.target_path=str(review_target)
                    if _execute_move(p,"rejected",reason):
                        reason_str=f"  ({reason})" if reason else ""
                        out(f"  {C.YELLOW}→ Review/{C.RESET}{reason_str}")
                    break

                elif choice in ("c","k","s"):
                    skipped_count+=1
                    append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder",
                                            "reason":"user_skipped","src":str(Path(p.folder_path)),"interactive_action":"skipped"})
                    out(f"  {C.DIM}→ Skipped{C.RESET}"); break

                elif choice=="e":
                    # e = album title edit (e<N> = track title edit — separate operations)
                    current_album=p.decision.get("dominant_album_display","")
                    print(f"  {C.DIM}Current:{C.RESET} {current_album}")
                    new_album=input(f"  New album title (Enter to cancel): ").strip()
                    if not new_album: continue
                    p.decision["dominant_album_display"]=new_album
                    p.decision["override_type"]=p.decision.get("override_type","")+"edit_title"
                    _re_derive_target(p)
                    print(f"  {C.CYAN}─ UPDATED ─{C.RESET}")
                    print(f"  Album: {C.BOLD}{new_album}{C.RESET}")
                    print(f"    {C.BOLD}→ {p.proposed_folder_name}{C.RESET}")

                elif choice.startswith("e") and len(choice)>1 and choice[1:].strip().isdigit():
                    # e<N> = edit individual track title tag (separate from album title)
                    track_num=int(choice[1:].strip())
                    if 1<=track_num<=len(_stream_track_files):
                        _edit_track_title(_stream_track_files[track_num-1],cfg)
                        print(f"  {C.DIM}Press space to refresh track view.{C.RESET}")
                    else:
                        print(f"  {C.DIM}Track {track_num} not found (folder has {len(_stream_track_files)} tracks).{C.RESET}")

                elif choice=="a":
                    current=p.decision.get("albumartist_display","--")
                    print(f"  {C.DIM}Current:{C.RESET} {current}")
                    new_artist=input(f"  New artist name: ").strip()
                    if not new_artist: continue
                    p.decision["albumartist_display"]=new_artist
                    p.decision["is_va"]=False
                    p.decision["override_type"]="set_artist"
                    p.decision["override_original"]=current
                    _re_derive_target(p)
                    print(f"  {C.CYAN}─ UPDATED ─{C.RESET}")
                    print(f"  Artist: {C.BOLD}{new_artist}{C.RESET}  (was: {current})")
                    print(f"    {C.BOLD}→ {p.proposed_folder_name}{C.RESET}")

                elif choice=="v":
                    was_va=p.decision.get("is_va",False)
                    if was_va:
                        new_artist=input(f"  Artist name: ").strip()
                        if not new_artist: continue
                        p.decision["is_va"]=False; p.decision["albumartist_display"]=new_artist
                        p.decision["override_type"]="unmark_va"
                    else:
                        p.decision["is_va"]=True
                        p.decision["albumartist_display"]="Various Artists"
                        p.decision["override_type"]="mark_va"
                    _re_derive_target(p)
                    va_label="VA" if p.decision["is_va"] else p.decision["albumartist_display"]
                    print(f"  {C.CYAN}→ {va_label}{C.RESET}  TO: {p.proposed_folder_name}")

                elif choice=="f":
                    held.append((idx,rp))
                    out(f"  {C.YELLOW}→ Flagged{C.RESET}"); break

                elif choice=="u":
                    # In-session undo picker
                    if not applied:
                        print(f"  {C.DIM}Nothing moved yet this session.{C.RESET}"); continue
                    print(f"\n  {C.BOLD}Moves this session:{C.RESET}")
                    print(f"  {'#':<4} {'Folder':<55} {'Dest'}")
                    print(f"  {'─'*66}")
                    for ui,uh in enumerate(applied,1):
                        uname=Path(uh.get("original_path","")).name or uh.get("action_id","")
                        udest=uh.get("destination","?")
                        ucol=C.GREEN if udest=="clean" else C.YELLOW
                        print(f"  {ui:<4} {uname:<55} {ucol}{udest}{C.RESET}")
                    print(f"\n  Enter number(s) to undo (e.g. 3  or  1,3), or Enter to cancel:")
                    try:
                        uraw=input(f"  > ").strip().lower()
                    except (EOFError,KeyboardInterrupt):
                        uraw=""
                    if not uraw: continue
                    uidxs=[]
                    for tok in re.split(r"[,\s]+",uraw):
                        try: uidxs.append(int(tok)-1)
                        except ValueError: pass
                    for ui in sorted(set(uidxs),reverse=True):
                        if not 0<=ui<len(applied): continue
                        uh=applied[ui]
                        usrc=Path(uh["target_path"]); udst=Path(uh["original_path"])
                        if not usrc.exists():
                            print(f"  {C.RED}SKIP (missing):{C.RESET} {usrc.name}"); continue
                        try:
                            shutil.move(str(usrc),str(udst))
                            applied.pop(ui)
                            print(f"  {C.CYAN}UNDONE:{C.RESET} {usrc.name}")
                        except Exception as ue:
                            print(f"  {C.RED}FAILED:{C.RESET} {ue}")

                elif choice==" ":
                    _display_tracks(p,cfg)

                elif choice=="o":
                    _open_in_finder(rp)

                elif choice in ("R",) or (choice in ("rescan",)):
                    # Rescan folder after manual changes in Finder
                    if not rp.exists():
                        print(f"  {C.RED}Folder no longer exists.{C.RESET}"); break
                    print(f"  {C.CYAN}Rescanning...{C.RESET}")
                    audio_files=list_audio_files(rp,exts,follow_sym)
                    if len(audio_files)<min_tracks:
                        print(f"  {C.YELLOW}Only {len(audio_files)} audio file(s) remain — below minimum ({min_tracks}).{C.RESET}")
                        print(f"  {C.DIM}Skip this folder? [y/N]{C.RESET}")
                        skip_confirm=input(f"  > ").strip().lower()
                        if skip_confirm in ("y","yes"):
                            skipped_count+=1; break
                        continue
                    new_p=build_folder_proposal(rp,audio_files,source_root,profile_obj,cfg)
                    if new_p is None:
                        print(f"  {C.YELLOW}Folder no longer qualifies after rescan.{C.RESET}"); break
                    _route_proposal(new_p,cfg,seen_names,existing_clean,manifest_entries,
                                    review_albums,dup_root,mixes_root)
                    # Replace current proposal
                    p=new_p
                    # Rebuild track file list
                    _stream_track_files=sorted([f for f in rp.iterdir() if f.suffix.lower() in _tf_exts],key=lambda f:f.name) if rp.exists() else []
                    # Re-display
                    _display_folder_card(idx,total,p)
                    _display_tracks(p,cfg)
                    print(f"  {C.CYAN}─ RESCANNED ─{C.RESET}  {len(audio_files)} tracks  ·  conf {conf_color(p.confidence)}  → {p.destination.title()}")

                elif choice=="q":
                    stopped=True
                    out(f"\n{C.YELLOW}Stopped.{C.RESET}"); break

                elif choice=="?":
                    _interactive_action_help()

                else:
                    print(f"  {C.DIM}Unknown key. Press ? for help.{C.RESET}")

    # ══════════════════════════════════════════════════════════════
    # PASS 1: Main review
    # ══════════════════════════════════════════════════════════════
    main_items=[(idx,rp) for idx,rp in enumerate(candidates,1)]
    _run_review_pass(main_items, "Main review")

    # ══════════════════════════════════════════════════════════════
    # Held queue — opt-in, not automatic
    # ══════════════════════════════════════════════════════════════
    if held and not stopped:
        print(f"\n{'─'*66}")
        print(f"  {len(held)} folder(s) held. Review now? [y/n]")
        try:
            review_held=input(f"  ").strip().lower()
        except (EOFError,KeyboardInterrupt):
            review_held="n"
        if review_held in ("y","yes"):
            _run_review_pass(held, "Held review")

    # ══════════════════════════════════════════════════════════════
    # End-of-session summary
    # ══════════════════════════════════════════════════════════════
    if _tag_cache is not None: _tag_cache.save()

    print(f"\n{'━'*66}")
    parts=[]
    if moved_clean or auto_approved:
        clean_total=moved_clean+auto_approved
        parts.append(f"{C.GREEN}{clean_total} → Clean{C.RESET}")
        if auto_approved: parts[-1]+=f" {C.DIM}({auto_approved} auto){C.RESET}"
    if moved_review: parts.append(f"{C.YELLOW}{moved_review} → Review{C.RESET}")
    if skipped_count: parts.append(f"{C.DIM}{skipped_count} skipped{C.RESET}")
    n_held=len([h for h in held if isinstance(h,tuple)])
    if n_held: parts.append(f"{C.YELLOW}{n_held} held{C.RESET}")
    if parts:
        print(f"  {C.BOLD}DONE{C.RESET}  ·  {'  ·  '.join(parts)}")
    else:
        print(f"  {C.DIM}Nothing processed.{C.RESET}")
    if applied:
        print(f"  Undo:  raagdosa undo --session {session_id}")
    print(f"{'━'*66}")
    manifest_set_last_run(cfg)


# ─────────────────────────────────────────────────────────────────
# Triage workflow (v7.1)
# ─────────────────────────────────────────────────────────────────

def _triage_proposals(proposals:List[FolderProposal],auto_threshold:float)->Dict[str,List[FolderProposal]]:
    """
    Split proposals into HIGH / MID / PROB tiers.

    HIGH:  conf >= auto_threshold AND destination == 'clean' AND no review-forcing flags
    PROB:  destination == 'review' OR has FORCE_HOLD flags
    MID:   everything else (dest=clean, conf < auto_threshold, no force-hold)

    Keys 'auto' and 'hold' are kept as aliases for backward compat.
    """
    FORCE_HOLD={"duplicate_in_run","already_in_clean","heuristic_fallback","unreadable_ratio_high"}
    high:List[FolderProposal]=[]; mid:List[FolderProposal]=[]; prob:List[FolderProposal]=[]
    for p in proposals:
        reasons=set(p.decision.get("route_reasons",[]))
        is_force_hold=bool(reasons & FORCE_HOLD)
        if p.confidence>=auto_threshold and p.destination=="clean" and not is_force_hold:
            high.append(p)
        elif p.destination=="review" or is_force_hold:
            prob.append(p)
        else:
            mid.append(p)
    high.sort(key=lambda p:p.confidence,reverse=True)
    mid.sort(key=lambda p:p.confidence,reverse=True)
    prob.sort(key=lambda p:p.confidence)  # worst first
    return {"high":high,"mid":mid,"prob":prob,"auto":high,"hold":mid+prob}


def _show_tier_detail(tier_name:str,proposals:List[FolderProposal])->None:
    """Show all proposals in a tier, paginated at 20."""
    if not proposals:
        print(f"\n  {C.DIM}No folders in {tier_name} tier.{C.RESET}"); return
    PAGE=20; total=len(proposals); offset=0
    color={"HIGH":C.GREEN,"MID":C.YELLOW,"PROB":C.RED}.get(tier_name,C.DIM)
    while True:
        chunk=proposals[offset:offset+PAGE]
        print(f"\n  {color}── {tier_name} tier — {total} folders ──{C.RESET}  (showing {offset+1}–{offset+len(chunk)})")
        for p in chunk:
            reasons=p.decision.get("route_reasons",[])
            reason_str=("  ~"+",".join(reasons[:2])) if reasons else ""
            dest=f"[{p.destination}]"
            name=p.proposed_folder_name[:54]
            print(f"  {color}{p.confidence:.2f}{C.RESET}  {name}  {C.DIM}{dest}{reason_str}{C.RESET}")
        remaining=total-(offset+len(chunk))
        if remaining<=0: print(f"\n  {C.DIM}(end of list){C.RESET}"); break
        try:
            key=input(f"  {C.DIM}n=next {remaining} · Enter=done > {C.RESET}").strip().lower()
        except (EOFError,KeyboardInterrupt): break
        if key=="n": offset+=PAGE
        else: break


def _show_triage_dashboard(triage:Dict[str,List[FolderProposal]],session_id:str,profile:str,auto_threshold:float,dry_run:bool)->None:
    """Print the 3-tier triage summary dashboard."""
    high=triage["high"]; mid=triage["mid"]; prob=triage["prob"]
    total=len(high)+len(mid)+len(prob)

    BAR=32
    def _bar(n:int,tot:int)->str:
        if not tot: return "░"*BAR
        filled=int(n/tot*BAR)
        return "█"*filled+"░"*(BAR-filled)
    def _pct(n:int)->int: return int(n/total*100) if total else 0

    print(f"\n{'═'*66}")
    print(f"  {C.BOLD}RAAGDOSA v{APP_VERSION}  ·  Triage  ·  Session {session_id[:12]}{C.RESET}")
    print(f"{'─'*66}")
    print(f"  Profile: {profile}   Scanned: {total} folders   Auto-approve ≥ {auto_threshold:.2f}")
    if dry_run: print(f"  {C.YELLOW}DRY RUN — nothing will be moved{C.RESET}")
    print(f"{'─'*66}")
    print(f"  {C.GREEN}HIGH{C.RESET}   {len(high):>3}  {_bar(len(high),total)}  {_pct(len(high)):>3}%  conf ≥ {auto_threshold:.2f} → Clean   {C.DIM}[h]{C.RESET}")
    print(f"  {C.YELLOW}MID{C.RESET}    {len(mid):>3}  {_bar(len(mid),total)}  {_pct(len(mid)):>3}%  conf < {auto_threshold:.2f} → Review  {C.DIM}[m]{C.RESET}")
    print(f"  {C.RED}PROB{C.RESET}   {len(prob):>3}  {_bar(len(prob),total)}  {_pct(len(prob)):>3}%  flagged → Review           {C.DIM}[p]{C.RESET}")
    print(f"{'─'*66}")

    # HIGH sample
    if high:
        n=min(6,len(high))
        print(f"\n  {C.GREEN}HIGH — top {n} of {len(high)}{C.RESET}  (sorted by confidence)")
        for p in high[:n]:
            print(f"  {C.GREEN}{p.confidence:.2f}{C.RESET}  {p.proposed_folder_name[:54]}")
        if len(high)>n: print(f"  {C.DIM}… {len(high)-n} more  (h to list all){C.RESET}")

    # MID sample
    if mid:
        n=min(4,len(mid))
        print(f"\n  {C.YELLOW}MID — top {n} of {len(mid)}{C.RESET}  (sorted by confidence)")
        for p in mid[:n]:
            print(f"  {C.YELLOW}{p.confidence:.2f}{C.RESET}  {p.proposed_folder_name[:54]}")
        if len(mid)>n: print(f"  {C.DIM}… {len(mid)-n} more  (m to list all){C.RESET}")

    # PROB sample
    if prob:
        n=min(4,len(prob))
        print(f"\n  {C.RED}PROB — first {n} of {len(prob)}{C.RESET}  (worst first)")
        for p in prob[:n]:
            reasons=p.decision.get("route_reasons",[])
            reason_str=("  ~"+",".join(reasons[:2])) if reasons else ""
            print(f"  {C.RED}{p.confidence:.2f}{C.RESET}  {p.folder_name[:46]}{C.DIM}{reason_str}{C.RESET}")
        if len(prob)>n: print(f"  {C.DIM}… {len(prob)-n} more  (p to list all){C.RESET}")

    print(f"\n{'─'*66}")
    if high:
        print(f"  a:bulk-approve({len(high)})   r:review-all-{total}   q:quit   h/m/p:list-tier   ?:help")
    else:
        print(f"  {C.DIM}(no auto-approvable folders){C.RESET}   r:review-all-{total}   q:quit   h/m/p:list-tier   ?:help")
    print(f"{'═'*66}")


def _prompt_triage_action(triage:Dict[str,List[FolderProposal]])->str:
    """Read the triage action from the user. Returns 'auto', 'review', or 'quit'."""
    high=triage["high"]; total=len(triage["high"])+len(triage["mid"])+len(triage["prob"])
    while True:
        try:
            raw=input(f"  > ").strip().lower()
        except (EOFError,KeyboardInterrupt):
            return "quit"
        if raw=="a" and high:
            return "auto"
        elif raw=="r":
            return "review"
        elif raw in ("q","quit"):
            return "quit"
        elif raw=="h":
            _show_tier_detail("HIGH",triage["high"])
        elif raw=="m":
            _show_tier_detail("MID",triage["mid"])
        elif raw=="p":
            _show_tier_detail("PROB",triage["prob"])
        elif raw=="?":
            print(f"  a  — bulk-approve the {len(high)} HIGH-confidence folders → Clean")
            print(f"  r  — review all {total} folders 1-by-1")
            print(f"  h  — list all HIGH tier folders (conf ≥ threshold, → Clean)")
            print(f"  m  — list all MID tier folders (conf < threshold, → Review)")
            print(f"  p  — list all PROB tier folders (flagged / low confidence)")
            print(f"  q  — quit without moving anything")
        else:
            print(f"  {C.DIM}Unknown. Press ? for help.{C.RESET}")


def _bulk_approve_auto(
    triage:Dict[str,List[FolderProposal]],
    cfg:Dict[str,Any],
    session_id:str,
    source_root:Optional[Path],
    dry_run:bool,
)->List[Dict[str,Any]]:
    """
    Show the bulk-approve confirmation gate and execute AUTO tier moves.
    Requires the user to type YES. Returns list of applied move entries.
    """
    auto=triage["auto"]
    mc=cfg.get("move",{}); policy=mc.get("on_collision","suffix"); sfmt=mc.get("suffix_format"," ({n})"); use_cs=bool(mc.get("use_checksum",False))
    hist_path=Path(cfg["logging"]["history_log"])

    print(f"\n{'═'*66}")
    print(f"  {C.BOLD}BULK APPROVE — confirm{C.RESET}")
    print(f"{'─'*66}")
    print(f"  {len(auto)} folders  →  Clean/")
    print(f"  Move mode: {'dry-run' if dry_run else 'enabled'}")
    print(f"  Undo:  raagdosa undo --session {session_id}")
    print(f"{'─'*66}")
    print(f"  Type {C.BOLD}YES{C.RESET} to confirm, or Enter to cancel: ",end="",flush=True)
    try:
        confirm=input("").strip()
    except (EOFError,KeyboardInterrupt):
        confirm=""

    if confirm!="YES":
        out(f"  {C.DIM}Cancelled. Returning all AUTO folders to manual review.{C.RESET}")
        triage["hold"]=auto+triage["hold"]
        triage["auto"]=[]
        return []

    print(f"{'─'*66}")
    applied:List[Dict[str,Any]]=[]
    skip_path=Path(cfg["logging"]["skipped_log"])

    for p in auto:
        src=Path(p.folder_path); dst=Path(p.target_path)
        if not src.exists():
            print(f"  {C.DIM}SKIP (gone):{C.RESET} {p.folder_name}"); continue
        dst2=collision_resolve(dst,policy,sfmt)
        if dst2 is None:
            warn(f"  Collision skip: {dst.name}")
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"collision_skip","src":str(src)})
            continue
        if dry_run:
            out(f"  {C.DIM}[dry-run]{C.RESET} {src.name[:52]}  →  {dst2.name[:30]}")
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"dry_run","src":str(src),"dst":str(dst2)})
            continue
        ensure_dir(dst2.parent)
        try:
            move_method,_=safe_move_folder(src,dst2,use_checksum=use_cs)
        except RuntimeError as e:
            err(f"  FAILED: {e}  ({src.name})"); continue
        action_id=uuid.uuid4().hex[:10]
        entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,"type":"folder",
               "original_path":str(src),"original_parent":str(src.parent),"original_folder_name":src.name,
               "target_path":str(dst2),"target_parent":str(dst2.parent),"target_folder_name":dst2.name,
               "destination":p.destination,"confidence":p.confidence,"decision":p.decision,
               "move_method":move_method,"interactive_action":"bulk_approved"}
        append_jsonl(hist_path,entry); applied.append(entry)
        manifest_add(cfg,dst2.name,{"original_path":str(src),"confidence":p.confidence,"session_id":session_id})
        print(f"  {C.GREEN}✓{C.RESET} {dst2.name[:60]}")
        if source_root:
            try: cleanup_empty_parents(src,source_root)
            except Exception: pass

    print(f"{'═'*66}")
    return applied


def _run_triage(
    cfg:Dict[str,Any],
    profile:str,
    session_id:str,
    proposals:List[FolderProposal],
    source_root:Path,
    dry_run:bool,
    auto_threshold:Optional[float]=None,
)->List[Dict[str,Any]]:
    """
    v7.1 triage workflow:
      1. Split proposals into AUTO / HOLD
      2. Show triage dashboard
      3. Bulk-approve AUTO tier (requires YES confirmation)
      4. Hand HOLD tier to interactive_review()
    Returns all applied move entries.
    """
    rr=cfg.get("review_rules",{})
    thresh=auto_threshold or float(rr.get("auto_approve_threshold",rr.get("min_confidence_for_clean",0.90)))
    thresh=max(thresh,float(rr.get("min_confidence_for_clean",0.85)))  # never below clean floor

    triage=_triage_proposals(proposals,thresh)
    _show_triage_dashboard(triage,session_id,profile,thresh,dry_run)

    action=_prompt_triage_action(triage)
    applied:List[Dict[str,Any]]=[]

    if action=="quit":
        out(f"\n  {C.DIM}Quit. Nothing moved. Session {session_id[:12]} preserved.{C.RESET}")
        out(f"  Resume: raagdosa resume {session_id}")
        return []

    if action=="auto" and triage["auto"]:
        applied+=_bulk_approve_auto(triage,cfg,session_id,source_root,dry_run)
        # Track renames for auto-approved clean moves
        trc=cfg.get("track_rename",{})
        if trc.get("enabled",True) and trc.get("scope","clean_only") in ("clean_only","both"):
            for a in applied:
                if a.get("destination")=="clean" and not dry_run:
                    try:
                        rename_tracks_in_clean_folder(cfg,Path(a["target_path"]),
                                                      a.get("decision",{}),
                                                      interactive=False,dry_run=False,session_id=session_id)
                    except Exception: pass

    # Interactive review for HOLD tier (or all folders if action=="review")
    review_queue=proposals if action=="review" else triage["hold"]
    if review_queue:
        print(f"\n{'═'*66}")
        tier_label="ALL" if action=="review" else f"HOLD ({len(review_queue)})"
        print(f"  {C.BOLD}Interactive Review — {tier_label}{C.RESET}  ·  Sorted by confidence (lowest first)")
        print(f"{'═'*66}")
        hold_applied=interactive_review(cfg,review_queue,session_id,dry_run=dry_run,source_root=source_root)
        applied+=hold_applied
        # Track renames for individually approved clean moves
        trc=cfg.get("track_rename",{})
        if trc.get("enabled",True) and trc.get("scope","clean_only") in ("clean_only","both"):
            for a in hold_applied:
                if a.get("destination")=="clean" and not dry_run:
                    try:
                        rename_tracks_in_clean_folder(cfg,Path(a["target_path"]),
                                                      a.get("decision",{}),
                                                      interactive=False,dry_run=False,session_id=session_id)
                    except Exception: pass
    return applied


def cmd_go(cfg_path:Path,cfg:Dict[str,Any],profile:str,interactive:bool,dry_run:bool,since:Optional[str],perf_tier:Optional[str]=None,genre_roots:Optional[List[str]]=None,itunes_mode:bool=False,review_threshold:Optional[float]=None,sort_by:str="name",force:bool=False,auto_above:Optional[float]=None,session_name:str="")->None:
    """
    v7.1 default path: scan all → triage dashboard → bulk-approve → interactive review.
    --force: bypass triage, use original streaming pipeline (nuclear option).
    --interactive / -i: bypass triage, review all folders 1-by-1 in streaming mode.
    --auto-above FLOAT: override auto_approve_threshold for this run.
    """
    if force:
        # --force: original _run_core streaming behaviour, no triage
        out(f"\n{C.YELLOW}--force: bypassing triage. Processing all folders without confirmation.{C.RESET}")
        _run_core(cfg_path,cfg,profile,interactive=False,dry_run=dry_run,since_str=since,
                  perf_tier=perf_tier,genre_roots=genre_roots,itunes_mode=itunes_mode)
        return

    if interactive:
        # --interactive: original streaming 1-by-1 mode, no triage
        _interactive_streaming(cfg,profile,dry_run=dry_run,since_str=since,
                               genre_roots=genre_roots,itunes_mode=itunes_mode,
                               threshold=review_threshold,sort_by=sort_by)
        return

    # ── v7.1 Triage workflow ──────────────────────────────────────────────
    for lk in ["history_log","track_history_log"]:
        lp=Path(cfg.get("logging",{}).get(lk,""))
        if lp.name: rotate_log_if_needed(lp,float(cfg.get("logging",{}).get("rotate_log_max_mb",10.0)))
    register_stop_handler()

    # Resolve profile
    profiles=cfg.get("profiles",{}); profile_obj=profiles.get(profile,{})
    source_root=Path(profile_obj.get("source_root","")).expanduser()
    if not source_root.exists():
        err(f"source_root not found: {source_root}"); sys.exit(3)

    setup_logging_paths(cfg,profile_obj,source_root)
    _init_skip_sets(cfg)

    session_id=make_session_id()
    session_dir=Path(cfg["logging"]["session_dir"])/session_id; ensure_dir(session_dir)

    out(f"\n{C.BOLD}Session:{C.RESET}   {session_id}")
    out(f"{C.BOLD}Scanning:{C.RESET}  {source_root}")
    if dry_run: out(f"  {C.YELLOW}DRY RUN — nothing will be moved{C.RESET}")

    _get_tag_cache(cfg)

    # Full scan — builds all proposals before any moves
    since_dt=_parse_since(since,cfg)
    _,_,proposals=scan_folders(cfg,profile,since=since_dt,genre_roots=genre_roots,itunes_mode=itunes_mode,session_name=session_name)

    if not proposals:
        out(f"  {C.DIM}No candidates found.{C.RESET}"); manifest_set_last_run(cfg); return

    applied=_run_triage(cfg,profile,session_id,proposals,source_root,dry_run,auto_threshold=auto_above)

    if _tag_cache is not None: _tag_cache.save()
    manifest_set_last_run(cfg)

    if applied:
        print(f"\n{'━'*66}")
        clean_n=sum(1 for a in applied if a.get("destination")=="clean")
        rev_n  =sum(1 for a in applied if a.get("destination")=="review")
        parts=[]
        if clean_n: parts.append(f"{C.GREEN}{clean_n} → Clean{C.RESET}")
        if rev_n:   parts.append(f"{C.YELLOW}{rev_n} → Review{C.RESET}")
        skip_n=len(proposals)-len(applied)
        if skip_n:  parts.append(f"{C.DIM}{skip_n} skipped{C.RESET}")
        print(f"  {C.BOLD}DONE{C.RESET}  ·  {'  ·  '.join(parts)}")
        print(f"  Undo:  raagdosa undo --session {session_id}")
        print(f"{'━'*66}\n")

def cmd_folders_only(cfg_path:Path,cfg:Dict[str,Any],profile:str,interactive:bool,dry_run:bool,since:Optional[str],genre_roots:Optional[List[str]]=None,itunes_mode:bool=False,sort_by:str="name")->None:
    if interactive:
        # Use streaming interactive — same one-at-a-time approach
        _interactive_streaming(cfg,profile,dry_run=dry_run,since_str=since,
                               genre_roots=genre_roots,itunes_mode=itunes_mode,sort_by=sort_by)
    else:
        register_stop_handler(); sid,_,proposals=scan_folders(cfg,profile,since=_parse_since(since,cfg),genre_roots=genre_roots,itunes_mode=itunes_mode)
        applied=apply_folder_moves(cfg,proposals,interactive=False,auto_above=None,dry_run=dry_run,session_id=sid)
        out(f"Folders applied: {len(applied)}"); manifest_set_last_run(cfg)

def cmd_tracks_only(cfg:Dict[str,Any],profile:str,interactive:bool,dry_run:bool)->None:
    profiles=cfg.get("profiles",{})
    if profile not in profiles: raise ValueError(f"Unknown profile: {profile}")
    prof=profiles[profile]; source_root=Path(prof["source_root"]).expanduser()
    setup_logging_paths(cfg, prof, source_root)
    _init_skip_sets(cfg)
    roots=ensure_roots(prof,source_root); clean_albums=roots["clean_albums"]
    sid=make_session_id(profile); out(f"Session: {sid} (tracks-only)"); register_stop_handler()
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    for folder in sorted([p for p in clean_albums.rglob("*") if p.is_dir()]):
        if should_stop(): break
        af=list_audio_files(folder,exts)
        if not af: continue
        prop=build_folder_proposal(folder,af,source_root,prof,cfg)
        decision=prop.decision if prop else {"dominant_album_share":0.0,"is_va":False}
        rename_tracks_in_clean_folder(cfg,folder,decision,interactive=interactive,dry_run=dry_run,session_id=sid)

def cmd_resume(cfg:Dict[str,Any],session_id:str,interactive:bool,dry_run:bool)->None:
    _resolve_log_paths_from_active_profile(cfg)
    sdir=Path(cfg["logging"]["session_dir"]); pp=sdir/session_id/"proposals.json"
    if not pp.exists(): err(f"No proposals.json for session: {session_id}"); sys.exit(1)
    hist=iter_jsonl(Path(cfg["logging"]["history_log"]))
    already={h.get("original_path") for h in hist if h.get("session_id")==session_id}
    payload=read_json(pp); raw=payload.get("folder_proposals",[])
    remaining=[fp_from_dict(fp) for fp in raw if fp.get("folder_path") not in already]
    out(f"Resume {session_id} | already: {len(already)} | remaining: {len(remaining)}")
    if not remaining: out("Nothing left."); return
    register_stop_handler()
    applied=apply_folder_moves(cfg,remaining,interactive=interactive,auto_above=None,dry_run=dry_run,session_id=session_id)
    trc=cfg.get("track_rename",{})
    if trc.get("enabled",True):
        for a in applied:
            if a.get("destination")=="clean":
                rename_tracks_in_clean_folder(cfg,Path(a["target_path"]),a.get("decision",{}),interactive=interactive,dry_run=dry_run,session_id=session_id)

# ─────────────────────────────────────────────
# History + undo
# ─────────────────────────────────────────────
def _resolve_log_paths_from_active_profile(cfg:Dict[str,Any])->None:
    """
    Resolve logging paths for commands that don't take an explicit profile arg
    (history, undo).  Uses active_profile + source_root to find the wrapper folder.
    Safe no-op if profile/source_root are not available.
    """
    prof_name=cfg.get("active_profile","")
    prof=cfg.get("profiles",{}).get(prof_name,{})
    if not prof: return
    try:
        source_root=Path(prof.get("source_root","")).expanduser()
        if source_root.exists():
            setup_logging_paths(cfg,prof,source_root)
    except Exception:
        pass  # fall back to config values as-is

def _resolve_last_session(hist:List[Dict[str,Any]])->Optional[str]:
    """Return the most recent session_id in a history list."""
    for h in reversed(hist):
        sid=h.get("session_id","")
        if sid: return sid
    return None

def cmd_sessions(cfg:Dict[str,Any],last:int=20)->None:
    """List recent sessions with move counts and timestamps."""
    _resolve_log_paths_from_active_profile(cfg)
    hist_path=Path(cfg["logging"]["history_log"])
    hist=iter_jsonl(hist_path)
    if not hist: out("No session history found."); return
    # Build ordered session summary
    seen:Dict[str,Dict]={}
    order:List[str]=[]
    for h in hist:
        sid=h.get("session_id",""); ts=h.get("timestamp","")
        dest=h.get("destination","")
        orig=h.get("original_folder_name","")
        if sid not in seen:
            seen[sid]={"first_ts":ts,"last_ts":ts,"total":0,"clean":0,"review":0,"examples":[]}
            order.append(sid)
        s=seen[sid]; s["last_ts"]=ts; s["total"]+=1
        if dest=="clean": s["clean"]+=1
        elif dest=="review": s["review"]+=1
        if len(s["examples"])<3 and orig: s["examples"].append(orig)
    recent=order[-last:]
    out(f"\n{C.BOLD}Recent sessions ({len(recent)} of {len(order)} total):{C.RESET}\n")
    for sid in reversed(recent):
        s=seen[sid]
        out(f"  {C.BOLD}{sid}{C.RESET}")
        out(f"    {s['total']} moves  ·  {C.GREEN}{s['clean']} clean{C.RESET}  ·  {C.YELLOW}{s['review']} review{C.RESET}  ·  {s['first_ts'][:16]}")
        if s["examples"]:
            out(f"    e.g. {', '.join(s['examples'][:3])}")
        out("")
    out(f"  To undo a session:  raagdosa undo --session <session_id>")
    out(f"  To undo last:       raagdosa undo --session last")

def cmd_history(cfg:Dict[str,Any],last:int,session:Optional[str],match:Optional[str],tracks:bool)->None:
    _resolve_log_paths_from_active_profile(cfg)
    hist_path=Path(cfg["logging"]["track_history_log"] if tracks else cfg["logging"]["history_log"])
    hist=iter_jsonl(hist_path)
    if session=="last": session=_resolve_last_session(hist)
    if session: hist=[h for h in hist if h.get("session_id")==session]
    if match:   hist=[h for h in hist if match in h.get("original_path","")+" "+h.get("target_path","")]
    hist=hist[-last:] if last and len(hist)>last else hist
    session_counts:Counter=Counter()
    for h in hist:
        session_counts[h.get("session_id","")]+=1
        src=Path(h.get("original_path","")).name; dst=Path(h.get("target_path","")).name
        out(f"  {h.get('action_id')}  {h.get('timestamp')}  {src}  →  {dst}")
    out(f"\nTotal: {len(hist)}")
    for sid,cnt in session_counts.most_common(): out(f"  {sid}: {cnt} action(s)")

def cmd_undo(cfg:Dict[str,Any],action_id:Optional[str],session_id:Optional[str],from_path:Optional[str],tracks:bool,folder:Optional[str])->None:
    """
    Undo folder moves or track renames.

    --folder <name>  works for BOTH folder-level and track-level undo:
      • Without --tracks: undoes all folder moves where the original path
        contains <name> as a path component  (targets a source folder by name).
      • With --tracks:    undoes all track renames inside clean folder <name>.

    Examples:
      raagdosa undo --folder "Burial - Untold" --session last
      raagdosa undo --tracks --folder "Burial - Untold"
      raagdosa undo --session 2026-03-08_14-30_incoming_test
    """
    _resolve_log_paths_from_active_profile(cfg)
    hist_path=Path(cfg["logging"]["track_history_log"] if tracks else cfg["logging"]["history_log"])
    hist=iter_jsonl(hist_path)
    if not hist: err("No history."); return
    selected:List[Dict[str,Any]]=[]
    # Handle -1/-2/... shorthand: resolve to the Nth-most-recent session
    if session_id and re.match(r'^-\d+$',session_id):
        n=abs(int(session_id))  # -1 → 1, -2 → 2
        all_sids=list(dict.fromkeys(h.get("session_id") for h in hist if h.get("session_id")))
        if n<=len(all_sids):
            session_id=all_sids[-n]  # -1 = last, -2 = second-to-last
            out(f"  Undo target: session {session_id}")
        else:
            err(f"Only {len(all_sids)} session(s) in history."); return
        hist=iter_jsonl(hist_path)  # re-read after exhausting iterator
    if session_id=="last": session_id=_resolve_last_session(hist)
    if action_id:    selected=[h for h in hist if h.get("action_id")==action_id]
    elif session_id: selected=[h for h in hist if h.get("session_id")==session_id]
    elif from_path:  selected=[h for h in hist if from_path in h.get("original_path","")]
    elif folder:
        if tracks:
            # Track-level: match by folder field stored in track history
            selected=[h for h in hist if h.get("folder")==folder]
        else:
            # Folder-level: match if folder name appears as a path component
            selected=[h for h in hist if
                      folder==Path(h.get("original_path","")).name or
                      folder in h.get("original_path","")]
    else:
        # Interactive picker — show last session's moves and let user pick
        last_sid=_resolve_last_session(hist)
        if not last_sid: err("No history found."); return
        session_hist=sorted([h for h in hist if h.get("session_id")==last_sid],
                            key=lambda x:x.get("timestamp",""))
        if not session_hist: err("No moves in last session."); return
        out(f"\n{C.BOLD}Last session: {last_sid}{C.RESET}")
        out(f"  {'#':<4} {'Folder':<55} {'Dest'}")
        out(f"  {'─'*70}")
        for i,h in enumerate(session_hist,1):
            name=Path(h.get("original_path","")).name or h.get("action_id","")
            dest=h.get("destination","?")
            dest_col=C.GREEN if dest=="clean" else C.YELLOW
            out(f"  {i:<4} {name:<55} {dest_col}{dest}{C.RESET}")
        out(f"\n  Enter number(s) to undo (e.g. 3  or  1,3,5  or  all), or Enter to cancel:")
        try:
            raw=input("  > ").strip().lower()
        except (EOFError,KeyboardInterrupt):
            raw=""
        if not raw: out("Cancelled."); return
        if raw=="all":
            selected=session_hist
        else:
            idxs=[]
            for tok in re.split(r"[,\s]+",raw):
                try: idxs.append(int(tok)-1)
                except ValueError: pass
            selected=[session_hist[i] for i in idxs if 0<=i<len(session_hist)]
        if not selected: out("Nothing selected."); return
    if not selected: err("No matches."); return
    selected=sorted(selected,key=lambda x:x.get("timestamp",""),reverse=True); undone=0
    for h in selected:
        src=Path(h["target_path"]); dst=Path(h["original_path"])
        if not src.exists(): out(f"  SKIP (missing): {src.name}"); continue
        if dst.exists():
            n=1
            while True:
                cand=dst.with_name(dst.stem+f" (UNDO{n})"+dst.suffix)
                if not cand.exists(): dst=cand; break
                n+=1
        try:
            ensure_dir(dst.parent); src.rename(dst) if tracks else shutil.move(str(src),str(dst))
            undone+=1; out(f"  UNDONE: {h.get('action_id')}  {src.name}  →  {dst.name}")
        except Exception as e: err(f"  FAILED: {h.get('action_id')} — {e}")
    out(f"\nUndo complete. Reverted: {undone}")

# ─────────────────────────────────────────────
# Doctor
# ─────────────────────────────────────────────
def cmd_doctor(cfg_path:Path,cfg:Dict[str,Any])->None:
    is_ok=True
    out(f"\n{C.BOLD}RaagDosa v{APP_VERSION} — Doctor{C.RESET}")
    out(f"Python {sys.version.split()[0]}  |  {platform.system()} {platform.release()}")
    out(f"Config:  {cfg_path}  ({'exists' if cfg_path.exists() else C.RED+'MISSING'+C.RESET})")
    out(f"PyYAML:  {'✓' if yaml else C.RED+'✗ pip install pyyaml'+C.RESET}")
    out(f"Mutagen: {'✓' if MutagenFile else C.RED+'✗ pip install mutagen'+C.RESET}")
    warns=validate_config(cfg)
    if warns:
        out(f"\n{C.YELLOW}Config warnings:{C.RESET}"); [warn(w) for w in warns]; is_ok=False
    else: ok_msg("Config valid")
    prof_name=cfg.get("active_profile")
    if not prof_name or prof_name not in cfg.get("profiles",{}):
        err(f"active_profile '{prof_name}' not found."); sys.exit(1)
    prof=cfg["profiles"][prof_name]; source_root=Path(prof["source_root"]).expanduser()
    out(f"\nProfile: {prof_name}")
    out(f"Source:  {source_root}  ({'exists' if source_root.exists() else C.RED+'MISSING'+C.RESET})")
    lib=cfg.get("library",{}); out(f"Template: {lib.get('template','{artist}/{album}')}  FLAC-seg: {lib.get('flac_segregation',False)}")
    art_norm=cfg.get("artist_normalization",{})
    out(f"The-prefix: {art_norm.get('the_prefix','keep-front')}  Aliases: {len(art_norm.get('aliases',{}) or {})} defined")
    if source_root.exists():
        # v5.5 — resolve logging and skip sets before anything that might use them
        setup_logging_paths(cfg, prof, source_root)
        _init_skip_sets(cfg)
        roots=ensure_roots(prof,source_root)
        wrapper=derive_wrapper_root(prof,source_root)
        out(f"Wrapper: {wrapper}")
        try:
            s=shutil.disk_usage(str(source_root)); fgb=s.free/1024**3
            out(f"Disk:    {fgb:.1f} GB free / {s.total/1024**3:.1f} GB total")
            if fgb<5.0: warn("Less than 5 GB free.")
        except Exception: pass
        dj=cfg.get("dj_safety",{})
        if dj.get("detect_dj_databases",True):
            dbs=find_dj_databases(source_root, cfg)
            if dbs: warn(f"DJ databases: {', '.join(dbs)}"); out(f"  halt_on_dj_databases = {dj.get('halt_on_dj_databases',False)}")
            else: ok_msg("No DJ databases detected.")
        if MutagenFile:
            exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
            cr=str(roots["clean_root"]); test_file:Optional[Path]=None
            for root,_,files in os.walk(source_root):
                if str(Path(root)).startswith(cr): continue
                for f in files:
                    if Path(f).suffix.lower() in exts: test_file=Path(root)/f; break
                if test_file: break
            if test_file:
                try:
                    r=read_audio_tags(test_file,cfg); has=any(v for k,v in r.items() if k not in ("bpm","key") and v)
                    out(f"Mutagen: {'✓ tags readable' if has else C.YELLOW+'⚠ no tags'+C.RESET} ({test_file.name})")
                except Exception as e: err(f"Mutagen test failed: {e}")
    log_root=Path(cfg.get("logging",{}).get("root_dir","logs"))
    out(f"Logs:    {log_root}")
    try:
        ensure_dir(log_root); t=log_root/".write_test"; t.write_text("ok",encoding="utf-8"); t.unlink(); ok_msg(f"Logs writable: {log_root}")
    except Exception as e: err(f"Logs not writable: {log_root} — {e}"); is_ok=False
    # Performance tier
    recommended=detect_recommended_tier()
    configured=cfg.get("performance",{}).get("tier","medium")
    perf=resolve_perf_settings(cfg)
    out(f"\n{C.BOLD}Performance{C.RESET}")
    out(f"  CPU cores:       {os.cpu_count() or '?'}")
    out(f"  Configured tier: {C.CYAN}{configured}{C.RESET}")
    out(f"  Recommended:     {C.GREEN}{recommended}{C.RESET}")
    if recommended!=configured:
        out(f"  {C.DIM}Tip: set performance.tier: {recommended} in config.yaml, or use --performance {recommended}{C.RESET}")
    out(f"  Active:          workers={perf['workers']}  lookahead={perf['lookahead']}  sleep_copy={perf['sleep_copy_ms']}ms (copy-path only)")
    out(f"\n{'✓ Doctor complete.' if is_ok else C.YELLOW+'⚠ Doctor complete — see warnings.'+C.RESET}")

# ─────────────────────────────────────────────
# Profile CRUD
# ─────────────────────────────────────────────
def profile_list(cfg:Dict[str,Any])->None:
    for n in cfg.get("profiles",{}).keys():
        mark=f"  {C.GREEN}* active{C.RESET}" if n==cfg.get("active_profile") else ""
        out(f"  {n}{mark}")

def profile_show(cfg:Dict[str,Any],name:str)->None:
    p=cfg.get("profiles",{}).get(name)
    if not p: err("No such profile."); return
    out(json.dumps(p,indent=2))

def profile_add(cfg_path:Path,cfg:Dict[str,Any],name:str,source:str,clean_mode:str,clean_folder:str,review_folder:str,template:Optional[str]=None)->None:
    cfg.setdefault("profiles",{})
    if name in cfg["profiles"]: raise ValueError("Profile already exists.")
    prof:Dict[str,Any]={"source_root":source,"clean_mode":clean_mode,"clean_folder_name":clean_folder,
                         "review_folder_name":review_folder,"clean_albums_folder_name":"Albums",
                         "clean_tracks_folder_name":"Tracks","review_albums_folder_name":"Albums",
                         "duplicates_folder_name":"Duplicates","orphans_folder_name":"Orphans"}
    if template:
        tpl=BUILTIN_TEMPLATES.get(template)
        if tpl:
            prof["library"]={"template":tpl["template"]}
            out(f"  Template: {template} ({tpl['name']})")
            if tpl.get("requires"):
                out(f"  {C.DIM}Requires tags: {', '.join(tpl['requires'])}{C.RESET}")
        else:
            prof["library"]={"template":template}
            out(f"  Template: {template}")
    cfg["profiles"][name]=prof
    write_yaml(cfg_path,cfg); out(f"Added profile: {name}")

def profile_set(cfg_path:Path,cfg:Dict[str,Any],name:str,source:Optional[str],clean_mode:Optional[str],clean_folder:Optional[str],review_folder:Optional[str],template:Optional[str]=None)->None:
    prof=cfg.get("profiles",{}).get(name)
    if not prof: raise ValueError("No such profile.")
    if source:        prof["source_root"]=source
    if clean_mode:    prof["clean_mode"]=clean_mode
    if clean_folder:  prof["clean_folder_name"]=clean_folder
    if review_folder: prof["review_folder_name"]=review_folder
    if template:
        tpl=BUILTIN_TEMPLATES.get(template)
        if tpl:
            prof.setdefault("library",{})["template"]=tpl["template"]
            out(f"  Template set to: {template} ({tpl['name']})")
            if tpl.get("requires"):
                out(f"  {C.DIM}Requires tags: {', '.join(tpl['requires'])}{C.RESET}")
        else:
            # Assume it's a raw template string like "{genre}/{artist}/{album}"
            prof.setdefault("library",{})["template"]=template
            out(f"  Template set to: {template}")
    write_yaml(cfg_path,cfg); out(f"Updated profile: {name}")

def profile_delete(cfg_path:Path,cfg:Dict[str,Any],name:str)->None:
    if name not in cfg.get("profiles",{}): raise ValueError("No such profile.")
    del cfg["profiles"][name]
    if cfg.get("active_profile")==name: cfg["active_profile"]=None
    write_yaml(cfg_path,cfg); out(f"Deleted: {name}")

def profile_use(cfg_path:Path,cfg:Dict[str,Any],name:str)->None:
    if name not in cfg.get("profiles",{}): raise ValueError("No such profile.")
    cfg["active_profile"]=name; write_yaml(cfg_path,cfg); out(f"Active profile: {name}")

# ─────────────────────────────────────────────
# Template commands
# ─────────────────────────────────────────────
_TEMPLATE_EXAMPLES:Dict[str,List[str]]={
    "standard":["Bicep/","  Isles (2021)/","  Isles Deluxe (2022)/","Floating Points/","  Crush (2019)/"],
    "dated":["Aphex Twin/","  1994 - Selected Ambient Works Vol II/","  2001 - Drukqs/","Burial/","  2007 - Untrue/"],
    "flat":["Bicep - Isles/","Burial - Untrue/","Four Tet - There Is Love In You/"],
    "bpm":["120-124 BPM/","  Bicep - Isles/","125-129 BPM/","  Amelie Lens - Higher/","_Unsorted/","  Unknown - Untitled/"],
    "genre-bpm":["House/","  120-124/","    Bicep - Isles/","Techno/","  130-134/","    Charlotte de Witte - Doppler/"],
    "genre-bpm-key":["Melodic Techno/","  125-129/","    8A/","      Tale Of Us - Afterlife 001/"],
    "genre":["Electronic/","  Bicep/","    Isles (2021)/","Jazz/","  Kamasi Washington/","    The Epic (2015)/","_Unsorted/","  Unknown Artist/","    Untitled/"],
    "label":["Warp Records/","  Aphex Twin - Selected Ambient Works/","4AD/","  Cocteau Twins - Heaven or Las Vegas/","_Unsorted/","  Unknown - Demo/"],
    "decade":["1990s/","  Drum & Bass/","    Goldie - Timeless/","2020s/","  Electronic/","    Bicep - Isles/"],
}

def cmd_template_list(cfg:Dict[str,Any])->None:
    """List all builtin templates and show which one is active per profile."""
    active_profile=cfg.get("active_profile","")
    active_tpl=""
    if active_profile:
        prof=cfg.get("profiles",{}).get(active_profile,{})
        lib=_resolve_lib_cfg(prof,cfg)
        active_tpl=lib.get("template","{artist}/{album}")

    out(f"\n{C.BOLD}Library Templates{C.RESET}")
    out(f"{'  ID':<18}{'Name':<22}{'Pattern':<45}{'Tags'}")
    out(f"  {'─'*16}  {'─'*20}  {'─'*43}  {'─'*15}")
    for tid,t in BUILTIN_TEMPLATES.items():
        marker=""
        if t["template"]==active_tpl:
            marker=f" {C.GREEN}← active{C.RESET}"
        reqs=", ".join(t.get("requires",[])) or "—"
        out(f"  {tid:<16}  {t['name']:<20}  {t['template']:<43}  {reqs}{marker}")
    out(f"\n  {C.DIM}Set on a profile: raagdosa profile set <name> --template <id>{C.RESET}")
    out(f"  {C.DIM}Or in config.yaml under profiles.<name>.library.template{C.RESET}\n")

def cmd_template_show(cfg:Dict[str,Any],name:str)->None:
    """Show details and example tree for a template."""
    t=BUILTIN_TEMPLATES.get(name)
    if not t:
        # Check if it matches an active profile's custom template
        err(f"Unknown template: {name}")
        out(f"  Available: {', '.join(BUILTIN_TEMPLATES.keys())}")
        return
    out(f"\n{C.BOLD}Template: {name}{C.RESET} — {t['name']}")
    out(f"Pattern:  {t['template']}")
    out(f"{t['description']}")
    if t.get("note"):
        out(f"{C.YELLOW}Note:{C.RESET} {t['note']}")
    reqs=t.get("requires",[])
    if reqs:
        out(f"\n{C.BOLD}Required tags:{C.RESET} {', '.join(reqs)}")
        out(f"  {C.DIM}Albums missing these tags will go to the fallback folder (_Unsorted).{C.RESET}")
    else:
        out(f"\n{C.BOLD}Required tags:{C.RESET} none (works with any metadata)")
    examples=_TEMPLATE_EXAMPLES.get(name,[])
    if examples:
        out(f"\n{C.BOLD}Example tree:{C.RESET}  Clean/Albums/")
        for line in examples:
            out(f"  {C.CYAN}{line}{C.RESET}")
    out("")


# ═══════════════════════════════════════════════════════════════════
# v4.1 — Genre root management
# ═══════════════════════════════════════════════════════════════════

def _resolve_genre_roots(cfg: Dict[str, Any], cli_roots: Optional[List[str]] = None) -> Set[str]:
    """Return effective set of genre root folder names (CLI override + config persistent list)."""
    roots: Set[str] = set()
    # Config persistent list
    for item in cfg.get("genre_roots", []) or []:
        if isinstance(item, str): roots.add(item)
        elif isinstance(item, dict) and item.get("name"): roots.add(item["name"])
    # CLI flag overrides (session-only, not written to config)
    if cli_roots:
        for r in cli_roots: roots.add(r.strip())
    return roots

def cmd_genre(cfg_path: Path, cfg: Dict[str, Any], action: str, name: Optional[str] = None) -> None:
    """Manage persistent genre root declarations."""
    roots_key = "genre_roots"

    if action == "list":
        roots = cfg.get(roots_key, []) or []
        if not roots:
            out(f"{C.DIM}No genre roots declared. Use: raagdosa genre add <FolderName>{C.RESET}")
            return
        out(f"{C.CYAN}Persistent genre roots:{C.RESET}")
        for r in sorted(str(x) for x in roots):
            out(f"  • {r}")
        return

    if action == "clear":
        cfg[roots_key] = []
        write_yaml(cfg_path, cfg)
        ok_msg("Cleared all genre roots.")
        return

    if action in ("add", "remove", "show"):
        if not name:
            err(f"'genre {action}' requires a folder name argument.")
            sys.exit(1)
        current: List[str] = [str(x) for x in (cfg.get(roots_key, []) or [])]

        if action == "show":
            if name in current:
                out(f"{C.GREEN}'{name}' IS a declared genre root.{C.RESET}")
            else:
                out(f"{C.DIM}'{name}' is NOT a declared genre root.{C.RESET}")
            return

        if action == "add":
            if name in current:
                warn(f"'{name}' is already a genre root.")
            else:
                current.append(name)
                cfg[roots_key] = current
                write_yaml(cfg_path, cfg)
                ok_msg(f"Added genre root: {name}")
            return

        if action == "remove":
            if name not in current:
                warn(f"'{name}' not found in genre roots.")
            else:
                current.remove(name)
                cfg[roots_key] = current
                write_yaml(cfg_path, cfg)
                ok_msg(f"Removed genre root: {name}")
            return

    err(f"Unknown genre action: {action}")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════════
# v4.1 — iTunes hierarchy flattening
# ═══════════════════════════════════════════════════════════════════

def _is_itunes_genre_bucket(name: str) -> bool:
    return name in _ITUNES_GENRE_BUCKETS


# ═══════════════════════════════════════════════════════════════════
# v4.1 — tree command
# ═══════════════════════════════════════════════════════════════════

_AUDIO_EXTS_TREE = {".mp3",".flac",".m4a",".aiff",".wav",".ogg",".opus",".wma",".aac",".alac",".ape"}
_NON_AUDIO_EXTS_TREE = {".jpg",".jpeg",".png",".gif",".nfo",".txt",".sfk",".m3u",".m3u8",".cue",".pdf",".log",".url"}

def cmd_tree(
    cfg: Dict[str, Any],
    path_str: str,
    audio_only: bool = False,
    depth: Optional[int] = None,
    list_mode: bool = False,
    diff_a: Optional[str] = None,
    diff_b: Optional[str] = None,
) -> None:
    """
    `raagdosa tree <path>`
    Each run creates its own subfolder inside logs/trees/:
      logs/trees/<FolderName>_YYYY-MM-DD_HH-MM/
        <FolderName>_YYYY-MM-DD_HH-MM.txt
    This keeps every snapshot self-contained and easy to locate by name + date.
    """
    trees_dir = Path(cfg.get("logging", {}).get("root_dir", "logs")) / "trees"
    ensure_dir(trees_dir)

    if list_mode:
        # Show subdirectories (one per snapshot run)
        subdirs = sorted(
            [d for d in trees_dir.iterdir() if d.is_dir()],
            key=lambda d: d.stat().st_mtime, reverse=True
        )
        # Also pick up legacy flat .txt files
        flat_txts = [f for f in trees_dir.glob("*.txt")]
        if not subdirs and not flat_txts:
            out(f"{C.DIM}No tree snapshots yet. Run: raagdosa tree <path>{C.RESET}")
            return
        out(f"{C.CYAN}Saved tree snapshots ({len(subdirs) + len(flat_txts)}):{C.RESET}")
        for sd in subdirs:
            try:
                txt = next(sd.glob("*.txt"))
                lines = txt.read_text(encoding="utf-8").splitlines()
                file_count = sum(1 for l in lines if not l.startswith("#") and l.strip() and not l.endswith("/"))
                created = dt.datetime.fromtimestamp(sd.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
                out(f"  {sd.name:<55}  {created}  {file_count:>6} files  → {txt.name}")
            except StopIteration:
                out(f"  {sd.name}  (empty)")
        for f in sorted(flat_txts, reverse=True):
            created = dt.datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
            out(f"  {f.name:<55}  {created}  (legacy flat)")
        return

    if diff_a and diff_b:
        _cmd_tree_diff(trees_dir, diff_a, diff_b)
        return

    if not path_str:
        err("Provide a path: raagdosa tree <path>")
        sys.exit(1)

    scan_path = Path(path_str).expanduser().resolve()
    if not scan_path.exists():
        err(f"Path not found: {scan_path}")
        sys.exit(1)

    # Build tree lines
    lines: List[str] = []
    _tree_walk(scan_path, scan_path, lines, audio_only=audio_only, max_depth=depth, current_depth=0)

    # Build snapshot folder name: FolderName_YYYY-MM-DD_HH-MM
    ts = dt.datetime.now().strftime("%Y-%m-%d_%H-%M")
    base_name = f"{scan_path.name}_{ts}"
    snap_dir = trees_dir / base_name
    # Handle same-minute duplicate
    if snap_dir.exists():
        for i in range(2, 100):
            candidate = trees_dir / f"{base_name}_{i}"
            if not candidate.exists():
                snap_dir = candidate
                base_name = snap_dir.name
                break
    ensure_dir(snap_dir)

    out_path = snap_dir / f"{base_name}.txt"
    header = [
        f"# RaagDosa tree snapshot",
        f"# Path:    {scan_path}",
        f"# Date:    {dt.datetime.now().isoformat(timespec='seconds')}",
        f"# Options: audio_only={audio_only} depth={depth}",
        f"# Files:   {sum(1 for l in lines if not l.startswith('#') and l.strip() and not l.endswith('/'))}",
        "",
    ]
    out_path.write_text("\n".join(header + lines), encoding="utf-8")
    file_count = sum(1 for l in lines if not l.endswith("/") and l.strip())
    folder_count = sum(1 for l in lines if l.endswith("/"))
    ok_msg(f"Tree saved → {snap_dir.name}/")
    out(f"  {file_count:,} files  |  {folder_count:,} folders  |  {out_path}")

def _tree_walk(
    root: Path,
    base: Path,
    lines: List[str],
    audio_only: bool,
    max_depth: Optional[int],
    current_depth: int,
) -> None:
    if max_depth is not None and current_depth > max_depth:
        return
    try:
        entries = sorted(root.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
    except PermissionError:
        return
    for entry in entries:
        rel = str(entry.relative_to(base))
        if entry.is_dir():
            if entry.name in _SKIP_FOLDER_NAMES: continue
            lines.append(rel + "/")
            _tree_walk(entry, base, lines, audio_only, max_depth, current_depth + 1)
        elif entry.is_file():
            if entry.name.startswith("._"): continue
            if entry.suffix.lower() in _SKIP_AUDIO_EXTENSIONS: continue
            if audio_only and entry.suffix.lower() not in _AUDIO_EXTS_TREE: continue
            lines.append(rel)

def _cmd_tree_diff(trees_dir: Path, name_a: str, name_b: str) -> None:
    """Show what appeared and disappeared between two tree snapshots."""
    def _find_snap(name: str) -> Optional[Path]:
        # Try as a subdirectory first (v4.2 format)
        sd = trees_dir / name
        if sd.is_dir():
            txts = sorted(sd.glob("*.txt"))
            if txts: return txts[0]
        # Try exact .txt (legacy flat)
        exact = trees_dir / name
        if exact.exists(): return exact
        exact_txt = trees_dir / f"{name}.txt"
        if exact_txt.exists(): return exact_txt
        # Search subdirs by partial name
        for d in sorted(trees_dir.iterdir()):
            if d.is_dir() and name.lower() in d.name.lower():
                txts = sorted(d.glob("*.txt"))
                if txts: return txts[0]
        # Glob flat legacy
        matches = sorted(trees_dir.glob(f"*{name}*.txt"))
        return matches[0] if matches else None

    pa = _find_snap(name_a); pb = _find_snap(name_b)
    if not pa: err(f"Snapshot not found: {name_a}"); sys.exit(1)
    if not pb: err(f"Snapshot not found: {name_b}"); sys.exit(1)

    lines_a = set(pa.read_text(encoding="utf-8").splitlines()) - set()
    lines_b = set(pb.read_text(encoding="utf-8").splitlines())
    # Strip header lines
    lines_a = {l for l in lines_a if not l.startswith("#") and l.strip()}
    lines_b = {l for l in lines_b if not l.startswith("#") and l.strip()}

    added = sorted(lines_b - lines_a)
    removed = sorted(lines_a - lines_b)

    out(f"\n{C.CYAN}Tree diff:{C.RESET} {pa.name} → {pb.name}")
    out(f"  {C.GREEN}+{len(added)} added{C.RESET}   {C.RED}-{len(removed)} removed{C.RESET}\n")
    for l in removed: out(f"{C.RED}− {l}{C.RESET}")
    for l in added:   out(f"{C.GREEN}+ {l}{C.RESET}")


# ═══════════════════════════════════════════════════════════════════
# v4.1 — catchall command
# ═══════════════════════════════════════════════════════════════════

_CATCHALL_FOLDER_NAMES = {
    "_singles","_unsorted","_inbox","_dump","sort","still sort","unzip",
    "staging","tempo","new music","macbook clean","dupes from tuneup",
    "chroma download",
}

def _extract_catchall_artist(path: Path, cfg: Dict[str, Any]) -> str:
    """
    Extract artist from a loose audio file for catchall grouping.
    Priority: albumartist tag → artist tag → filename parse → parent folder name.
    """
    if MutagenFile is not None:
        try:
            mf = MutagenFile(str(path), easy=True)
            if mf and mf.tags:
                t = mf.tags
                for key in ["albumartist", "artist"]:
                    v = t.get(key)
                    if isinstance(v, list): v = v[0] if v else None
                    if v: return str(v).strip()
        except Exception as e:
            out(f"  {C.DIM}Tag read failed: {e}{C.RESET}",level=VERBOSE)
    # Filename parse: "Artist - Title.mp3"
    stem = path.stem.strip()
    m = re.match(r"^(.+?)\s*[-–]\s*(.+)$", stem)
    if m:
        candidate = m.group(1).strip()
        if len(candidate) > 1 and not candidate.isdigit():
            return smart_title_case(candidate)
    # Fall back to parent folder
    return path.parent.name or "_Unknown"

def cmd_catchall(
    cfg: Dict[str, Any],
    path_str: str,
    profile_name: str,
    dry_run: bool = False,
    genre_roots: Optional[List[str]] = None,
) -> None:
    """
    Process a flat dump folder: group loose files by artist into subfolders.
    Artists with ≥ group_threshold tracks get their own folder.
    Artists with fewer tracks go into _Singles/.
    Cross-references with Clean/ to propose merges for known artists.
    """
    cc = cfg.get("catchall", {})
    group_threshold = int(cc.get("group_threshold", 3))
    create_singles = bool(cc.get("create_singles_bucket", True))
    cross_ref = bool(cc.get("cross_reference_clean", True))

    # Auto-recognise catchall folder names
    extra_names = [n.lower() for n in cc.get("folder_names", []) or []]
    catchall_names = _CATCHALL_FOLDER_NAMES | set(extra_names)

    scan_path = Path(path_str).expanduser().resolve()
    if not scan_path.exists():
        err(f"Path not found: {scan_path}")
        sys.exit(1)

    is_known_catchall = scan_path.name.lower() in catchall_names
    out(f"\n{C.CYAN}Catchall:{C.RESET} {scan_path}" +
        (f"  {C.DIM}(auto-recognised){C.RESET}" if is_known_catchall else ""))

    exts = {e.lower() for e in cfg.get("scan", {}).get("audio_extensions", [".mp3", ".flac", ".m4a"])}

    # Collect loose audio files (non-recursive — catchall is a flat dump)
    audio_files: List[Path] = []
    for f in scan_path.iterdir():
        if f.is_file() and f.suffix.lower() in exts and not f.name.startswith("._"):
            audio_files.append(f)

    if not audio_files:
        warn(f"No audio files found in {scan_path}")
        return

    out(f"  Found {len(audio_files)} audio files")

    # Group by artist
    by_artist: Dict[str, List[Path]] = {}
    for af in audio_files:
        artist = _extract_catchall_artist(af, cfg)
        by_artist.setdefault(artist, []).append(af)

    # Classify
    group_artists = {a: fs for a, fs in by_artist.items() if len(fs) >= group_threshold}
    singles_files = [f for a, fs in by_artist.items() if len(fs) < group_threshold for f in fs]

    # Cross-reference with Clean/ for merge proposals
    clean_root: Optional[Path] = None
    if cross_ref and profile_name in cfg.get("profiles", {}):
        prof = cfg["profiles"][profile_name]
        source_root = Path(prof["source_root"]).expanduser()
        clean_root = derive_clean_root(prof, source_root)

    proposals: List[Dict[str, Any]] = []

    # Build proposals for grouped artists
    for artist, files in sorted(group_artists.items()):
        dst = scan_path / sanitize_name(artist)
        # Check if artist already exists in Clean/
        merge_target: Optional[Path] = None
        if clean_root and clean_root.exists():
            for existing in clean_root.rglob("*"):
                if existing.is_dir() and artists_are_same(existing.name, artist, cfg):
                    merge_target = existing
                    break
        props = {
            "type": "catchall_group",
            "artist": artist,
            "files": [str(f) for f in files],
            "count": len(files),
            "destination": str(dst),
            "merge_target": str(merge_target) if merge_target else None,
        }
        proposals.append(props)
        if merge_target:
            out(f"  {C.YELLOW}[MERGE?]{C.RESET}  {artist} ({len(files)} tracks) → {C.DIM}{merge_target}{C.RESET}")
        else:
            out(f"  {C.GREEN}[GROUP ]{C.RESET}  {artist} ({len(files)} tracks) → {dst.name}/")

    # Singles bucket
    if singles_files:
        singles_dst = scan_path / "_Singles"
        out(f"  {C.DIM}[SINGLE ]{C.RESET}  {len(singles_files)} tracks → _Singles/")
        proposals.append({
            "type": "catchall_singles",
            "files": [str(f) for f in singles_files],
            "count": len(singles_files),
            "destination": str(singles_dst),
        })

    # Album detection: tracks sharing an album tag → propose album folder
    album_groups: Dict[str, List[Path]] = {}
    for af in audio_files:
        if MutagenFile:
            try:
                mf = MutagenFile(str(af), easy=True)
                if mf and mf.tags:
                    alb = mf.tags.get("album")
                    if isinstance(alb, list): alb = alb[0] if alb else None
                    if alb:
                        album_groups.setdefault(str(alb).strip(), []).append(af)
            except Exception as e:
                out(f"  {C.DIM}Tag read failed: {e}{C.RESET}",level=VERBOSE)
    for alb_name, alb_files in album_groups.items():
        if len(alb_files) >= 2:
            out(f"  {C.BLUE}[ALBUM?]{C.RESET}  '{alb_name}' — {len(alb_files)} tracks share this album tag")

    if dry_run:
        out(f"\n  {C.DIM}--dry-run: no changes made. {len(proposals)} proposals generated.{C.RESET}")
        return

    # Apply moves
    applied = 0
    for prop in proposals:
        dst_path = Path(prop["destination"])
        for f_str in prop["files"]:
            f = Path(f_str)
            if not f.exists(): continue
            try:
                ensure_dir(dst_path)
                tgt = dst_path / f.name
                if tgt.exists():
                    tgt = dst_path / (f.stem + "_" + uuid.uuid4().hex[:4] + f.suffix)
                f.rename(tgt)
                applied += 1
            except Exception as e:
                warn(f"Could not move {f.name}: {e}")

    # Save proposals JSON
    proposals_out = scan_path / "catchall_proposals.json"
    write_json(proposals_out, {
        "raagdosa_version": APP_VERSION,
        "generated_at": now_iso(),
        "source_folder": str(scan_path),
        "proposals": proposals,
    })
    ok_msg(f"Applied {applied} moves  |  proposals saved: {proposals_out.name}")


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────
def build_parser()->argparse.ArgumentParser:
    p=argparse.ArgumentParser(prog="raagdosa",description=f"RaagDosa v{APP_VERSION} — deterministic music library cleanup.",
        formatter_class=argparse.RawDescriptionHelpFormatter,epilog="""
Examples:
  raagdosa init                                 # guided first-time setup
  raagdosa doctor                               # verify config, deps, disk
  raagdosa go --dry-run                         # preview — nothing moves
  raagdosa go                                   # scan + move + rename tracks
  raagdosa go --genre-roots "B,House,Techno"   # protect genre root folders
  raagdosa go --itunes                          # strip iTunes Genre/ layer first
  raagdosa tree /Volumes/music/Incoming        # snapshot the directory tree
  raagdosa tree --list                          # show all saved snapshots
  raagdosa tree --diff snap_a snap_b            # what changed between snapshots
  raagdosa catchall /path/to/_Dump             # group loose files by artist
  raagdosa genre add "Bass"                    # declare a persistent genre root
  raagdosa genre list                          # show all genre roots
  raagdosa sessions                            # review past sessions
  raagdosa undo --session last                 # undo a whole session
  raagdosa orphans                             # find loose audio files
  raagdosa artists --list                      # list all artists in Clean
  raagdosa review-list --older-than 30         # Review folders >30 days old
  raagdosa review-promote "Album Name"         # force VA→album re-evaluation
  raagdosa review-promote "Album" --artist "X" # force artist + re-evaluate
  raagdosa clean-report                        # stats on your Clean library
""")
    p.add_argument("--config",default="config.yaml"); p.add_argument("--version",action="version",version=f"raagdosa {APP_VERSION}")
    p.add_argument("--verbose",action="store_true"); p.add_argument("--quiet",action="store_true")
    sub=p.add_subparsers(dest="cmd",required=True)
    sub.add_parser("init",help="Guided first-time setup")
    sp=sub.add_parser("profile",help="Manage profiles"); psub=sp.add_subparsers(dest="profile_cmd",required=True)
    psub.add_parser("list"); psub.add_parser("show").add_argument("name")
    add=psub.add_parser("add"); add.add_argument("name"); add.add_argument("--source",required=True)
    add.add_argument("--clean-mode",default="inside_root",choices=["inside_root","inside_parent"])
    add.add_argument("--clean-folder",default="Clean"); add.add_argument("--review-folder",default="Review")
    add.add_argument("--template",metavar="TEMPLATE",help="Builtin template ID or custom pattern (e.g. 'genre' or '{genre}/{artist}/{album}')")
    setp=psub.add_parser("set"); setp.add_argument("name"); setp.add_argument("--source"); setp.add_argument("--clean-mode"); setp.add_argument("--clean-folder"); setp.add_argument("--review-folder")
    setp.add_argument("--template",metavar="TEMPLATE",help="Builtin template ID or custom pattern")
    psub.add_parser("delete").add_argument("name"); psub.add_parser("use").add_argument("name")
    # Template commands
    tp=sub.add_parser("template",help="Library organisation templates"); tsub=tp.add_subparsers(dest="template_cmd",required=True)
    tsub.add_parser("list",help="List all builtin templates"); tsub.add_parser("show",help="Show template details and example tree").add_argument("name")
    sc=sub.add_parser("scan",help="Scan → proposals.json"); sc.add_argument("--profile"); sc.add_argument("--out"); sc.add_argument("--since")
    sc.add_argument("--genre-roots",metavar="ROOTS",help="Comma-separated genre root folder names (session-only)")
    sc.add_argument("--itunes",action="store_true",help="Strip iTunes Genre/ layer before scanning")
    sc.add_argument("--session-name",metavar="NAME",help="Human-friendly session name (e.g. 'Bandcamp Friday')")
    ap=sub.add_parser("apply",help="Apply proposals.json"); ap.add_argument("proposals",nargs="?"); ap.add_argument("--last-session",action="store_true"); ap.add_argument("--interactive",action="store_true"); ap.add_argument("--auto-above",type=float); ap.add_argument("--dry-run",action="store_true")
    for nc in ("run","go"):
        c=sub.add_parser(nc,help="Scan + apply (folders + tracks)")
        c.add_argument("--profile"); c.add_argument("--interactive",action="store_true")
        c.add_argument("--dry-run",action="store_true"); c.add_argument("--since")
        c.add_argument("--performance",choices=["slow","medium","fast","ultra"],default=None,metavar="TIER",help="Hardware tier: slow|medium|fast|ultra")
        c.add_argument("--genre-roots",metavar="ROOTS",help="Comma-separated genre root folder names (session-only)")
        c.add_argument("--itunes",action="store_true",help="Strip iTunes Genre/ layer before scanning")
        c.add_argument("--threshold",type=float,metavar="SCORE",help="Interactive: only review folders below this confidence score")
        c.add_argument("--sort",choices=["name","date-created","date-modified"],default="name",help="Interactive: folder sort order (default: name)")
        c.add_argument("--force",action="store_true",help="Nuclear option: bypass triage, process all folders without confirmation (original streaming behaviour)")
        c.add_argument("--auto-above",type=float,metavar="SCORE",dest="auto_above",help="Override auto-approve threshold for triage (default: review_rules.auto_approve_threshold)")
        c.add_argument("--session-name",metavar="NAME",help="Human-friendly session name (e.g. 'Bandcamp Friday')")
    fo=sub.add_parser("folders",help="Folder pass only"); fo.add_argument("--profile"); fo.add_argument("--interactive",action="store_true"); fo.add_argument("--dry-run",action="store_true"); fo.add_argument("--since")
    fo.add_argument("--genre-roots",metavar="ROOTS"); fo.add_argument("--itunes",action="store_true")
    fo.add_argument("--sort",choices=["name","date-created","date-modified"],default="name",help="Interactive: folder sort order")
    tr=sub.add_parser("tracks",help="Track rename pass"); tr.add_argument("--profile"); tr.add_argument("--interactive",action="store_true"); tr.add_argument("--dry-run",action="store_true")
    sub.add_parser("status",help="Library overview").add_argument("--profile")
    rs=sub.add_parser("resume",help="Resume interrupted session"); rs.add_argument("session_id"); rs.add_argument("--interactive",action="store_true"); rs.add_argument("--dry-run",action="store_true")
    sh=sub.add_parser("show",help="Debug a single folder"); sh.add_argument("folder"); sh.add_argument("--profile"); sh.add_argument("--tracks",action="store_true",help="Also show per-track rename preview")
    sub.add_parser("verify",help="Audit Clean library health").add_argument("--profile")
    le=sub.add_parser("learn",help="Suggest config improvements"); le.add_argument("--session")
    rp=sub.add_parser("report",help="View session report"); rp.add_argument("--session"); rp.add_argument("--format",default="txt",choices=["txt","csv","html"])
    sub.add_parser("doctor",help="Check config, deps, disk, DJ databases")
    hi=sub.add_parser("history",help="Show history"); hi.add_argument("--last",type=int,default=50); hi.add_argument("--session"); hi.add_argument("--match"); hi.add_argument("--tracks",action="store_true")
    un=sub.add_parser("undo",help="Undo moves or renames"); un.add_argument("--id"); un.add_argument("--session"); un.add_argument("--last",action="store_true",help="Undo last session (shortcut for --session last)"); un.add_argument("--from-path"); un.add_argument("--tracks",action="store_true"); un.add_argument("--folder")
    se=sub.add_parser("sessions",help="List recent sessions with move counts"); se.add_argument("--last",type=int,default=20)

    # dump-tree command (legacy)
    dt_p = sub.add_parser("dump-tree", help="Export raw folder/file tree to a text file")
    dt_p.add_argument("--profile")
    dt_p.add_argument("--out", required=True, help="Output text file path")
    dt_p.add_argument("--include-clean", action="store_true")
    dt_p.add_argument("--include-review", action="store_true")
    dt_p.add_argument("--include-logs", action="store_true")
    dt_p.add_argument("--folders-only", action="store_true")
    dt_p.add_argument("--files-only", action="store_true")

    # v4.1 — tree command
    tr_p = sub.add_parser("tree", help="Snapshot a directory tree to logs/trees/")
    tr_p.add_argument("path", nargs="?", help="Path to scan (omit with --list or --diff)")
    tr_p.add_argument("--audio-only", action="store_true", help="Strip non-audio files from output")
    tr_p.add_argument("--depth", type=int, default=None, metavar="N", help="Limit recursion depth")
    tr_p.add_argument("--list", dest="list_mode", action="store_true", help="Show all saved snapshots")
    tr_p.add_argument("--diff", nargs=2, metavar=("A", "B"), help="Diff two snapshots")

    # v4.1 — catchall command
    ca_p = sub.add_parser("catchall", help="Group loose files in a dump folder by artist")
    ca_p.add_argument("path", help="Path to the flat dump folder")
    ca_p.add_argument("--profile")
    ca_p.add_argument("--dry-run", action="store_true")
    ca_p.add_argument("--genre-roots", metavar="ROOTS")

    # v4.1 — genre command
    gn_p = sub.add_parser("genre", help="Manage persistent genre root declarations")
    gn_sub = gn_p.add_subparsers(dest="genre_cmd", required=True)
    gn_sub.add_parser("list", help="List all declared genre roots")
    gn_add = gn_sub.add_parser("add", help="Declare a folder as a genre root")
    gn_add.add_argument("name", help="Folder name to protect")
    gn_rem = gn_sub.add_parser("remove", help="Remove a genre root declaration")
    gn_rem.add_argument("name")
    gn_sub.add_parser("clear", help="Remove all genre root declarations")
    gn_show = gn_sub.add_parser("show", help="Check if a folder is a declared genre root")
    gn_show.add_argument("name")

    # v3.5 commands
    ar=sub.add_parser("artists",help="List or find artists in Clean library"); ar.add_argument("--profile")
    ar.add_argument("--list",dest="list_mode",action="store_true",help="List all artists")
    ar.add_argument("--find",dest="find_query",metavar="QUERY",help="Fuzzy-find an artist")
    rl=sub.add_parser("review-list",help="Summarise Review folder contents"); rl.add_argument("--profile")
    rl.add_argument("--older-than",type=int,metavar="DAYS",help="Only show folders older than N days")
    rp=sub.add_parser("review-promote",help="Re-evaluate a Review folder as album (not VA)")
    rp.add_argument("folder",help="Folder name or path in Review/"); rp.add_argument("--profile")
    rp.add_argument("--artist",metavar="NAME",help="Force artist name for re-evaluation")
    rp.add_argument("--dry-run",action="store_true",help="Preview only — don't move")
    sub.add_parser("clean-report",help="Stats and health report for Clean library").add_argument("--profile")
    ex=sub.add_parser("extract",help="Extract tracks from a VA/mix folder"); ex.add_argument("folder"); ex.add_argument("--profile")
    ex.add_argument("--by-artist",action="store_true",required=True,help="Group tracks by artist tag")
    ex.add_argument("--dry-run",action="store_true")
    cp=sub.add_parser("compare",help="Compare two folders"); cp.add_argument("--folder",nargs=2,metavar="FOLDER",required=True)
    df=sub.add_parser("diff",help="Diff two session reports"); df.add_argument("session_a",metavar="SESSION_A"); df.add_argument("session_b",metavar="SESSION_B")
    ca=sub.add_parser("cache",help="Manage the tag cache (status/clear/evict)")
    ca.add_argument("action",nargs="?",default="status",choices=["status","clear","evict"])
    sub.add_parser("orphans",help="Find loose audio files in Clean/Review").add_argument("--profile")
    # v7.0 — reference (musical reference) commands
    ref_p=sub.add_parser("reference",help="Manage the musical reference (aliases, labels, patterns)")
    ref_sub=ref_p.add_subparsers(dest="ref_cmd",required=True)
    ref_sub.add_parser("list",help="Show reference contents summary")
    ref_imp=ref_sub.add_parser("import",help="Import a reference file")
    ref_imp.add_argument("file",help="Path to reference YAML file to import")
    ref_exp=ref_sub.add_parser("export",help="Export reference to shareable file")
    ref_exp.add_argument("--section",help="Export only this section (e.g. artist_aliases)")
    ref_exp.add_argument("--out",metavar="FILE",help="Output file path (default: reference_export.yaml)")
    return p

def _parse_genre_roots_arg(roots_str: Optional[str]) -> Optional[List[str]]:
    """Parse --genre-roots "A,B,C" into a list of stripped strings."""
    if not roots_str: return None
    return [r.strip() for r in roots_str.split(",") if r.strip()]

def main()->None:
    parser=build_parser(); args=parser.parse_args()
    if getattr(args,"verbose",False): set_verbosity(VERBOSE)
    elif getattr(args,"quiet",False): set_verbosity(QUIET)
    cfg_path=Path(args.config); cmd=args.cmd
    if cmd=="init": cmd_init(cfg_path); return
    cfg=load_config_with_paths(cfg_path)
    _validate_config(cfg)
    # Human-friendly check: warn if no profiles have source_root configured
    profiles_cfg=cfg.get("profiles",{})
    has_source=any(isinstance(p,dict) and "source_root" in p for p in profiles_cfg.values())
    if not has_source:
        paths_file=cfg_path.parent/"paths.local.yaml"
        example_file=cfg_path.parent/"paths.local.example.yaml"
        if not paths_file.exists():
            err(f"No source paths configured — paths.local.yaml not found.")
            out(f"  To get started:")
            if example_file.exists():
                out(f"    cp {example_file} {paths_file}")
            else:
                out(f"    Create {paths_file} with your source_root paths.")
            out(f"    Then edit it to point at your music folders.")
            sys.exit(2)
    def gp()->str:
        p=getattr(args,"profile",None) or cfg.get("active_profile")
        if not p: raise ValueError("No profile specified and no active_profile set.")
        return p
    if cmd=="profile":
        pc=args.profile_cmd
        if pc=="list":    profile_list(cfg)
        elif pc=="show":  profile_show(cfg,args.name)
        elif pc=="add":   profile_add(cfg_path,cfg,args.name,args.source,args.clean_mode,args.clean_folder,args.review_folder,template=getattr(args,"template",None))
        elif pc=="set":   profile_set(cfg_path,cfg,args.name,args.source,args.clean_mode,args.clean_folder,args.review_folder,template=getattr(args,"template",None))
        elif pc=="delete": profile_delete(cfg_path,cfg,args.name)
        elif pc=="use":   profile_use(cfg_path,cfg,args.name)
    elif cmd=="template":
        tc=args.template_cmd
        if tc=="list":   cmd_template_list(cfg)
        elif tc=="show": cmd_template_show(cfg,args.name)
    elif cmd=="scan":
        gr=_parse_genre_roots_arg(getattr(args,"genre_roots",None))
        cmd_scan(cfg_path,cfg,gp(),args.out,getattr(args,"since",None),genre_roots=gr,itunes_mode=bool(getattr(args,"itunes",False)),session_name=getattr(args,"session_name","") or "")
    elif cmd=="apply":
        pp=load_last_session(cfg) if args.last_session else (Path(args.proposals) if args.proposals else None)
        if not pp: err("Provide proposals.json or --last-session"); sys.exit(1)
        cmd_apply(cfg,pp,interactive=bool(args.interactive),auto_above=args.auto_above,dry_run=bool(args.dry_run))
    elif cmd in("run","go"):
        gr=_parse_genre_roots_arg(getattr(args,"genre_roots",None))
        cmd_go(cfg_path,cfg,gp(),interactive=bool(args.interactive),dry_run=bool(args.dry_run),since=getattr(args,"since",None),perf_tier=getattr(args,"performance",None),genre_roots=gr,itunes_mode=bool(getattr(args,"itunes",False)),review_threshold=getattr(args,"threshold",None),sort_by=getattr(args,"sort","name"),force=bool(getattr(args,"force",False)),auto_above=getattr(args,"auto_above",None),session_name=getattr(args,"session_name","") or "")
    elif cmd=="folders":
        gr=_parse_genre_roots_arg(getattr(args,"genre_roots",None))
        cmd_folders_only(cfg_path,cfg,gp(),interactive=bool(args.interactive),dry_run=bool(args.dry_run),since=getattr(args,"since",None),genre_roots=gr,itunes_mode=bool(getattr(args,"itunes",False)),sort_by=getattr(args,"sort","name"))
    elif cmd=="tracks":   cmd_tracks_only(cfg,gp(),interactive=bool(args.interactive),dry_run=bool(args.dry_run))
    elif cmd=="status":   cmd_status(cfg,gp())
    elif cmd=="resume":   cmd_resume(cfg,args.session_id,interactive=bool(args.interactive),dry_run=bool(args.dry_run))
    elif cmd=="show":     cmd_show(cfg,args.folder,getattr(args,"profile",None) or cfg.get("active_profile",""),show_tracks=bool(getattr(args,"tracks",False)))
    elif cmd=="verify":   cmd_verify(cfg,gp())
    elif cmd=="learn":    cmd_learn(cfg_path,cfg,getattr(args,"session",None))
    elif cmd=="report":   cmd_report(cfg,getattr(args,"session",None),args.format)
    elif cmd=="doctor":   cmd_doctor(cfg_path,cfg)
    elif cmd=="history":  cmd_history(cfg,last=args.last,session=args.session,match=args.match,tracks=bool(args.tracks))
    elif cmd=="undo":     cmd_undo(cfg,action_id=args.id,session_id=("last" if getattr(args,"last",False) else args.session),from_path=args.from_path,tracks=bool(args.tracks),folder=args.folder)
    elif cmd=="sessions": cmd_sessions(cfg,last=getattr(args,"last",20))
    elif cmd=="dump-tree":
        cmd_dump_tree(
            cfg,
            gp(),
            args.out,
            include_clean=args.include_clean,
            include_review=args.include_review,
            include_logs=args.include_logs,
            folders_only=args.folders_only,
            files_only=args.files_only,
        )

    # v4.1 commands
    elif cmd=="tree":
        cmd_tree(cfg,path_str=getattr(args,"path","") or "",audio_only=bool(getattr(args,"audio_only",False)),
                 depth=getattr(args,"depth",None),list_mode=bool(getattr(args,"list_mode",False)),
                 diff_a=(getattr(args,"diff",None) or [None,None])[0],
                 diff_b=(getattr(args,"diff",None) or [None,None])[1])
    elif cmd=="catchall":
        gr=_parse_genre_roots_arg(getattr(args,"genre_roots",None))
        cmd_catchall(cfg,args.path,getattr(args,"profile",None) or cfg.get("active_profile",""),
                     dry_run=bool(getattr(args,"dry_run",False)),genre_roots=gr)
    elif cmd=="genre":
        cmd_genre(cfg_path,cfg,action=args.genre_cmd,name=getattr(args,"name",None))
    # v3.5 commands
    elif cmd=="orphans":      cmd_orphans(cfg,gp())
    elif cmd=="artists":      cmd_artists(cfg,gp(),list_mode=bool(getattr(args,"list_mode",False)),find_query=getattr(args,"find_query",None))
    elif cmd=="review-list":  cmd_review_list(cfg,gp(),older_than_days=getattr(args,"older_than",None))
    elif cmd=="review-promote": cmd_review_promote(cfg,gp(),args.folder,dry_run=bool(getattr(args,"dry_run",False)),artist_override=getattr(args,"artist",None))
    elif cmd=="clean-report": cmd_clean_report(cfg,gp())
    elif cmd=="extract":      cmd_extract_by_artist(cfg,gp(),args.folder,dry_run=bool(getattr(args,"dry_run",False)))
    elif cmd=="compare":      cmd_compare_folders(cfg,args.folder[0],args.folder[1])
    elif cmd=="diff":         cmd_diff(cfg,args.session_a,args.session_b)
    elif cmd=="cache":        cmd_cache(cfg,args.action)
    elif cmd=="reference":
        rc=args.ref_cmd
        cmd_reference(cfg_path,cfg,action=rc,
                      import_path=getattr(args,"file",None),
                      section=getattr(args,"section",None),
                      export_path=getattr(args,"out",None))
    else: parser.error(f"Unknown: {cmd}")

if __name__=="__main__":
    main()
