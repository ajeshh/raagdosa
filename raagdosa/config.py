"""
RaagDosa config — config loading, validation, paths overlay, migrations.

Layer 2: may import from ui (L0) and core (L0).
"""
from __future__ import annotations

import fnmatch
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore[assignment]

from raagdosa.ui import C, out, err


# ─────────────────────────────────────────────
# YAML helpers
# ─────────────────────────────────────────────
def read_yaml(path: Path) -> Dict[str, Any]:
    if yaml is None:
        raise RuntimeError("Missing: pyyaml — pip install pyyaml")
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def write_yaml(path: Path, cfg: Dict[str, Any]) -> None:
    if yaml is None:
        raise RuntimeError("Missing: pyyaml — pip install pyyaml")
    path.write_text(
        yaml.safe_dump(cfg, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge overlay into base. overlay values win for non-dict leaves."""
    merged = dict(base)
    for k, v in overlay.items():
        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


# ─────────────────────────────────────────────
# Paths overlay
# ─────────────────────────────────────────────
def load_paths_overlay(cfg_path: Path) -> Dict[str, Any]:
    """Load paths.local.yaml if it exists alongside the config file."""
    paths_file = cfg_path.parent / "paths.local.yaml"
    if paths_file.exists():
        return read_yaml(paths_file)
    return {}


_PATH_KEYS_IN_PROFILE = {"source_root", "clean_mode"}
_PATH_KEYS_TOPLEVEL = {"active_profile"}


def _has_paths_in_config(cfg: Dict[str, Any]) -> bool:
    """Check if config.yaml still contains path-related keys that should be in paths.local.yaml."""
    for pname, pdata in (cfg.get("profiles") or {}).items():
        if isinstance(pdata, dict) and "source_root" in pdata:
            return True
    lcfg = cfg.get("logging", {})
    if lcfg.get("root_dir") and not lcfg.get("_paths_migrated"):
        return True
    return False


def _migrate_paths_to_local(cfg_path: Path, cfg: Dict[str, Any]) -> None:
    """Offer to extract paths from config.yaml into paths.local.yaml."""
    paths_file = cfg_path.parent / "paths.local.yaml"
    if paths_file.exists():
        return
    if not _has_paths_in_config(cfg):
        return
    out(f"\n{C.YELLOW}v7.0 change:{C.RESET} Paths now belong in "
        f"{C.BOLD}paths.local.yaml{C.RESET} (keeps config.yaml safe to share).")
    out(f"  Found paths in config.yaml — extracting to paths.local.yaml...")
    paths_data: Dict[str, Any] = {}
    if cfg.get("profiles"):
        paths_data["profiles"] = {}
        for pname, pdata in cfg["profiles"].items():
            if isinstance(pdata, dict):
                extracted = {}
                for k in list(pdata.keys()):
                    if k in _PATH_KEYS_IN_PROFILE:
                        extracted[k] = pdata[k]
                if extracted:
                    paths_data["profiles"][pname] = extracted
    if cfg.get("active_profile"):
        paths_data["active_profile"] = cfg["active_profile"]
    lcfg = cfg.get("logging", {})
    log_keys = ["root_dir", "session_dir", "history_log", "skipped_log",
                "track_history_log", "track_skipped_log"]
    extracted_log = {k: lcfg[k] for k in log_keys if k in lcfg}
    if extracted_log:
        paths_data["logging"] = extracted_log
    if paths_data:
        write_yaml(paths_file, paths_data)
        out(f"  {C.GREEN}Created:{C.RESET} {paths_file}")
        out(f"  {C.DIM}Paths in config.yaml still work but are now overridden "
            f"by paths.local.yaml.{C.RESET}")
        out(f"  {C.DIM}You can safely remove path keys from config.yaml "
            f"when ready.{C.RESET}\n")


def load_config_with_paths(cfg_path: Path) -> Dict[str, Any]:
    """Load config.yaml, then overlay paths.local.yaml, then handle brain->reference migration."""
    cfg = read_yaml(cfg_path)
    _migrate_paths_to_local(cfg_path, cfg)
    paths = load_paths_overlay(cfg_path)
    if paths:
        cfg = _deep_merge(cfg, paths)
    # brain -> reference migration (v7.0)
    if "brain" in cfg and "reference" not in cfg:
        cfg["reference"] = cfg.pop("brain")
    elif "brain" in cfg and "reference" in cfg:
        cfg["reference"] = _deep_merge(cfg.pop("brain"), cfg["reference"])
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
                    errors.append(
                        f"review_rules.{key} must be between 0.0 and 1.0, got {val}")
            except (TypeError, ValueError):
                errors.append(
                    f"review_rules.{key} must be a number, got '{val}'")
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
            errors.append(
                f"profiles.{pname} must be a mapping, got {type(pdata).__name__}")
    if errors:
        err("Config validation failed:")
        for e in errors:
            out(f"  - {e}")
        sys.exit(2)


# ─────────────────────────────────────────────
# Per-folder override (.raagdosa file)
# ─────────────────────────────────────────────
def load_folder_override(folder: Path) -> Optional[Dict[str, Any]]:
    """
    Load a .raagdosa YAML override from inside a folder.
    Supported keys: name, artist, album, year, skip, force_clean, confidence_boost
    """
    p = folder / ".raagdosa"
    if not p.exists():
        return None
    if yaml is None:
        return None
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def folder_matches_ignore(folder_name: str, patterns: List[str]) -> bool:
    """True if folder_name matches any pattern (exact or glob)."""
    for pat in patterns:
        if folder_name == pat or fnmatch.fnmatch(folder_name, pat):
            return True
    return False
