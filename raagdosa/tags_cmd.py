"""
RaagDosa tags_cmd — tag fix review, apply, and undo commands.

Layer 7: CLI commands that work with the scanner database.
Imports from core (L0), ui (L0), tags (L1).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from raagdosa.core import make_session_id, now_iso
from raagdosa.ui import C, out, err, conf_color, read_key
from raagdosa.tags import read_tags, write_tag, RISK_TIERS


# ─────────────────────────────────────────────
# Schema for tag apply tracking tables
# ─────────────────────────────────────────────
_TAG_APPLY_SCHEMA = """
CREATE TABLE IF NOT EXISTS tag_apply_sessions (
    apply_session_id TEXT PRIMARY KEY,
    applied_at       TEXT NOT NULL,
    proposal_count   INTEGER NOT NULL DEFAULT 0,
    dry_run          INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS tag_snapshots (
    snapshot_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    apply_session_id TEXT NOT NULL REFERENCES tag_apply_sessions(apply_session_id),
    proposal_id   INTEGER NOT NULL,
    file_path     TEXT NOT NULL,
    field_name    TEXT NOT NULL,
    original_value TEXT,
    new_value     TEXT NOT NULL,
    written       INTEGER NOT NULL DEFAULT 0,
    undone        INTEGER NOT NULL DEFAULT 0,
    undone_at     TEXT
);
CREATE INDEX IF NOT EXISTS idx_tag_snapshots_session ON tag_snapshots(apply_session_id);
CREATE INDEX IF NOT EXISTS idx_tag_snapshots_proposal ON tag_snapshots(proposal_id);
"""


def _open_scanner_db(db_path: str):
    """Open the scanner SQLite DB, ensure tag_apply tables exist. Returns connection or None."""
    p = Path(db_path)
    if not p.exists():
        err(f"Scanner database not found: {db_path}")
        out(f"  Run 'raagdosa-scanner scan <folder>' first to generate proposals.")
        return None
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(_TAG_APPLY_SCHEMA)
    conn.commit()
    return conn


# ─────────────────────────────────────────────
# cmd_tags_status
# ─────────────────────────────────────────────
def cmd_tags_status(cfg: Dict[str, Any], db_path: str) -> None:
    """Show proposal summary from scanner DB, grouped by risk tier and status."""
    conn = _open_scanner_db(db_path)
    if not conn:
        return
    try:
        tag_fix_cfg = cfg.get("tag_fix", {})
        enabled = set(tag_fix_cfg.get("enabled_fixes", []))

        rows = conn.execute("""
            SELECT p.fix_type, p.status, COUNT(*) as cnt,
                   AVG(p.confidence) as avg_conf
            FROM proposals p
            GROUP BY p.fix_type, p.status
            ORDER BY p.fix_type, p.status
        """).fetchall()

        if not rows:
            out(f"\n  {C.DIM}No proposals found in {db_path}{C.RESET}")
            return

        out(f"\n{C.BOLD}Tag Fix Status{C.RESET}  ({db_path})")
        out(f"{'═' * 60}")

        by_type: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            ft = r["fix_type"]
            if ft not in by_type:
                by_type[ft] = {"pending": 0, "accepted": 0, "rejected": 0, "applied": 0, "total": 0}
            by_type[ft][r["status"]] = r["cnt"]
            by_type[ft]["total"] += r["cnt"]
            if r["status"] == "pending":
                by_type[ft]["avg_conf"] = r["avg_conf"]

        for tier in ("safe", "moderate", "destructive"):
            tier_types = [(ft, d) for ft, d in by_type.items() if RISK_TIERS.get(ft) == tier]
            if not tier_types:
                continue
            color = C.GREEN if tier == "safe" else (C.YELLOW if tier == "moderate" else C.RED)
            out(f"\n  {color}{C.BOLD}{tier.upper()}{C.RESET}")
            for ft, d in sorted(tier_types):
                enabled_mark = "  " if ft in enabled else f"{C.DIM}✗ "
                avg = d.get("avg_conf", 0) or 0
                out(f"  {enabled_mark}{ft:<28}{C.RESET} "
                    f"pending={C.BOLD}{d['pending']}{C.RESET}  "
                    f"accepted={C.GREEN}{d['accepted']}{C.RESET}  "
                    f"rejected={C.RED}{d.get('rejected', 0)}{C.RESET}  "
                    f"applied={C.CYAN}{d.get('applied', 0)}{C.RESET}  "
                    f"avg_conf={avg:.2f}")

        total_pending = sum(d["pending"] for d in by_type.values())
        total_accepted = sum(d["accepted"] for d in by_type.values())
        total_applied = sum(d.get("applied", 0) for d in by_type.values())
        out(f"\n{'─' * 60}")
        out(f"  Total: {C.BOLD}{total_pending}{C.RESET} pending  "
            f"{C.GREEN}{total_accepted}{C.RESET} accepted  "
            f"{C.CYAN}{total_applied}{C.RESET} applied")

        sessions = conn.execute("""
            SELECT apply_session_id, applied_at, proposal_count
            FROM tag_apply_sessions
            ORDER BY applied_at DESC LIMIT 5
        """).fetchall()
        if sessions:
            out(f"\n  {C.BOLD}Recent apply sessions:{C.RESET}")
            for s in sessions:
                out(f"    {s['apply_session_id'][:12]}…  {s['applied_at'][:19]}  "
                    f"{s['proposal_count']} proposals")
        out("")
    finally:
        conn.close()


# ─────────────────────────────────────────────
# cmd_tags_review
# ─────────────────────────────────────────────
def cmd_tags_review(cfg: Dict[str, Any], db_path: str, *,
                    folder: Optional[str] = None, risk: Optional[str] = None,
                    fix_type: Optional[str] = None, auto: bool = False,
                    include_protected: bool = False) -> None:
    """Interactively review pending proposals from scanner DB."""
    conn = _open_scanner_db(db_path)
    if not conn:
        return
    try:
        tag_fix_cfg = cfg.get("tag_fix", {})
        protected = set(tag_fix_cfg.get("protected_fields", ["title", "artist"]))
        enabled = set(tag_fix_cfg.get("enabled_fixes", []))
        auto_threshold = float(tag_fix_cfg.get("auto_approve_threshold", 1.0))
        auto_risk_tiers = set(tag_fix_cfg.get("auto_approve_risk_tiers", ["safe"]))

        conditions = ["p.status = 'pending'"]
        params: List[Any] = []
        if folder:
            conditions.append("f.folder_path LIKE ?")
            params.append(f"%{folder}%")
        if risk:
            risk_types = [ft for ft, tier in RISK_TIERS.items() if tier == risk]
            if risk_types:
                placeholders = ",".join("?" for _ in risk_types)
                conditions.append(f"p.fix_type IN ({placeholders})")
                params.extend(risk_types)
        if fix_type:
            conditions.append("p.fix_type = ?")
            params.append(fix_type)
        if not include_protected:
            for pf in protected:
                conditions.append("p.field_name != ?")
                params.append(pf)
        if enabled:
            placeholders = ",".join("?" for _ in enabled)
            conditions.append(f"p.fix_type IN ({placeholders})")
            params.extend(enabled)

        where = " AND ".join(conditions)
        query = f"""
            SELECT p.proposal_id, p.fix_type, p.field_name, p.old_value, p.new_value,
                   p.confidence, p.reason, f.file_path,
                   COALESCE(f.folder_path, '') as folder_path
            FROM proposals p
            JOIN files f ON p.file_id = f.file_id
            WHERE {where}
            ORDER BY f.folder_path, p.fix_type, p.confidence DESC
        """
        proposals = conn.execute(query, params).fetchall()

        if not proposals:
            out(f"\n  {C.DIM}No pending proposals match the filters.{C.RESET}")
            return

        out(f"\n{C.BOLD}Tag Fix Review{C.RESET}  ({len(proposals)} proposals)")
        out(f"{'═' * 60}")
        if not include_protected:
            out(f"  {C.DIM}Protected fields excluded: {', '.join(protected)}{C.RESET}")
        out(f"  {C.DIM}[a]ccept  [r]eject  [s]kip  [q]uit{C.RESET}\n")

        accepted = 0
        rejected = 0
        skipped = 0
        current_folder = ""

        for row in proposals:
            fp = row["folder_path"]
            if fp != current_folder:
                current_folder = fp
                out(f"\n  {C.BOLD}{Path(fp).name}{C.RESET}  {C.DIM}{fp}{C.RESET}")

            tier = RISK_TIERS.get(row["fix_type"], "?")
            tier_color = C.GREEN if tier == "safe" else (C.YELLOW if tier == "moderate" else C.RED)
            conf = row["confidence"]
            conf_str = conf_color(conf)

            if auto and tier in auto_risk_tiers and conf >= auto_threshold:
                conn.execute("UPDATE proposals SET status='accepted' WHERE proposal_id=?",
                             (row["proposal_id"],))
                accepted += 1
                out(f"    {C.GREEN}AUTO ✓{C.RESET}  {tier_color}[{tier}]{C.RESET} "
                    f"{row['fix_type']:<24} {row['field_name']:<12} "
                    f"{conf_str}  {C.DIM}{row['reason']}{C.RESET}")
                continue

            old_v = row["old_value"] or "(empty)"
            new_v = row["new_value"] or "(empty)"
            out(f"    {tier_color}[{tier}]{C.RESET} "
                f"{row['fix_type']:<24} {row['field_name']:<12} "
                f"{conf_str}")
            out(f"      {C.DIM}File:{C.RESET}   {Path(row['file_path']).name}")
            out(f"      {C.RED}Old:{C.RESET}    {old_v}")
            out(f"      {C.GREEN}New:{C.RESET}    {new_v}")
            out(f"      {C.DIM}Why:{C.RESET}    {row['reason']}")

            while True:
                try:
                    choice = read_key(f"      [{C.GREEN}a{C.RESET}]ccept  "
                                      f"[{C.RED}r{C.RESET}]eject  "
                                      f"[s]kip  [q]uit  > ")
                except (KeyboardInterrupt, EOFError):
                    choice = "q"
                if choice in ("a", "r", "s", "q"):
                    break

            if choice == "a":
                conn.execute("UPDATE proposals SET status='accepted' WHERE proposal_id=?",
                             (row["proposal_id"],))
                accepted += 1
                out(f"      {C.GREEN}✓ Accepted{C.RESET}")
            elif choice == "r":
                conn.execute("UPDATE proposals SET status='rejected' WHERE proposal_id=?",
                             (row["proposal_id"],))
                rejected += 1
                out(f"      {C.RED}✗ Rejected{C.RESET}")
            elif choice == "s":
                skipped += 1
                out(f"      {C.DIM}— Skipped{C.RESET}")
            elif choice == "q":
                conn.commit()
                out(f"\n  {C.BOLD}Review paused.{C.RESET}")
                break

        conn.commit()
        out(f"\n{'─' * 60}")
        out(f"  {C.GREEN}Accepted: {accepted}{C.RESET}  "
            f"{C.RED}Rejected: {rejected}{C.RESET}  "
            f"Skipped: {skipped}")
        if accepted:
            out(f"  Run '{C.BOLD}raagdosa tags apply{C.RESET}' to write changes to files.")
        out("")
    finally:
        conn.close()


# ─────────────────────────────────────────────
# cmd_tags_apply
# ─────────────────────────────────────────────
def cmd_tags_apply(cfg: Dict[str, Any], db_path: str, *,
                   dry_run: bool = False, max_batch: Optional[int] = None) -> None:
    """Apply accepted proposals to audio files. Snapshots originals for undo."""
    conn = _open_scanner_db(db_path)
    if not conn:
        return
    try:
        tag_fix_cfg = cfg.get("tag_fix", {})
        protected = set(tag_fix_cfg.get("protected_fields", ["title", "artist"]))
        batch_size = max_batch or int(tag_fix_cfg.get("max_batch_size", 50))

        conditions = ["p.status = 'accepted'"]
        params: List[Any] = []
        for pf in protected:
            conditions.append("p.field_name != ?")
            params.append(pf)
        where = " AND ".join(conditions)

        proposals = conn.execute(f"""
            SELECT p.proposal_id, p.fix_type, p.field_name, p.old_value, p.new_value,
                   p.confidence, p.reason, f.file_path
            FROM proposals p
            JOIN files f ON p.file_id = f.file_id
            WHERE {where}
            ORDER BY f.file_path, p.fix_type
            LIMIT ?
        """, params + [batch_size]).fetchall()

        if not proposals:
            out(f"\n  {C.DIM}No accepted proposals to apply.{C.RESET}")
            out(f"  Run '{C.BOLD}raagdosa tags review{C.RESET}' first to accept proposals.")
            return

        session_id = make_session_id()
        now_str = now_iso()

        out(f"\n{C.BOLD}Tag Fix Apply{C.RESET}  (session: {session_id[:12]}…)")
        out(f"{'═' * 60}")
        out(f"  Proposals: {len(proposals)}")
        if dry_run:
            out(f"  {C.YELLOW}DRY RUN — no files will be modified{C.RESET}")
        out("")

        by_file: Dict[str, List] = {}
        for row in proposals:
            fp = row["file_path"]
            if fp not in by_file:
                by_file[fp] = []
            by_file[fp].append(row)

        if not dry_run:
            out(f"  {C.BOLD}{len(proposals)} tag changes{C.RESET} across "
                f"{C.BOLD}{len(by_file)} files{C.RESET}")
            out(f"  Protected fields (excluded): {', '.join(protected)}")
            out("")
            try:
                answer = input(f"  Apply these changes? [{C.GREEN}y{C.RESET}/N] ").strip().lower()
            except (KeyboardInterrupt, EOFError):
                answer = "n"
            if answer != "y":
                out(f"  {C.DIM}Cancelled.{C.RESET}")
                return

        conn.execute(
            "INSERT INTO tag_apply_sessions (apply_session_id, applied_at, proposal_count, dry_run) VALUES (?,?,?,?)",
            (session_id, now_str, len(proposals), 1 if dry_run else 0))

        applied = 0
        failed = 0

        for fp, file_proposals in by_file.items():
            file_path = Path(fp)
            fname = file_path.name
            file_exists = file_path.exists()

            if not file_exists and not dry_run:
                out(f"  {C.RED}MISSING{C.RESET}  {fname}")
                for row in file_proposals:
                    conn.execute("UPDATE proposals SET status='pending' WHERE proposal_id=?",
                                 (row["proposal_id"],))
                failed += len(file_proposals)
                continue

            current_tags = read_tags(file_path) if file_exists else {}

            for row in file_proposals:
                field = row["field_name"]
                new_val = row["new_value"]
                old_val = current_tags.get(field) if file_exists else row["old_value"]

                conn.execute("""
                    INSERT INTO tag_snapshots
                    (apply_session_id, proposal_id, file_path, field_name, original_value, new_value, written)
                    VALUES (?,?,?,?,?,?,?)
                """, (session_id, row["proposal_id"], fp, field, old_val, new_val, 0 if dry_run else 0))

                tier = RISK_TIERS.get(row["fix_type"], "?")
                tier_color = C.GREEN if tier == "safe" else (C.YELLOW if tier == "moderate" else C.RED)

                if dry_run:
                    out(f"  {C.DIM}DRY{C.RESET}  {fname:<40} {field:<12} "
                        f"{tier_color}[{tier}]{C.RESET} "
                        f"'{old_val or ''}' → '{new_val}'")
                    applied += 1
                    continue

                if write_tag(file_path, field, new_val):
                    conn.execute("UPDATE tag_snapshots SET written=1 WHERE proposal_id=? AND apply_session_id=?",
                                 (row["proposal_id"], session_id))
                    conn.execute("UPDATE proposals SET status='applied' WHERE proposal_id=?",
                                 (row["proposal_id"],))
                    applied += 1
                    out(f"  {C.GREEN}✓{C.RESET}  {fname:<40} {field:<12} "
                        f"{tier_color}[{tier}]{C.RESET} "
                        f"'{old_val or ''}' → '{new_val}'")
                else:
                    conn.execute("UPDATE proposals SET status='pending' WHERE proposal_id=?",
                                 (row["proposal_id"],))
                    failed += 1
                    out(f"  {C.RED}✗{C.RESET}  {fname:<40} {field:<12} WRITE FAILED")

        conn.commit()

        out(f"\n{'─' * 60}")
        if dry_run:
            out(f"  {C.YELLOW}DRY RUN{C.RESET}: {applied} changes previewed, 0 written")
        else:
            out(f"  {C.GREEN}Applied: {applied}{C.RESET}  "
                f"{C.RED}Failed: {failed}{C.RESET}")
            if applied:
                out(f"  Undo:  raagdosa tags undo --last")
        out("")
    finally:
        conn.close()


# ─────────────────────────────────────────────
# cmd_tags_undo
# ─────────────────────────────────────────────
def cmd_tags_undo(cfg: Dict[str, Any], db_path: str, *,
                  session_id: Optional[str] = None, last: bool = False) -> None:
    """Revert applied tag changes from snapshots."""
    conn = _open_scanner_db(db_path)
    if not conn:
        return
    try:
        if last:
            row = conn.execute("""
                SELECT apply_session_id FROM tag_apply_sessions
                WHERE dry_run = 0
                ORDER BY applied_at DESC LIMIT 1
            """).fetchone()
            if not row:
                out(f"\n  {C.DIM}No apply sessions found to undo.{C.RESET}")
                return
            session_id = row["apply_session_id"]
        elif not session_id:
            sessions = conn.execute("""
                SELECT apply_session_id, applied_at, proposal_count
                FROM tag_apply_sessions
                WHERE dry_run = 0
                ORDER BY applied_at DESC LIMIT 10
            """).fetchall()
            if not sessions:
                out(f"\n  {C.DIM}No apply sessions found.{C.RESET}")
                return
            out(f"\n{C.BOLD}Apply Sessions{C.RESET}")
            out(f"{'─' * 60}")
            for s in sessions:
                out(f"  {s['apply_session_id']}  {s['applied_at'][:19]}  {s['proposal_count']} proposals")
            out(f"\n  Use: raagdosa tags undo --session <ID>")
            out(f"       raagdosa tags undo --last")
            return

        snapshots = conn.execute("""
            SELECT snapshot_id, proposal_id, file_path, field_name, original_value, new_value
            FROM tag_snapshots
            WHERE apply_session_id = ? AND written = 1 AND undone = 0
            ORDER BY file_path
        """, (session_id,)).fetchall()

        if not snapshots:
            out(f"\n  {C.DIM}No undoable changes in session {session_id[:12]}…{C.RESET}")
            return

        out(f"\n{C.BOLD}Tag Fix Undo{C.RESET}  (session: {session_id[:12]}…)")
        out(f"{'═' * 60}")
        out(f"  Changes to revert: {len(snapshots)}")
        out("")

        try:
            answer = input(f"  Revert {len(snapshots)} tag changes? [{C.GREEN}y{C.RESET}/N] ").strip().lower()
        except (KeyboardInterrupt, EOFError):
            answer = "n"
        if answer != "y":
            out(f"  {C.DIM}Cancelled.{C.RESET}")
            return

        reverted = 0
        failed = 0
        now_str = now_iso()

        for snap in snapshots:
            file_path = Path(snap["file_path"])
            fname = file_path.name
            field = snap["field_name"]
            original = snap["original_value"]

            if not file_path.exists():
                out(f"  {C.RED}MISSING{C.RESET}  {fname}")
                failed += 1
                continue

            if original is None:
                original = ""

            if write_tag(file_path, field, original):
                conn.execute("UPDATE tag_snapshots SET undone=1, undone_at=? WHERE snapshot_id=?",
                             (now_str, snap["snapshot_id"]))
                conn.execute("UPDATE proposals SET status='accepted' WHERE proposal_id=?",
                             (snap["proposal_id"],))
                reverted += 1
                out(f"  {C.GREEN}↩{C.RESET}  {fname:<40} {field:<12} "
                    f"'{snap['new_value']}' → '{original or '(empty)'}'")
            else:
                failed += 1
                out(f"  {C.RED}✗{C.RESET}  {fname:<40} {field:<12} REVERT FAILED")

        conn.commit()

        out(f"\n{'─' * 60}")
        out(f"  {C.GREEN}Reverted: {reverted}{C.RESET}  "
            f"{C.RED}Failed: {failed}{C.RESET}")
        out("")
    finally:
        conn.close()
