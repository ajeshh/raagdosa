# Changelog

All notable changes to RaagDosa are documented here.  
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [3.0.0] — 2026-03-05

### Release summary

v3.0 is the first public release. Core architecture is stable and field-tested.
This version is intentionally scoped to library structure and normalisation —
tag writing, duplicate resolution, and MusicBrainz lookup are v4 targets.

---

### New — Library structure

- **`library.template` system** — folder destination is now driven by a configurable
  path template. Default `{artist}/{album}` places albums under an artist subfolder.
  Supports tokens: `{artist}`, `{album}`, `{year}`, `{album_year}`.
- **FLAC segregation** (`library.flac_segregation`) — all-FLAC folders are placed under
  `Artist/FLAC/Album/` when enabled, keeping archival masters separate from MP3 copies.
- **Library template presets** — `{artist}/{album}` (recommended) and `{album}` (flat).
  Full custom templates supported.

### New — Artist normalisation

- **`artist_normalization` config section** — complete fuzzy normalisation pipeline:
  1. Unicode NFC
  2. Unicode char map (user-defined, e.g. `Ø → O` for `MØ → MO`)
  3. ALL-CAPS → Title Case
  4. Alias map (exact canonical mapping, case-insensitive)
  5. Hyphen variant normalisation (en-dash, em-dash → ASCII hyphen)
  6. "The" prefix policy: `keep-front` | `move-to-end` | `strip`
- **`artist_normalization.the_prefix`** — controls "The" handling globally.
  Default `keep-front` (The Beatles stays The Beatles).
- **`artist_normalization.unicode_map`** — per-character substitution before everything else.
- **`artist_normalization.aliases`** — canonical name map. Add `"jay z": "Jay-Z"` and
  all variants collapse to one artist folder automatically.
- **`artist_normalization.fuzzy_dedup_threshold`** — Jaccard word-set similarity
  threshold for fuzzy artist comparison (default 0.92).
- **`artist_normalization.normalize_hyphens`** — normalise typographic hyphens to ASCII.

### New — Incremental scanning (`--since`)

- **`raagdosa go --since last_run`** — only scans folders modified after the last
  successful run timestamp (stored in `logs/clean_manifest.json`). Safe to use every
  time new music is added to source.
- **`raagdosa go --since 2026-01-15`** — scan folders modified after any ISO date.
- **Manifest-based exclusion** — folders already committed to Clean are excluded from
  all future scans regardless of `--since`. Re-scanning is always safe.

### New — Progress indicators

- **Live progress bar** — `Scanning [████████████░░░░░░░░░░] 142/500 28%  Artist - Album`
  printed with carriage-return overwrite during scan and apply passes.
- **Colour-coded output** — CLEAN in green, REVIEW in yellow, DUPES in red,
  confidence scores colour-coded by value (green ≥ 0.90, yellow ≥ 0.75, red below).
- Auto-detects TTY; falls back to plain text for pipes, cron, and log redirection.

### New — Graceful stop

- **Ctrl+C (once)** — sets `stop_after_current` flag. Current folder completes fully,
  then the run stops cleanly. State is logged. Use `raagdosa resume <session_id>` to
  continue from where it left off.
- **Ctrl+C (twice)** — immediate force stop (`sys.exit(130)`). Partially moved folders
  are left in place.

### New — `raagdosa show` (debug command)

```bash
raagdosa show "/path/to/folder"
```

Full breakdown for a single folder without moving anything: tags read per file,
voting results, confidence calculation, proposed name, routing decision, and
actionable tips to get a Review folder into Clean.

### New — `raagdosa verify` (library audit)

```bash
raagdosa verify
```

Read-only audit of `Clean/Albums/`:
- Manifest entries missing from disk
- Disk folders not recorded in manifest (appeared outside RaagDosa)
- Empty folders
- Track filenames that don't match the expected pattern
Writes a timestamped `logs/verify_YYYYMMDD_HHMMSS.txt` report.

### New — `raagdosa learn` (config suggestion)

```bash
raagdosa learn
```

Analyses Review folders from recent sessions, identifies patterns, and interactively
proposes config changes:
- Lower `min_confidence_for_clean` when many folders cluster near the threshold
- Add bracket suffixes (e.g. `deluxe`, `remastered`) to `strip_common_suffixes_for_voting`
- Flag tag-less folders for pre-processing with a tag editor
Writes proposed changes directly to `config.yaml` on confirmation.

### New — Improved session reports (TXT + CSV + HTML)

Every scan now produces three report formats in `logs/sessions/<session_id>/`:

- **`report.txt`** — human-readable summary with aligned columns, confidence scores,
  routing reasons, and format duplicate warnings
- **`report.csv`** — properly formatted CSV with columns: status, confidence,
  original_folder, proposed_name, target_path, track_count, tagged_count,
  unreadable_count, extensions, route_reasons, heuristic, format_duplicates.
  Import directly into Excel or Numbers.
- **`report.html`** — self-contained dark-theme HTML report with colour-coded rows
  (green/yellow/red by status), sortable table, confidence colour coding.
  Open in any browser. `raagdosa report --format html` to regenerate/view.

```bash
raagdosa report                     # view last session (txt)
raagdosa report --format csv        # path to CSV
raagdosa report --format html       # path to HTML
raagdosa report --session <id>      # specific session
```

### New — `raagdosa init` (guided setup)

```bash
raagdosa init
```

Interactive wizard: source folder, library template, FLAC segregation, "The" prefix
policy. Writes a complete `config.yaml`. Followed automatically by next-step instructions.

### New — Python packaging

- `pyproject.toml` with `[project.scripts]` entry point — `raagdosa` command available
  immediately after `pip install raagdosa`.
- Explicit `requires-python = ">=3.9"` and pinned dependencies (`mutagen>=1.46`, `pyyaml>=6.0`).
- `raagdosa --version` flag.

### Improved — `raagdosa status`

- New since-last-run folder count
- Disk space display for destination
- DJ database scan inline
- Manifest entry count and last run timestamp

### Improved — `raagdosa doctor`

- Library template and FLAC segregation summary
- Artist normalisation summary (the_prefix, alias count)
- Mutagen read test on a real file from source

---

## [2.0.0] — 2026-03-04

### Release summary

v2.0 was an internal hardening release focused on correctness and safety.

### Fixed — Logic bugs

- **Display-case name recovery** — dominant album/albumartist now uses the most-common
  raw display form (e.g. `"Mezzanine"`) rather than the normalised voting key
  (`"mezzanine"`). Folder names now have correct capitalisation.
- **Confidence formula** — replaced broken max-of-two formula with true weighted blend:
  `album_share * 0.60 + albumartist_share * 0.40`. Artist fallback penalty applied
  consistently.
- **Clean/Review skip logic** — now uses `str.startswith()` against resolved root paths
  rather than substring match on path components. Fixes false positives for folders
  whose names contain "clean" or "review" (e.g. `/Music/Clean Bandit/`).
- **Sanitize_name slash handling** — slashes/backslashes → space before other illegal
  char substitution. Fixes `AC/DC → AC DC` (not `AC - DC`).

### Added — Missing MVP features

- **Folder name heuristic parser** — `Artist - Album (YYYY)`, `[YYYY] Artist - Album`,
  `Artist_-_Album_YYYY`, ALL-CAPS normalisation, format suffix stripping.
  Heuristic folders always route to Review.
- **Track dry-run preview** — `--dry-run` now prints proposed renames with confidence.
- **`raagdosa status` command** — library overview including folder/track counts,
  manifest entries, pending candidates, DJ database scan, disk space.
- **Cross-run duplicate detection** — scans existing `Clean/Albums/` before routing
  to detect folders already processed in prior sessions.
- **Collision warnings** — target name conflicts produce clear warning and suffix.

### Added — Safeguards

- **Copy-verify-delete** (`safe_move_folder`) — replaces all `shutil.move()` calls.
  Verifies file count and byte size before deleting source.
- **Checksum verification** — optional MD5 per-file verification (`move.use_checksum`).
- **Disk space pre-check** — requires 110% of source size free before starting.
- **Locked file detection** — warns and prompts when files cannot be opened for write.
- **DJ database detection** — scans for Rekordbox/Serato files with configurable
  halt/warn policy.
- **Interrupted-run recovery** — `raagdosa resume <session_id>`.
- **Unicode normalisation** — NFC applied to all path comparisons.
- **Hidden file filtering** — `.DS_Store`, `Thumbs.db`, `._*`, `__MACOSX` excluded.
- **Symlink handling** — `scan.follow_symlinks: false` default.
- **Path length validation** — warns on paths > 260 chars (Windows limit).
- **Config schema validation** — version mismatch, missing sections, out-of-range values.
- **Log rotation** — archives logs exceeding `rotate_log_max_mb`.
- **Proposal path validation** — anti-traversal check on all target paths.
- **Clean manifest** — persistent `logs/clean_manifest.json` for cross-run tracking.
- **Format duplicate detection** — flags same-stem multi-format files (`.mp3` + `.flac`).

---

## [1.0.0] — 2026-03-03

Initial internal release. Core scan → vote → propose → move → rename pipeline.
Single-file Python script with YAML config. No packaging, no tests.
