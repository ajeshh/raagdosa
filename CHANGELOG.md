# Changelog

All notable changes to RaagDosa are documented here.  
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

---

## [3.5.1] — 2026-03-06

### Release summary

Performance patch. Three independent wins that compound: same-filesystem moves are now
atomic renames (~1ms vs 1s/album), scan and apply now overlap via a streaming pipeline,
and a persistent tag cache eliminates mutagen reads for unchanged files on subsequent runs.

---

### Performance

- **Same-filesystem fast path in `safe_move_folder`** — detects when source and destination
  are on the same device (`st_dev` comparison, walking up ancestry for non-existent paths).
  Uses `os.rename()` instead of `copytree → verify → rmtree`. Atomic, ~1ms per folder
  regardless of size. Falls back to copy+verify for cross-device moves. Typical DJ setup
  (everything on one drive): 500 albums moves from ~8 minutes to ~0.5 seconds.

- **Streaming scan→apply pipeline** — scan and apply now overlap. Candidates are split
  into configurable batches (`scan.streaming_batch_size`, default 50). A background scanner
  thread fills a queue; the main thread drains it, routes proposals, applies moves, and
  renames tracks per batch immediately. First folder moves within seconds of starting even
  on large libraries. Within-run duplicate tracking is maintained correctly across batches
  via a shared accumulator.

- **Persistent tag cache** — `TagCache` persisted to `logs/tag_cache.json`, keyed by
  `(absolute_path, mtime)`. On warm runs (files unchanged), mutagen is skipped entirely.
  Parallel-safe with `threading.Lock`. Atomic disk write via temp-file replace. Managed
  via `raagdosa cache` (status / clear / evict).

- **Parallel scan workers** — `ThreadPoolExecutor` within each batch for concurrent tag
  reading. I/O-bound so threads give ~7× speedup vs sequential. Configurable via
  `scan.workers` (default: min(8, cpu_count)).

- **Thread-safe progress bar** — `Progress` now uses `Lock` for concurrent tick() calls,
  shows real-time rate (folders/s) and ETA alongside the bar.

- **move_method logged** — history entries now include `move_method: rename|copy` and
  elapsed ms. `--dry-run` previews which method would be used per folder.

### Config

- `scan.workers: 8` — parallel scan workers
- `scan.tag_cache_enabled: true` — persistent tag cache on/off
- `scan.streaming_batch_size: 50` — folders per batch in streaming pipeline

### Commands

- `raagdosa cache` — show cache status (entries, size, last saved)
- `raagdosa cache clear` — force full re-read on next scan
- `raagdosa cache evict` — remove stale entries for missing files


## [3.5.0] — 2026-03-06

### Release summary

v3.5 delivers the intelligence layer on top of the v3.0 foundation: deeper naming logic,
a richer multi-factor confidence system, mix/EP classification, seven new commands, and a suite
of quality-of-life improvements from the 3.1 planning backlog. No database — stays
fully CLI-only and file-system-based.

---

### New — Logic / Naming Intelligence

- **EP detection** — folders with 3–6 tracks classified as EPs, labelled `[EP]` in
  proposed folder name (configurable via `ep_detection`).
- **Garbage naming** — three-stage pipeline: bracket stack stripper (noise/promo/format
  bracket groups removed from album names), token flood guard (5+ parenthetical groups
  flagged), promo watermark detection (`www.`, `.com`, `free download` etc.).
- **Mojibake detection** — garbled double-encoded Unicode flagged; folder routed to Review.
- **Title resolution priority chain** — tag-first; numeric filename prefix fallback; heuristic last.
- **Vinyl track notation** — A1/B2/C3/D4 side-track format recognised and converted to
  absolute track numbers (A1=1, B1=9, C1=17, D1=25).
- **Capitalisation pathologies** — ALL CAPS and all-lowercase album/title names converted
  to intelligent title case. Configurable via `title_case.never_cap` and
  `title_case.always_cap` (e.g. always-cap `DJ`, `MC`, `UK`, `EP`).
- **Bracket content classifier** — each bracket group tagged as
  year | format | edition | remix_credit | promo | noise | unknown.
- **Per-folder `.raagdosa` override file** — place YAML inside any folder to force
  `album`, `artist`, `year`, `skip`, or `confidence_boost`.
- **Disc indicator stripping** — `Album - Disc 1`, `Album (CD2)` normalised to `Album`
  for voting and deduplication.
- **Display name noise stripping** — `(Official Audio)`, `[HD]`, `(Lyrics)` stripped
  from album names and titles before voting.

### New — Completeness + Confidence

- **Named confidence factor breakdown** — `confidence_factors` dict in `decision`:
  - `dominance` (0.40 weight) — vote quality
  - `tag_coverage` (0.15) — fraction of tracks with tags
  - `title_quality` (0.12) — meaningful title ratio
  - `completeness` (0.12) — track gap and duplicate penalties
  - `filename_consistency` (0.10) — filename vs tag agreement
  - `aa_consistency` (0.06) — albumartist uniformity
  - `folder_alignment` (0.05) — source folder name match bonus
- **Track gap detection** — missing track numbers penalise `completeness` factor.
- **Duplicate track numbers** — same track number on two files penalises score.
- **Meaningful title ratio** — garbage/watermark titles reduce `title_quality`.
- **Filename-vs-tag consistency** — `Artist - Title` filename pattern scored against tags.
- **`raagdosa show` confidence bar chart** — each factor shown as colour-coded bar.

### New — Large Folder / Mix Handling

- **Mix / chart classifier** — keyword matching plus unique-artist ratio heuristic.
- **Mix routing** — mix folders go to `Clean/_Mixes/` (configurable via `library.mixes_folder`).
- **`raagdosa extract --by-artist FOLDER`** — splits a VA/mix folder into per-artist groups.
- **`raagdosa compare --folder A B`** — diffs two folders: track overlap, tag comparison.

### New — Commands

- **`raagdosa orphans`** — find loose audio files in Clean/Review outside album subfolders.
- **`raagdosa artists --list`** — list artist directories with album and track counts.
- **`raagdosa artists --find <query>`** — fuzzy-find an artist in Clean.
- **`raagdosa review-list`** — tabular view of Review folders with age, confidence, reason.
- **`raagdosa review-list --older-than <days>`** — filter to long-stale folders.
- **`raagdosa clean-report`** — Clean library audit: counts, formats, quality issues.
- **`raagdosa show --tracks`** — per-track rename preview alongside folder analysis.
- **`raagdosa diff <session_a> <session_b>`** — compare two sessions side-by-side.
  Accepts `last` and `prev` as shorthand.

### Improved — From 3.1 planning backlog

- **Extension case normalisation** — `.MP3` → `.mp3` during track rename pass.
- **Windows reserved filename sanitisation** — `CON`, `NUL`, `PRN`, `COMn`, `LPTn`
  get a trailing underscore before use as folder names.
- **Empty parent cleanup** — empty parent dirs left after folder moves are removed.
- **`ignore_folder_names` glob support** — patterns like `_*`, `tmp*` now work.
- **Collision detail in route reasons** — `duplicate_in_run` now includes colliding name.
- **`.raagdosa` override shown in `raagdosa show`** — override applied and displayed.

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
