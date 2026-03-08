<div align="center">

```
██████╗  █████╗  █████╗  ██████╗ ██████╗  ██████╗ ███████╗ █████╗
██╔══██╗██╔══██╗██╔══██╗██╔════╝ ██╔══██╗██╔═══██╗██╔════╝██╔══██╗
██████╔╝███████║███████║██║  ███╗██║  ██║██║   ██║███████╗███████║
██╔══██╗██╔══██║██╔══██║██║   ██║██║  ██║██║   ██║╚════██║██╔══██║
██║  ██║██║  ██║██║  ██║╚██████╔╝██████╔╝╚██████╔╝███████║██║  ██║
╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═════╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝
```

**Deterministic music library cleanup for DJs and collectors.**

[![PyPI version](https://img.shields.io/pypi/v/raagdosa?style=flat-square&color=0d1117&labelColor=21262d&logo=pypi&logoColor=f5f5f5)](https://pypi.org/project/raagdosa/)
[![Python versions](https://img.shields.io/pypi/pyversions/raagdosa?style=flat-square&color=0d1117&labelColor=21262d&logo=python&logoColor=f5f5f5)](https://pypi.org/project/raagdosa/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue?style=flat-square&color=0d1117&labelColor=21262d)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen?style=flat-square&color=0d1117&labelColor=21262d&logo=github-actions&logoColor=f5f5f5)](https://github.com/YOUR_USERNAME/raagdosa/actions)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg?style=flat-square)](https://github.com/psf/black)

</div>

---

RaagDosa is **Calibre for DJs** — a local-first, CLI-driven tool that transforms a chaotic music folder into a clean, coherent library structure you can trust. It reads your tags, votes across tracks to find consensus metadata, assigns a confidence score, and routes each album to `Clean/` or `Review/` accordingly.

It never touches your source. Every action is sessioned, logged, and fully undoable.

```
raagdosa go --dry-run     # see exactly what would happen
raagdosa go               # do it
raagdosa undo --session last  # change your mind
```

---

## What it looks like

```
Session:   2026-03-06_14-30-00_a3f1
Pipeline:  streaming batches of 50, 8 scan workers

Scanning [██████████████████████] 833/833 100%  42/s  ~0s  Portishead - Dummy

  MOVED ✦ clean   Massive Attack - Mezzanine (1998)   conf=0.97
  MOVED ✦ clean   Portishead - Dummy (1994)           conf=0.95
  MOVED ◐ review  Unknown Artist - 2024-03-01         conf=0.52  [low_confidence, heuristic_fallback]
  MOVED ✦ clean   The Prodigy - Music for the Jilted Generation (1994)  conf=0.91
  MOVED ◈ dupes   Massive Attack - Mezzanine (1998)   conf=0.97  [duplicate_in_run]
  MOVED ✦ clean   DJ Shadow - Endtroducing..... (1996)  conf=0.94
  MOVED ✦ clean   Björk - Homogenic (1997)            conf=0.93

  All 502 folder(s) moved via instant rename (same filesystem)

Results:   833 proposals | Clean: 712 | Review: 98 | Dupes: 23
Reports:   logs/sessions/2026-03-06_14-30-00_a3f1/report.{txt,csv,html}
```

---

## Installation

```bash
pip install raagdosa
```

**Requirements:** Python 3.9+ · [`mutagen`](https://pypi.org/project/mutagen/) · [`pyyaml`](https://pypi.org/project/PyYAML/)

Or run from source:

```bash
git clone https://github.com/YOUR_USERNAME/raagdosa
cd raagdosa
pip install -e .
```

---

## Quick start

```bash
# 1. Set up your config (interactive wizard)
raagdosa init

# 2. Preview — nothing moves
raagdosa go --dry-run

# 3. Run it
raagdosa go
```

That's it for most sessions. On subsequent runs:

```bash
raagdosa go --since last_run   # only process music added since last time
```

---

## How it works

RaagDosa's pipeline has six stages — each folder goes through all of them:

```
  ┌─────────────────────────────────────────────────────────────────┐
  │  SOURCE FOLDER                                                  │
  │                                                                 │
  │  1. SCAN      Walk source tree, find candidate album folders    │
  │               8 parallel workers, tag cache for warm runs       │
  │                                                                 │
  │  2. READ      mutagen reads audio tags from every track         │
  │               Cache keyed by (path, mtime) — zero cost if       │
  │               file unchanged                                    │
  │                                                                 │
  │  3. VOTE      Plurality vote across all tags in the folder      │
  │               Winner: most common non-empty value per field     │
  │               Ties broken by track count                        │
  │                                                                 │
  │  4. SCORE     7-factor confidence score (0.0 → 1.0)            │
  │               dominance · coverage · title quality · gaps       │
  │               filename consistency · albumartist · alignment    │
  │                                                                 │
  │  5. ROUTE     score ≥ threshold → Clean/                        │
  │               score < threshold → Review/                       │
  │               name collision → Duplicates/                      │
  │                                                                 │
  │  6. MOVE      Same filesystem → atomic os.rename() (~1ms)       │
  │               Cross-device → copy → verify → delete             │
  │               Track rename → 01 - Title.flac pattern            │
  └─────────────────────────────────────────────────────────────────┘
```

---

## Debug a single folder

```bash
raagdosa show "/Music/Incoming/some strange folder" --tracks
```

```
══════════════════════════════════════════════════════════════════
raagdosa show — DJ Shadow - Endtroducing..... (1996)
══════════════════════════════════════════════════════════════════

Source:   /Music/Incoming/DJ Shadow - Endtroducing (1996)
Files:    16 × .flac

Tag votes
  album          "Endtroducing....."  16/16  ████████████████  ✓
  albumartist    "DJ Shadow"          16/16  ████████████████  ✓
  artist         "DJ Shadow"          16/16  ████████████████  ✓
  year           "1996"               15/16  ███████████████░  ✓

Confidence  0.94  ██████████████████░░  → Clean/

  dominance         0.97  ████████████████████  album+artist fully consistent
  tag_coverage      1.00  ████████████████████  all tracks tagged
  title_quality     0.94  ██████████████████░░  all titles look real
  completeness      1.00  ████████████████████  no track gaps or dupes
  filename_consist  0.88  █████████████████░░░  some tracks have noisy filenames
  aa_consistency    1.00  ████████████████████
  folder_alignment  0.91  ██████████████████░░  source name close to proposed

Proposed:   DJ Shadow - Endtroducing..... (1996)
Routing:    ✦ clean → Clean/Albums/DJ Shadow/Endtroducing..... (1996)/

Track renames (--tracks)
  01 Stem And a Hum.flac             → 01 - Stem And a Hum.flac
  02 Building Steam with a Grain.flac → 02 - Building Steam with a Grain of Salt.flac
  ...
══════════════════════════════════════════════════════════════════
```

---

## Folder routing

| Condition | Destination |
|-----------|-------------|
| Confidence ≥ threshold, not a duplicate | `Clean/Albums/Artist/Album/` |
| Detected as DJ mix / chart folder | `Clean/_Mixes/` |
| Confidence < threshold | `Review/Albums/` |
| Same proposed name appears twice in this run | `Review/Duplicates/` |
| Exists in Clean AND incoming has missing tracks | Tracks merged into existing folder |
| Exists in Clean as MP3, incoming is FLAC | `Clean/Albums/Artist/FLAC/Album/` (if `flac_segregation: true`) |
| Exists in Clean as FLAC, incoming is MP3 | `Review/Duplicates/` tagged `lower_quality_mp3` |
| Already exists in Clean (exact or partial) | `Review/Duplicates/` |
| Tags absent, folder name used as fallback | `Review/Albums/` |
| Too many unreadable files | `Review/Albums/` |
| Non-audio junk (`.nfo`, `.log`, `.png`, etc.) | `Review/Artifacts/<album>/` |

Review is a **holding area**, not a bin. Nothing is ever deleted.

---

## Performance

RaagDosa is fast even on large libraries:

| Library | Old (sequential) | v3.81 (same drive) | v3.81 (first scan) | v3.81 (warm cache) |
|---------|-----------------|-------------------|-------------------|-------------------|
| 600 tracks | ~50s | **<1s** | ~2s | **<1s** |
| 6k tracks | ~8m | **<2s** | ~8s | **<1s** |
| 60k tracks | ~90m | **<5s** | ~25s | **~2s** |

**Same-drive moves** use `os.rename()` (atomic, ~1ms per folder) instead of copy+verify+delete.  
**Parallel scan** runs 8 concurrent workers reading tags — ~7× faster than sequential.  
**Tag cache** persists tag data between runs keyed by `(path, mtime)` — warm scans cost almost nothing.  
**Streaming pipeline** starts moving folders immediately rather than waiting for the full scan to complete.

---

## Configuration

RaagDosa is configured through a single `config.yaml`. Key sections:

### Confidence threshold

```yaml
review_rules:
  min_confidence_for_clean: 0.85  # below this → Review/
```

### Artist normalisation — fix "Jay-Z" vs "Jay Z" vs "JAYZ"

```yaml
artist_normalization:
  the_prefix: keep-front      # keep-front | move-to-end | strip

  aliases:
    "jay z":   "Jay-Z"
    "jayz":    "Jay-Z"
    "mos def": "Yasiin Bey"
    "beatles": "The Beatles"
```

### Library template

```yaml
library:
  template: "{artist}/{album}"    # how Clean/ is structured
  flac_segregation: false         # true → Artist/FLAC/Album/ for archival masters
```

### Per-folder override

Drop a `.raagdosa` file inside any folder to override detection:

```yaml
# .raagdosa
album: Correct Album Name
artist: Correct Artist
year: 1998
confidence_boost: 0.15
```

### Performance tier

```yaml
performance:
  tier: medium    # slow | medium | fast | ultra
  # Individual overrides (optional):
  # workers: 4
  # sleep_between_moves_ms: 0
```

### Artifact handling

```yaml
artifacts:
  enabled: true
  keep_extensions: [.jpg, .jpeg, .pdf, .cue]
  quarantine_extensions: [.png, .nfo, .sfv, .txt, .url, .log, .m3u, .m3u8]
  quarantine_folder: Review/Artifacts

### Duplicate resolution

```yaml
duplicates:
  compare_before_routing: true   # compare contents before routing to Duplicates
  merge_missing_tracks: true     # auto-copy missing tracks into existing folder
  flac_mp3_coexistence: keep_both
```

---

## Track naming

| Folder type | Pattern |
|-------------|---------|
| Standard album | `01 - Title.ext` |
| Multi-disc album | `1-01 - Title.ext` |
| Various Artists | `01 - Artist - Title.ext` |
| EP (3–6 tracks) | Folder labelled `[EP]`, tracks use album pattern |
| Mixed bag | `Artist - Title.ext` |

Title cleanup removes: domains (`free-mp3s.net`), upload watermarks, bitrate tags (`320kbps`), and YouTube suffixes (`[Official Audio]`, `(Lyrics Video)`) — while preserving meaningful DJ suffixes (`Original Mix`, `Extended`, `Dub`, `Remix`).

---

## DJ workflow

> **Before overwriting your library:** RaagDosa will warn you if it detects Rekordbox or Serato database files. Moving or renaming files will break existing cue points, waveforms, and beatgrids.

**Recommended approach:**
1. Run RaagDosa to clean your library structure
2. Re-import the `Clean/` folder into Rekordbox or Serato as a new collection
3. Re-analyse — you get fresh, accurate waveforms for the clean copies

This is intentional: you're building a clean foundation, not patching the old one.

**FLAC segregation** (`library.flac_segregation: true`) keeps archival masters under `Artist/FLAC/Album/` and MP3 working copies under `Artist/Album/` — physically separate, no filename collisions.

---

## Safety

| Mechanism | What it does |
|-----------|-------------|
| **Copy-verify-delete** | Files copied, count+size verified, source deleted only on success. No raw `mv`. |
| **Same-fs fast path** | Uses atomic `os.rename()` where possible — if it fails, no partial state. |
| **Manifest** | Every Clean folder recorded in `logs/clean_manifest.json`. Used for cross-run dedup and `verify`. |
| **History log** | Append-only `logs/history.jsonl`. Full undo by session, action ID, or original path. |
| **Disk space check** | Aborts if destination has less than 110% of source size available. |
| **Path validation** | Proposed destinations validated against allowed roots before any move. |
| **Stop handling** | Ctrl+C once = graceful stop after current folder. Ctrl+C twice = force quit. `resume <session_id>` continues from where it left off. |

---

## All commands

```
Setup & health
  init                         Interactive setup wizard → config.yaml
  doctor                       Validate config, check deps, disk space, DJ databases

Core workflow
  go / run                     Full pipeline: scan → move folders → rename tracks
  go --dry-run                 Preview without moving anything
  go --interactive             Confirm each folder before moving
  go --since last_run          Only process folders added since last run
  folders                      Folder move pass only (no track rename)
  tracks                       Track rename pass only (inside Clean/)
  scan                         Generate proposals without applying
  apply [file]                 Execute from a proposals.json
  resume <session_id>          Continue an interrupted session

Inspection
  show <path>                  Full debug breakdown for one folder
  show <path> --tracks         Also preview per-track renames
  status                       Library overview: counts, disk, pending

Auditing
  verify                       Deep audit of Clean/ vs manifest
  clean-report                 Stats and format breakdown for Clean library
  orphans                      Find loose audio files without album folders
  review-list                  Tabular view of Review folder contents
  review-list --older-than 30  Only show folders waiting >30 days
  diff last prev               Compare two session reports

Artist & library tools
  artists --list               List every artist in Clean/ with counts
  artists --find "portis"      Fuzzy-search for an artist
  extract <path> --by-artist   Split VA/mix folder into per-artist groups
  compare --folder A B         Diff two folders (tracks, tags)

Sessions & history
  report                       View last session (txt / csv / html)
  report --session <id>        View specific session
  history                      Recent action log
  undo --session <id>          Reverse all moves from a session
  undo --tracks --folder <p>   Reverse track renames inside one folder

Maintenance
  cache                        Tag cache status (entries, size, last saved)
  cache clear                  Force full re-read on next scan
  cache evict                  Remove stale entries for missing files
  learn                        Analyse Review patterns, suggest config improvements

Profiles
  profile list / show / add / set / use / delete
```

---

## Project links

- **Documentation:** [RaagDosa-Commands.md](https://github.com/YOUR_USERNAME/raagdosa/blob/main/RaagDosa-Commands.md)
- **Changelog:** [CHANGELOG.md](https://github.com/YOUR_USERNAME/raagdosa/blob/main/CHANGELOG.md)
- **Issues:** [GitHub Issues](https://github.com/YOUR_USERNAME/raagdosa/issues)
- **License:** [MIT](LICENSE)

---

<div align="center">

Made with strong opinions about folder structure.

</div>
