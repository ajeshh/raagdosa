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

[![Version](https://img.shields.io/badge/version-6.0.0-brightgreen?style=flat-square&color=0d1117&labelColor=21262d)](CHANGELOG.md)
[![Python versions](https://img.shields.io/pypi/pyversions/raagdosa?style=flat-square&color=0d1117&labelColor=21262d&logo=python&logoColor=f5f5f5)](https://pypi.org/project/raagdosa/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue?style=flat-square&color=0d1117&labelColor=21262d)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen?style=flat-square&color=0d1117&labelColor=21262d&logo=github-actions&logoColor=f5f5f5)](https://github.com/raagdosa/raagdosa/actions)

</div>

---

RaagDosa is **Calibre for DJs** — a local-first, CLI-driven tool that transforms a chaotic music folder into a clean, coherent library structure you can trust. It reads your audio file tags, votes across all tracks to find consensus metadata, assigns a confidence score, and routes each album to `Clean/` or `Review/` accordingly.

**The ID3/Vorbis/AAC tag is the source of truth.** The folder name supplements when tags are missing — it never overrides them.

It never touches your source. Every action is sessioned, logged, and fully undoable.

```
raagdosa go --dry-run     # see exactly what would happen
raagdosa go               # do it
raagdosa undo --session last  # change your mind
```

---

## What it looks like

```
Session:   2026-03-08_14-30-00_a3f1
Pipeline:  streaming batches of 50, 4 scan workers

Scanning [██████████████████████] 833/833 100%  42/s  ~0s  Portishead - Dummy

  MOVED ✦ clean   Massive Attack - Mezzanine (1998)                conf=0.97
  MOVED ✦ clean   Portishead - Dummy (1994)                        conf=0.95
  MOVED ◐ review  Unknown Artist - 2024-03-01                      conf=0.52  [low_confidence, heuristic_fallback]
  MOVED ✦ clean   The Prodigy - Music for the Jilted Generation (1994)  conf=0.91
  MOVED ◈ dupes   Massive Attack - Mezzanine (1998)                conf=0.97  [duplicate_in_run]
  MOVED ✦ clean   DJ Shadow - Endtroducing..... (1996)             conf=0.94
  MOVED ✦ clean   Björk - Homogenic (1997)                         conf=0.93

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
git clone https://github.com/raagdosa/raagdosa
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

On subsequent runs:

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
  │               Parallel workers, tag cache for warm runs         │
  │                                                                 │
  │  2. READ      mutagen reads every tag from every track          │
  │               genre, compilation, grouping, comment, label,     │
  │               bpm, key — all used as classification signals      │
  │                                                                 │
  │  3. VOTE      Plurality vote across all tags in the folder      │
  │               Tags win over folder name. Folder name used       │
  │               only when tags are absent or incomplete.          │
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

## Tag-first philosophy

RaagDosa reads every audio tag it can find — not just artist and album, but `compilation` (iTunes TCMP flag), `genre`, `grouping`, `comment`, `label`, `bpm`, and `key`. These all feed the classification engine:

| Tag | How it's used |
|-----|---------------|
| `compilation` / `TCMP` | If majority of tracks are flagged, folder → VA |
| `genre` | "EP" or "Single" in genre tag → EP/Single classification |
| `grouping` | EP/Single signals from grouping field |
| `comment` | Year fallback — scans comment text for 4-digit year when year tag is missing |
| `label` | Detects label-as-albumartist contamination; `{label}` template token for label-first folder structures |
| `albumartist` | Primary artist for folder naming and VA detection |
| `artist` | Per-track artist for VA ratio calculation |
| `bpm` | `{bpm_range}` template token — buckets into named zones (House, Techno, D&B) or numeric ranges |
| `key` | `{camelot_key}` template token — converts to Camelot notation (1A–12B) for harmonic mixing folders |
| `isrc` | Available for future deduplication |

When both a tag and the folder name agree on a value, confidence is boosted. When they disagree, the tag wins but confidence is reduced — potentially routing to Review.

**Year recovery chain:** ID3 date/year tags → comment tag year → folder name year. Each fallback level penalises confidence proportionally.

---

## Folder name cleaning

Before any parsing, folder names pass through a 28-step pre-processor that strips noise patterns confirmed in real library data (968 folders, 13,588 files analysed):

```
13th_Ward_Social_Club-Afrobeat_Vol_1-WEB-2023-FTD  →  13th Ward Social Club - Afrobeat Vol 1
bonobo - black sands remixed [zencd178] 2012 cd 320  →  bonobo - black sands remixed
aukai.  [2016] aukai                                 →  aukai. - aukai
Www.ElectronicFresh.Com - Artist - Title             →  Artist - Title
[www.freestep.net] Artist - Title                    →  Artist - Title
flying lotus - los angeles                           →  Flying Lotus - Los Angeles
Tropical Twista Records - 2024 - Cigarra - Limbica   →  Cigarra - 2024 - Limbica
```

Noise stripped: scene release group suffixes (`-WEB-2023-FTD`), format brackets/parens (`[MP3]`, `( FLAC )`), catalog codes (`[zencd178]`), duplicate years, label-year-artist-album 4-dash slugs, double-dash slugs, website domains and URLs (all positions, any case), promo watermarks, hash/checksum tails.

All-lowercase folder names (~10% of real libraries) are automatically rescued to Smart Title Case — with DJ/EP/VA/UK acronyms preserved.

---

## Debug a single folder

```bash
raagdosa show "~/Music/Incoming/some folder" --tracks
```

```
══════════════════════════════════════════════════════════════════
raagdosa show — DJ Shadow - Endtroducing..... (1996)
══════════════════════════════════════════════════════════════════

Source:   ~/Music/Incoming/DJ Shadow - Endtroducing (1996)
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
══════════════════════════════════════════════════════════════════
```

---

## Folder routing

| Condition | Destination |
|-----------|-------------|
| Confidence ≥ threshold, not a duplicate | `Clean/Albums/Artist/Album/` |
| Detected as DJ mix / chart / playlist | `Clean/_Mixes/` |
| Detected as EP (3–6 tracks or "EP" in name) | `Clean/Albums/Artist/Album (EP)/` |
| Single (1–2 tracks or "Single" in name) | `Clean/_Singles/` |
| Confidence < threshold | `Review/Albums/` |
| Same proposed name appears twice in this run | `Review/Duplicates/` |
| Already exists in Clean (manifest or disk) | `Review/Duplicates/` |
| Tags absent, folder name used as fallback | `Review/Albums/` |

Review is a **holding area**, not a bin. Nothing is ever deleted.

---

## Library profiles & templates

Profiles let you run RaagDosa against different source folders with different settings. Each profile can use a different **library template** to control how albums land in your Clean folder.

```bash
raagdosa template list              # see all 9 built-in templates
raagdosa template show genre-bpm    # full details + example tree
raagdosa profile add vinyl --source ~/Music/Vinyl --template dated
raagdosa profile set default --template genre
```

### Built-in templates

| ID | Pattern | Best for |
|----|---------|----------|
| `standard` | `{artist}/{album}` | Safe default for any collection |
| `dated` | `{artist}/{year} - {album}` | Chronological discography view |
| `flat` | `{artist} - {album}` | Minimal depth, fast browsing |
| `genre` | `{genre}/{artist}/{album}` | Large multi-genre collections |
| `decade` | `{decade}/{genre}/{artist} - {album}` | Era-first browsing |
| `bpm` | `{bpm_range}/{artist} - {album}` | Tempo-first for single-genre DJs |
| `genre-bpm` | `{genre}/{bpm_range}/{artist} - {album}` | Open-format DJ structure |
| `genre-bpm-key` | `{genre}/{bpm_range}/{camelot_key}/{artist} - {album}` | Harmonic mixing structure |
| `label` | `{label}/{artist} - {album}` | Label-focused collectors |

### Template tokens

| Token | Source | Fallback |
|-------|--------|----------|
| `{artist}` | Voted albumartist tag | `_Unknown` |
| `{album}` | Voted album tag | `_Untitled` |
| `{year}` | Year tag or folder name | empty |
| `{album_year}` | `Album (Year)` combined | album only |
| `{genre}` | Voted genre tag, normalised via `genre_map` | `_Unsorted` |
| `{decade}` | Derived from year (e.g. `1990s`) | `_Unknown Era` |
| `{bpm_range}` | Median BPM bucketed into named zones or numeric ranges | `_Unknown BPM` |
| `{camelot_key}` | Voted key tag converted to Camelot notation (1A–12B) | `_Unknown Key` |
| `{label}` | Voted label tag, corporate suffixes stripped | `_Unknown Label` |

All fallback labels are configurable in `config.yaml` under `library:`.

### Per-profile library overrides

Each profile can override the global `library:` block:

```yaml
profiles:
  default:
    source_root: ~/Music/Incoming
    library:
      template: "{artist}/{album}"

  dj-usb:
    source_root: ~/Music/DJ-Prep
    library:
      template: "{genre}/{bpm_range}/{artist} - {album}"
      genre_fallback: "_Unsorted"
      bpm_fallback: "_Unknown BPM"
```

### BPM buckets

BPM bucketing is configurable. Named zones are checked first; unmatched BPMs fall to numeric ranges:

```yaml
bpm_buckets:
  width: 10                 # 120-129, 130-139, etc.
  named_zones:
    "Downtempo":    [60, 99]
    "House":        [120, 132]
    "Techno":       [133, 145]
    "D&B / Jungle": [160, 180]
```

### Genre normalisation

A 150+ entry `genre_map` in config.yaml maps raw genre tags to canonical folder names:

```yaml
genre_map:
  "Electronica":     "Electronic"
  "Deep House":      "House"
  "Boom Bap":        "Hip-Hop"
  "Nu Jazz":         "Jazz"
  # ... 150+ entries covering all major genres
```

### Tag coverage report

After a scan, RaagDosa shows how well your library's tags support your chosen template:

```
Tag coverage for template: {genre}/{bpm_range}/{artist} - {album}
  genre      ████████████████████  92%
  bpm        ████████░░░░░░░░░░░░  41%
  artist     ████████████████████  99%
```

---

## Performance

| Library | First scan | Warm scan (cached) | Same-drive apply |
|---------|------------|--------------------|------------------|
| 600 tracks | ~2s | <1s | ~0s |
| 6k tracks | ~8s | <1s | <1s |
| 60k tracks | ~25s | ~2s | <5s |

**Same-drive moves** use `os.rename()` (atomic, ~1ms per folder regardless of album size).
**Parallel scan** runs concurrent workers reading tags — ~7× faster than sequential.
**Tag cache** persists tag data between runs keyed by `(path, mtime)` — warm scans cost almost nothing.
**Streaming pipeline** starts moving folders immediately rather than waiting for the full scan.

---

## Configuration

RaagDosa is configured through a single `config.yaml` split into two zones.

### SETTINGS — operational behaviour

```yaml
review_rules:
  min_confidence_for_clean: 0.85  # below this → Review/

artist_normalization:
  the_prefix: keep-front          # keep-front | move-to-end | strip
  fuzzy_dedup_threshold: 0.92     # Jaccard threshold for artist matching

title_cleanup:
  strip_trailing_domains: true    # strip www.site.com from titles/albums
  strip_trailing_phrases:
    - "free download"
    - "official video"
    - "ncs release"
    # ... full list in config.yaml
```

### BRAIN — learned knowledge about your library

```yaml
brain:
  artist_aliases:
    # Diacritics — fuzzy matching handles Björk↔Bjork automatically
    # but aliases control the *display form* used in folder names:
    "bjork":         "Björk"
    "sigur ros":     "Sigur Rós"
    "mo":            "MØ"
    "jay z":         "Jay-Z"
    "mos def":       "Yasiin Bey"

  known_labels:
    # Record labels to detect in albumartist tags (prevents
    # "Cosmovision Records - Album" being used as folder name)
    - "Cosmovision Records"
    - "Tropical Twista Records"

  va_rescue_prefixes:
    # Artist prefixes that should never be classified as VA
    - "sven wunder"
    - "blend mishkin"
```

When `brain:` grows large, move it to `raagdosa-brain.yaml` and add `brain_file: raagdosa-brain.yaml` in config.

### Per-folder override

Drop a `.raagdosa` file inside any folder to override detection:

```yaml
# .raagdosa
album: Correct Album Name
artist: Correct Artist
year: 1998
confidence_boost: 0.15
```

---

## Track naming

| Folder type | Pattern |
|-------------|---------|
| Standard album | `01 - Title.ext` |
| Multi-disc album | `1-01 - Title.ext` |
| Various Artists | `01 - Artist - Title.ext` |
| EP | Folder labelled `(EP)`, tracks use album pattern |
| Single | Routed to `_Singles/`, track number stripped |
| Mixed bag | `Artist - Title.ext` |

Supported source filename formats: `01 - Title`, `01. Title`, `02. - Title`, `01-slug-title`, `001/12 Title`, `D-NN Title`, `A1 - Title`, and 4-part `Artist - Album - 01 - Title`.

Title cleanup removes: domains (`free-mp3s.net`), upload watermarks, bitrate tags (`320kbps`), promo labels (`NCS Release`, `EDM Sauce`), and YouTube suffixes (`[Official Audio]`) — while preserving meaningful suffixes (`Original Mix`, `Extended`, `Dub`, `Remix`, `feat.`).

---

## Artist matching and diacritics

RaagDosa uses a 4-step fuzzy artist matching pipeline:

1. Exact Unicode NFC match
2. "The"-prefix strip then exact match
3. ASCII-fold comparison — `Björk ↔ Bjork`, `MØ ↔ MO`, `Sigur Rós ↔ Sigur Ros`
4. Jaccard word-set similarity ≥ 0.92

This means tags and folder names with diacritic variants are always matched and deduplicated correctly. Use `brain.artist_aliases` to control which display form appears in the final folder name.

---

## DJ workflow

> **Before overwriting your library:** RaagDosa will warn you if it detects Rekordbox or Serato database files. Moving or renaming files will break existing cue points, waveforms, and beatgrids.

**Recommended approach:**
1. Run RaagDosa to clean your library structure
2. Re-import the `Clean/` folder into Rekordbox or Serato as a new collection
3. Re-analyse — you get fresh, accurate waveforms for the clean copies

**FLAC segregation** (`library.flac_segregation: true`) keeps archival masters under `Artist/FLAC/Album/` and MP3 working copies under `Artist/Album/`.

---

## Safety

| Mechanism | What it does |
|-----------|-------------|
| **Copy-verify-delete** | Files copied, count+size verified, source deleted only on success |
| **Same-fs fast path** | Atomic `os.rename()` — if it fails, no partial state |
| **Manifest** | Every Clean folder recorded in `logs/clean_manifest.json` for cross-run dedup |
| **History log** | Append-only `logs/history.jsonl`. Full undo by session, action ID, or path |
| **Disk space check** | Aborts if destination has less than 110% of source size available |
| **Path validation** | Anti-traversal check on all target paths before any move |
| **Stop handling** | Ctrl+C once = graceful stop after current folder. `resume` continues from checkpoint |

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

Profiles & templates
  profile list / show / add / set / use / delete
  template list                  Show all 9 built-in library templates
  template show <id>             Full details, required tags, example tree
```

---

## Project links

- **Documentation:** [RaagDosa-Commands.md](https://github.com/raagdosa/raagdosa/blob/main/RaagDosa-Commands.md)
- **Changelog:** [CHANGELOG.md](https://github.com/raagdosa/raagdosa/blob/main/CHANGELOG.md)
- **Design document:** [RaagDosa_v5_Design.docx](https://github.com/raagdosa/raagdosa/blob/main/RaagDosa_v5_Design.docx)
- **Issues:** [GitHub Issues](https://github.com/raagdosa/raagdosa/issues)
- **License:** [MIT](LICENSE)

---

<div align="center">

Made with strong opinions about folder structure.

</div>
