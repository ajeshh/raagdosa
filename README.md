<div align="center">

```
██████╗  █████╗  █████╗  ██████╗ ██████╗  ██████╗ ███████╗ █████╗
██╔══██╗██╔══██╗██╔══██╗██╔════╝ ██╔══██╗██╔═══██╗██╔════╝██╔══██╗
██████╔╝███████║███████║██║  ███╗██║  ██║██║   ██║███████╗███████║
██╔══██╗██╔══██║██╔══██║██║   ██║██║  ██║██║   ██║╚════██║██╔══██║
██║  ██║██║  ██║██║  ██║╚██████╔╝██████╔╝╚██████╔╝███████║██║  ██║
╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═════╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝
```

**A tag-aware folder organiser for DJs and music collectors.**

[![Version](https://img.shields.io/badge/version-8.0.0-brightgreen?style=flat-square&color=0d1117&labelColor=21262d)](CHANGELOG.md)
[![Python versions](https://img.shields.io/pypi/pyversions/raagdosa?style=flat-square&color=0d1117&labelColor=21262d&logo=python&logoColor=f5f5f5)](https://pypi.org/project/raagdosa/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue?style=flat-square&color=0d1117&labelColor=21262d)](LICENSE)

</div>

---

You have a downloads folder. Or three. Files named `dj_shadow--endtroducing-WEB-1996-FTD`, folders with no tags, folders where every track has a different artist. RaagDosa reads the tags inside each file, groups them into albums, scores how well-tagged each one is, and sorts them into `Clean/` or `Review/` — without touching your originals.

```
Session:  2026-03-09_14-22_incoming
Scanning: ~/Music/Incoming  (247 folders)

[████████████████████] 100%  38/s  threshold=0.85

  ✦ clean  Massive Attack - Mezzanine (1998)            conf=0.97
  ✦ clean  Portishead - Dummy (1994)                    conf=0.95
  ✦ clean  DJ Shadow - Endtroducing..... (1996)         conf=0.94
  ✦ clean  Aphex Twin - Selected Ambient Works (1992)   conf=0.91
  ◐ review Unknown Artist - 2024-03-01                  conf=0.52  [low_confidence]
  ◈ dupes  Massive Attack - Mezzanine (1998)            conf=0.97  [duplicate_in_run]

Results:  247 folders  ·  Clean: 201  ·  Review: 40  ·  Dupes: 6
Undo:     raagdosa undo --session last
```

Every run is logged and fully undoable. Your source folder is never modified.

> **Beta.** This tool moves files. Run `--dry-run` first. Point it at a test folder before your real library.

---

## Who it's for

You're a DJ or serious music collector with a large, inconsistently organised library. Your downloads folder has thousands of files in a mix of folder naming conventions, some with good tags, some with none. You want a structured folder layout you can actually navigate — without manually renaming hundreds of folders.

RaagDosa is a CLI tool that runs locally, on your machine, on your files.

---

## Install

```bash
pip install raagdosa
```

**Requires:** Python 3.9+ · macOS, Linux, or Windows (including NTFS drives)

Or run from source:

```bash
git clone https://github.com/raagdosa/raagdosa
cd raagdosa
pip install -e .
```

---

## Quick start

```bash
# 1. Set up config (one-time wizard)
raagdosa init

# 2. Preview — nothing moves
raagdosa go --dry-run

# 3. Run it
raagdosa go

# 4. Change your mind
raagdosa undo --session last
```

**→ New to RaagDosa? Start with the [User Guide](guide/GUIDE.md), which includes a step-by-step [Quick Start](guide/GUIDE.md#quick-start).**

---

## How it works

RaagDosa runs the same pipeline on every folder in your source:

```
SCAN → READ TAGS → VOTE → SCORE → ROUTE → MOVE
```

It reads every audio tag in every file — artist, album, year, genre, BPM, key, label — and runs a plurality vote across all tracks in the folder to find consensus metadata. A 7-factor confidence score (0.0–1.0) determines the route: above threshold goes to `Clean/`, below goes to `Review/`.

`Review/` is a holding area, not a bin. Fix the tags in your tag editor, re-run, and those folders promote to `Clean/` on the next pass. Nothing in `Review/` is ever deleted or modified.

When the tool is confident, it moves. When it isn't, it asks. The triage dashboard shows you the split before a single file moves:

```
  AUTO tier   (conf ≥ 0.85)   201 folders  →  Clean/
  HOLD tier   (conf < 0.85)    40 folders  →  needs review

  [a] Bulk-approve AUTO + review HOLD   [r] Review all   [q] Quit
```

---

## What RaagDosa does not do

- Does not modify audio tags — tags are read-only input
- Does not transcode or convert files
- Does not connect to Discogs, MusicBrainz, or any external service
- Does not delete anything, ever
- Does not touch your source folder — it only copies or renames to the destination

---

## Folder name cleaning

Before parsing, folder names pass through a noise-stripping pipeline. Some examples:

| Input | Cleaned |
|-------|---------|
| `Aphex_Twin-Selected_Ambient_Works_Vol2-WEB-1992-FTD` | `Aphex Twin - Selected Ambient Works Vol 2` |
| `bonobo - black sands remixed [zencd178] 2012 cd 320` | `Bonobo - Black Sands Remixed` |
| `[www.mp3-scene.net] Daft Punk - Homework` | `Daft Punk - Homework` |
| `talking heads  [2001] remain in light` | `Talking Heads - Remain in Light` |
| `flying lotus - los angeles (2008)` | `Flying Lotus - Los Angeles (2008)` |
| `Cosmovision Records - 2019 - Cigarra - Limbica` | `Cigarra - Limbica` |

Noise stripped: scene release suffixes, format and quality brackets, catalog codes, website domains, duplicate years, label-year-artist-album slugs, and promo watermarks. All-lowercase folder names are rescued to Smart Title Case.

---

## Confidence score

Each folder is scored on 7 factors. The weighted total determines its route.

| Factor | Weight | What it measures |
|--------|--------|-----------------|
| `dominance` | 0.40 | Fraction of tracks that agree on album + artist |
| `tag_coverage` | 0.15 | Fraction of tracks with all key tags present |
| `title_quality` | 0.12 | Track titles look like real titles, not garbage |
| `completeness` | 0.12 | No duplicate or missing track numbers |
| `filename_consist` | 0.10 | Filenames match tag content |
| `aa_consistency` | 0.06 | Consistent albumartist across all tracks |
| `folder_alignment` | 0.08 | Source folder name matches proposed clean name |

Low on a factor? `raagdosa show <path>` shows you exactly which ones and by how much.

---

## Library structure

The default output layout:

```
~/Music/Incoming/raagdosa/
  Clean/
    Albums/
      DJ Shadow/
        Endtroducing..... (1996)/
      Massive Attack/
        Mezzanine (1998)/
    _Mixes/
    _Singles/
  Review/
    Albums/
    Duplicates/
```

9 built-in templates let you organise by genre, BPM, decade, Camelot key, or label instead. [See library templates →](guide/GUIDE.md#library-templates)

---

## Key commands

```
raagdosa go                    Triage dashboard → move
raagdosa go --dry-run          Preview without moving
raagdosa go --interactive      Review every folder 1-by-1
raagdosa show <path>           Full debug breakdown for one folder
raagdosa undo --session last   Reverse the last run
raagdosa sessions              List recent sessions
raagdosa doctor                Validate config, check deps
```

Full command reference: [RaagDosa-Commands.md](RaagDosa-Commands.md)

---

## Safety

Every file move is logged to `logs/history.jsonl`. Undo is always available by session, action ID, or path. Same-filesystem moves use atomic `os.rename()` — if it fails, there is no partial state. Cross-filesystem moves copy, verify, then delete.

---

## Project links

- **User Guide:** [guide/GUIDE.md](guide/GUIDE.md)
- **Commands reference:** [RaagDosa-Commands.md](RaagDosa-Commands.md)
- **Changelog:** [CHANGELOG.md](CHANGELOG.md)
- **Issues:** [GitHub Issues](https://github.com/ajeshh/raagdosa/issues)
- **License:** [MIT](LICENSE)

---

<div align="center">

Made with strong opinions about folder structure.

<br>

[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20me%20a%20coffee-support-yellow?style=flat-square&logo=buy-me-a-coffee&logoColor=white)](https://buymeacoffee.com/adaajio)

</div>
