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

[![Version](https://img.shields.io/badge/version-10.0.0-brightgreen?style=flat-square&color=0d1117&labelColor=21262d)](CHANGELOG.md)
[![Python versions](https://img.shields.io/pypi/pyversions/raagdosa?style=flat-square&color=0d1117&labelColor=21262d&logo=python&logoColor=f5f5f5)](https://pypi.org/project/raagdosa/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue?style=flat-square&color=0d1117&labelColor=21262d)](LICENSE)

</div>

---

You have a downloads folder. Or three. Files named `dj_shadow--endtroducing-WEB-1996-FTD`, folders with no tags, folders where every track has a different artist. RaagDosa reads the tags inside each file, groups them into albums, scores how well-tagged each one is, and sorts them into `Clean/` or `Review/` — without touching your originals.

```
raagdosa go --dry-run          # see exactly what would happen
raagdosa go                    # do it
raagdosa undo --session last   # change your mind
```

Every run is logged and fully undoable. Your source folder is never modified.

> **Beta.** This tool moves files. Run `--dry-run` first. Point it at a test folder before your real library.

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
raagdosa init              # guided setup (one-time wizard)
raagdosa go --dry-run      # preview — nothing moves
raagdosa go                # do it
raagdosa undo --session last   # change your mind
```

**New to RaagDosa?** Start with the [User Guide](guide/GUIDE.md).

---

## How it works

```
SCAN → READ TAGS → VOTE → SCORE → ROUTE → MOVE
```

It reads every audio tag in every file — artist, album, year, genre, BPM, key, label — and runs a plurality vote across all tracks in the folder to find consensus metadata. A 7-factor confidence score (0.0–1.0) determines the route: above threshold goes to `Clean/`, below goes to `Review/`.

`Review/` is a holding area, not a bin. Fix the tags, re-run, and those folders promote to `Clean/`.

---

## What RaagDosa does not do

- Does not modify audio tags during folder organising — tags are read-only input for the move pipeline. The separate [tag fix workflow](guide/GUIDE.md#tag-fixing) can write tag corrections, but only when you explicitly review and approve each change.
- Does not transcode or convert files
- Does not connect to Discogs, MusicBrainz, or any external service
- Does not delete anything, ever
- Does not touch your source folder

---

## Privacy

RaagDosa runs entirely on your machine. It does not phone home, collect telemetry, upload usage data, or connect to any external service. Your music library stays yours — no accounts, no analytics, no cloud. Just your files, your tags, your folders.

---

## Safety

Every file move is logged to `logs/history.jsonl`. Undo is always available by session, action ID, or path. Same-filesystem moves use atomic `os.rename()` — if it fails, there is no partial state. Cross-filesystem moves copy, verify, then delete.

---

## Documentation

| Guide | What it covers |
|-------|---------------|
| [User Guide](guide/GUIDE.md) | Setup, triage, interactive review, DJ crates, profiles, templates, scoring, config, sessions, undo, tag fixing |
| [Commands Reference](RaagDosa-Commands.md) | Every command and flag |
| [Changelog](CHANGELOG.md) | Release history |

---

## Project links

- **Issues:** [GitHub Issues](https://github.com/ajeshh/raagdosa/issues)
- **License:** [MIT](LICENSE)

---

<div align="center">

Made with strong opinions about folder structure.

If RaagDosa is saving you time, [let me know](https://github.com/ajeshh/raagdosa/issues) — or buy me a coffee so I know it's resonating.

<br>

[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20me%20a%20coffee-support-yellow?style=flat-square&logo=buy-me-a-coffee&logoColor=white)](https://buymeacoffee.com/adaajio)

</div>
