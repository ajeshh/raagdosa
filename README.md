# 🍛 RaagDosa

**Deterministic music library cleanup for DJs and collectors.**  
CLI-first · safe-by-default · fully undoable · no cloud · no lock-in.

---

RaagDosa takes a messy music folder — years of downloads, rips, and DJ sets dumped into one place — and turns it into a clean, coherent structure you can trust. It reads your tags, votes on the most consistent metadata, proposes a destination, and only moves files when you say go.

It never touches your source. Everything is logged. Everything is undoable.

---

## What it does

1. **Scans** a source folder and reads audio tags from every candidate album folder
2. **Votes** on the dominant album, albumartist, and year across all tracks
3. **Proposes** a normalised folder name and destination path
4. **Routes** each folder to `Clean/`, `Review/`, or `Duplicates/` based on confidence
5. **Moves** using copy-verify-delete (never a raw `mv`) — safe by design
6. **Renames** tracks inside Clean folders to a consistent pattern
7. **Logs** every action to a manifest and history file for full traceability

---

## Quick start

### Install

```bash
pip install raagdosa
```

Or from source:

```bash
git clone https://github.com/YOUR_USERNAME/raagdosa
cd raagdosa
pip install -e .
```

**Requirements:** Python 3.9+ · [mutagen](https://pypi.org/project/mutagen/) · [pyyaml](https://pypi.org/project/PyYAML/)

### First-time setup

```bash
raagdosa init
```

This runs an interactive wizard that asks for your source folder, library structure preference, and "The" prefix policy, then writes a ready-to-use `config.yaml`.

### Verify your setup

```bash
raagdosa doctor
```

Checks dependencies, config validity, source folder, disk space, and DJ database detection.

### Preview (nothing moves)

```bash
raagdosa go --dry-run
```

Prints exactly what would happen. Shows confidence scores, proposed names, and routing decisions.

### Run it

```bash
raagdosa go
```

Scans, moves, renames. Confirms each folder by default (configurable).

### Incremental runs

```bash
raagdosa go --since last_run
```

Only processes folders modified since the last successful run. Folders already committed to Clean are always excluded automatically.

---

## Output structure

```
~/Music/Incoming/
  Clean/
    Albums/
      Artist/                     ← library template: {artist}/{album}
        Album Name/
          01 - Track Title.mp3
          02 - Track Title.mp3
        Album Name (2019)/
        FLAC/                     ← FLAC segregation (optional)
          Album Name/
      _Various Artists/
        Compilation Name (2023)/
  Review/
    Albums/                       ← low confidence, needs checking
    Duplicates/                   ← already in Clean, or duplicate in this run
    Orphans/
  logs/
    sessions/
      2026-03-05_14-30-00_.../
        proposals.json
        report.txt
        report.csv
        report.html
    history.jsonl
    clean_manifest.json
```

---

## Command reference

```
raagdosa init                         Guided first-time setup → config.yaml
raagdosa doctor                       Check config, deps, disk, DJ databases

raagdosa go                           Full run: scan → move folders → rename tracks
raagdosa go --dry-run                 Preview without moving anything
raagdosa go --interactive             Confirm each folder before moving
raagdosa go --since last_run          Only process folders new since last run
raagdosa go --since 2026-01-15        Only process folders modified after date

raagdosa scan                         Scan only → generates proposals.json
raagdosa apply [proposals.json]       Apply an existing proposals.json
raagdosa apply --last-session         Apply the most recent session
raagdosa folders                      Folder move pass only (no track rename)
raagdosa tracks                       Track rename pass only (inside Clean/)
raagdosa resume <session_id>          Resume an interrupted session

raagdosa show "/path/to/folder"       Debug a single folder — why did it route here?
raagdosa status                       Library overview: counts, disk, pending folders
raagdosa verify                       Audit Clean library health vs. manifest
raagdosa learn                        Analyse Review patterns and suggest config fixes
raagdosa report                       View last session report (txt/csv/html)
raagdosa report --session <id>        View specific session
raagdosa report --format html         Open HTML report in browser

raagdosa history                      Show recent history entries
raagdosa undo --session <id>          Undo an entire session
raagdosa undo --id <action_id>        Undo a single action

raagdosa profile list                 List profiles
raagdosa profile use <name>           Switch active profile
raagdosa profile add <name> --source  Add a new profile
```

---

## Configuration

RaagDosa is configured entirely through `config.yaml`. Key sections:

### Library template

```yaml
library:
  template: "{artist}/{album}"    # artist/album (recommended)
  flac_segregation: false         # true → artist/FLAC/album/ for all-FLAC folders
```

### Artist normalisation

This is where you fix the "The Beatles vs Beatles" and "Jay-Z vs Jay Z" problem:

```yaml
artist_normalization:
  the_prefix: keep-front          # keep-front | move-to-end | strip

  unicode_map:                    # character substitutions applied first
    "Ø": "O"
    "ø": "o"

  aliases:                        # exact canonical mapping (case-insensitive key)
    "jay z":     "Jay-Z"
    "jayz":      "Jay-Z"
    "mos def":   "Yasiin Bey"
    "mo":        "MØ"
    "beatles":   "The Beatles"
```

**How normalisation works (in order):**
1. Unicode NFC normalisation
2. Unicode char map (e.g. Ø → O)
3. ALL-CAPS → Title Case
4. Alias map lookup (wins immediately if matched)
5. Hyphen variant normalisation (en-dash, em-dash → ASCII hyphen)
6. "The" prefix policy

### Confidence routing

```yaml
review_rules:
  min_confidence_for_clean: 0.85  # below this → Review/Albums/
```

Confidence is a 0–1 score based on how consistently tracks in a folder agree on their album and albumartist tags. High agreement = high confidence = Clean.

### Config learning

After a few runs, let RaagDosa suggest improvements:

```bash
raagdosa learn
```

Analyses your Review folders, identifies patterns (common suffixes causing low confidence, folders with no tags, threshold calibration), and proposes specific config changes. You choose which to apply.

---

## Folder routing logic

| Condition | Destination |
|-----------|-------------|
| Confidence ≥ threshold, not a duplicate | `Clean/Albums/Artist/Album/` |
| Confidence < threshold | `Review/Albums/` |
| Same proposed name appears twice in this run | `Review/Duplicates/` |
| Already exists in Clean (manifest or disk scan) | `Review/Duplicates/` |
| Tags absent, name derived from folder heuristic | `Review/Albums/` |
| Too many unreadable files | `Review/Albums/` |

---

## DJ workflow notes

**Before your library overhaul:** RaagDosa will detect Rekordbox and Serato database files in your source folders and warn you. Moving or renaming files will break existing analysis data (hot cues, waveforms, beatgrids).

**Recommended workflow:**
1. Run RaagDosa to clean your library structure
2. Re-import your Clean library into Rekordbox/Serato from scratch
3. Re-analyse tracks

This is intentional — you're building a new, clean library foundation, not patching the old one.

**FLAC segregation** (`library.flac_segregation: true`) places all-FLAC folders under `Artist/FLAC/Album/` instead of `Artist/Album/`, keeping your archival masters physically separate from MP3 working copies.

---

## Stopping mid-run

- **Ctrl+C once** — finishes the current folder, then stops cleanly. State is logged; use `raagdosa resume <session_id>` to continue.
- **Ctrl+C twice** — immediate force stop. Partially moved folders are left in place.

---

## Safety

- **Copy-verify-delete** — files are copied to destination, verified by file count and total byte size (optionally MD5 checksum), then source is deleted. Never a raw move.
- **Manifest** — every folder committed to Clean is recorded in `logs/clean_manifest.json`. Used for cross-run duplicate detection and `verify`.
- **History log** — every action is appended to `logs/history.jsonl`. Full undo support by session, action ID, or original path.
- **Path traversal protection** — proposed destination paths are validated against allowed roots before any move is executed.
- **Disk space pre-check** — aborts if destination has less than 110% of source size available.

---

## Troubleshooting

**"Could not build a proposal"**  
The folder has no readable tags and the folder name doesn't match any known pattern. Use `raagdosa show "/path/to/folder"` to see exactly what was found. Fix with a tag editor (MusicBrainz Picard, beets) and re-run.

**Everything going to Review**  
Run `raagdosa learn` after a few sessions. It will identify the most common reasons and suggest config changes. Common fixes: lower `min_confidence_for_clean`, add suffixes to `strip_common_suffixes_for_voting`.

**Artist names creating multiple folders**  
Add entries to `artist_normalization.aliases` in config.yaml. Example:
```yaml
aliases:
  "jay z": "Jay-Z"
  "jayz":  "Jay-Z"
```
Then re-run `raagdosa go`.

**Wrong year on folder names**  
Raise `year.require_presence_ratio` or `year.agreement_threshold` in config to be more selective, or set `year.enabled: false` to disable year entirely.

---

## License

MIT — see [LICENSE](LICENSE).

---

## Contributing

Issues and PRs welcome. Please open an issue before starting significant work to discuss the approach.

