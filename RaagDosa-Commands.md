# RaagDosa — Command Reference (v9.0)

> **CLI-first. Safe by default. Always undoable.**
> RaagDosa never silently reorganises your music. Every action is sessioned, logged, and reversible.

---

## Quick start

```bash
raagdosa init                    # guided setup (first time)
raagdosa doctor                  # verify everything is ready
raagdosa go --dry-run            # preview — nothing moves
raagdosa go                      # do the work
raagdosa undo --session last     # oops — put everything back
```

That's it for most runs. Run `raagdosa help` for a grouped overview, or read on for full detail.

---

## How it works (30-second mental model)

```
Source folder  →  [scan]  →  [score & route]  →  [move]  →  Clean/ or Review/
```

- **Clean/** = music RaagDosa is confident about. Folders renamed, tracks renamed.
- **Review/** = music that needs your eyes. Nothing is deleted, nothing is wrong.
- **`go`** does scan + triage + move + rename in one step.
- **`scan`** + **`apply`** splits it so you can inspect proposals first.
- Every action is sessioned. `undo` reverses any of it.

---

## 1. Getting started

### `init` — guided setup
```bash
raagdosa init
```
Creates `config.yaml` (shareable settings) and `paths.local.yaml` (your private folder paths). Asks for source folder, profile name, and where output should go. Run it again to add another source folder as a new profile.

### `doctor` — verify setup
```bash
raagdosa doctor
```
Checks config validity, dependencies, disk space, and DJ database safety. Run any time.

### `status` — library at a glance
```bash
raagdosa status
raagdosa status --profile bandcamp
```
Folder/track counts in Clean, Review, and Duplicates. Disk space. New folders since last run.

### `help` — grouped command reference
```bash
raagdosa help
```
Prints all commands organised by workflow — faster than scrolling `--help`.

---

## 2. Core workflow

### `go` — the main command
```bash
raagdosa go                                    # scan → triage → move → rename
raagdosa go --dry-run                          # preview, nothing moves
raagdosa go -i                                 # interactive: review each folder 1-by-1
raagdosa go -i --sort confidence               # interactive: hardest folders first
raagdosa go -i --sort confidence-desc          # interactive: easiest folders first
raagdosa go -i --threshold 0.7                 # only review folders below 0.7
raagdosa go --auto-above 0.95                  # only auto-approve above 0.95
raagdosa go --force                            # skip triage, process all without confirmation
raagdosa go --since last_run                   # only new music since last time
raagdosa go --since 2026-01-15                 # only music modified after a date
raagdosa go --profile bandcamp                 # use a different profile
raagdosa go --session-name "Bandcamp Friday"   # custom session name
raagdosa go --genre-roots "Bass,House,Techno"  # protect genre root folders
raagdosa go --itunes                           # strip iTunes Genre/ layer first
```

**Default path:** scans everything, shows a triage dashboard (HIGH / MID / PROB tiers), then lets you bulk-approve or review individually.

**Interactive mode** (`-i`): skip triage, stream folders one by one with single-keypress actions.

**Sort options:**
| Sort | Effect |
|------|--------|
| `name` | Alphabetical (default) |
| `date-created` | Oldest first |
| `date-modified` | Oldest first |
| `confidence` | Hardest first — tackle problems first |
| `confidence-desc` | Easiest first — build momentum |

### `run` — alias for `go`
```bash
raagdosa run --interactive
```

### `folders` — folder pass only
```bash
raagdosa folders --dry-run
raagdosa folders --interactive
```
Moves and renames folders. Does not touch individual track files.

### `tracks` — track rename pass only
```bash
raagdosa tracks --dry-run
raagdosa tracks --interactive
```
Renames audio files inside `Clean/` folders. Skips `Review/`. Safe to re-run.

---

## 3. Preview workflow (scan + apply)

```bash
raagdosa scan                                    # score folders → proposals.json
raagdosa scan --out /tmp/proposals.json          # write to specific path
raagdosa apply --last-session                    # apply the most recent scan
raagdosa apply --last-session --interactive      # apply with per-folder confirmation
raagdosa apply logs/sessions/<id>/proposals.json # apply from a specific file
```

Scan now prints a **compact per-folder table** sorted by confidence:
```
  Conf  Route    Folder                                         Tracks  Format
──────────────────────────────────────────────────────────────────────────────────
  0.31  Review   techno_incoming                                     7  MP3 [CRATE]
  0.87  Clean    Burial - Untrue (2007)                              9  MP3
  0.92  Clean    Floating Points - Crush (2019)                     12  FLAC
```

Use `--verbose` to see *why* each folder scored the way it did (weak factors, route reasons).

### `resume` — continue an interrupted session
```bash
raagdosa resume <session_id>
raagdosa resume <session_id> --dry-run
```
If you hit Ctrl+C during a run, `resume` picks up where you left off.

---

## 4. Inspect & debug

### `show` — deep-dive a single folder
```bash
raagdosa show "~/Music/Incoming/some folder"
raagdosa show "~/Music/Incoming/some folder" --tracks
```
Full breakdown: tags, voting results, confidence bar chart, routing decision, tips. Add `--tracks` for per-track rename preview.

### `report` — session reports
```bash
raagdosa report                      # last session (text)
raagdosa report --format csv         # CSV for spreadsheets
raagdosa report --format html        # dark-theme HTML
raagdosa report --session <id>       # specific session
```
Reports are also auto-written to `logs/sessions/<id>/` after every scan.

### `sessions` — list past sessions
```bash
raagdosa sessions
raagdosa sessions --last 50
```

### `history` — action log
```bash
raagdosa history                         # last 50 actions
raagdosa history --last 100
raagdosa history --session <id>          # one session
raagdosa history --match "Moon Safari"   # filter by name
raagdosa history --tracks                # track renames instead of folder moves
```

### `tree` — directory snapshots
```bash
raagdosa tree /Volumes/music/Incoming    # snapshot the tree
raagdosa tree --list                     # list saved snapshots
raagdosa tree --diff snap_a snap_b       # what changed
```

### `compare` — diff two folders
```bash
raagdosa compare --folder "Folder A" "Folder B"
```
Tracks in A only, B only, or both — plus tag comparison on matches.

### `diff` — diff two sessions
```bash
raagdosa diff last prev
raagdosa diff <session_a> <session_b>
```

---

## 5. Library management

### `artists` — browse your Clean library
```bash
raagdosa artists --list                  # all artists with album/track counts
raagdosa artists --find "portishead"     # fuzzy search
```

### `review-list` — what's waiting in Review
```bash
raagdosa review-list
raagdosa review-list --older-than 30     # stale folders only
raagdosa review-list --older-than 90     # quarterly cleanup
```

### `review-promote` — fix a wrong VA classification
```bash
raagdosa review-promote "Album Name"               # re-evaluate as album
raagdosa review-promote "Album Name" --artist "X"   # force artist
raagdosa review-promote "Album Name" --dry-run      # preview
```

### `verify` — audit Clean library health
```bash
raagdosa verify
```
Checks manifest vs disk, empty folders, naming pattern compliance.

### `clean-report` — Clean library stats
```bash
raagdosa clean-report
```
Album count, tracks, tagged ratio, format breakdown, track-number gaps.

### `orphans` — loose audio files
```bash
raagdosa orphans
```
Finds tracks sitting at the wrong level (no album folder, fell through the cracks).

### `extract` — split a VA/mix folder
```bash
raagdosa extract "path/to/folder" --by-artist
raagdosa extract "path/to/folder" --by-artist --dry-run
```
Groups tracks by artist tag → moves each group to `Artist/_Singles/`.

### `catchall` — group loose files by artist
```bash
raagdosa catchall /path/to/dump-folder
raagdosa catchall /path/to/dump-folder --dry-run
```

---

## 6. Learning & config

### `learn` — config improvement suggestions
```bash
raagdosa learn
raagdosa learn --session <id>
```
Analyses Review folders, spots patterns, proposes config changes (threshold tweaks, new strip suffixes, zero-tag warnings).

### `learn-crates` — discover crate naming patterns
```bash
raagdosa learn-crates /Volumes/bass/DJ\ Genres/
raagdosa learn-crates /path --min-tracks 5
```
Walks a directory tree, finds folders that look like DJ crates (high album diversity, non-sequential tracks), groups them by naming pattern, and offers to save patterns to config.

### `genre` — genre root declarations
```bash
raagdosa genre list
raagdosa genre add "Bass"
raagdosa genre remove "Bass"
raagdosa genre show "Bass"
```
Genre roots are protected during sorting — RaagDosa won't rename them.

### `profile` — manage source profiles
```bash
raagdosa profile list
raagdosa profile show incoming
raagdosa profile add bandcamp --source "~/Music/Bandcamp"
raagdosa profile set incoming --source "~/Music/Incoming"
raagdosa profile use bandcamp
raagdosa profile delete bandcamp
```
Profiles let you run RaagDosa against different source folders independently. Pass `--profile <name>` to any command to use a non-active profile.

### `template` — library organisation templates
```bash
raagdosa template list
raagdosa template show genre
```

### `reference` — musical reference (aliases, labels, patterns)
```bash
raagdosa reference list
raagdosa reference export
raagdosa reference export --section artist_aliases
raagdosa reference import community_ref.yaml
```
The Musical Reference holds accumulated knowledge: artist aliases, known labels, VA rescue prefixes, and noise patterns. Import/export enables community sharing.

### `cache` — tag cache management
```bash
raagdosa cache                   # show cache stats
raagdosa cache clear             # clear all cached tags
```

---

## 7. Undo

RaagDosa keeps separate undo streams for folder moves and track renames.

### Undo folder moves
```bash
raagdosa undo --session last                 # undo most recent session
raagdosa undo --session <session_id>         # undo a specific session
raagdosa undo --id <action_id>               # undo one specific move
raagdosa undo --from-path "/original/path"   # undo by original path
raagdosa undo --folder "Album Name"          # undo all moves for a folder
```

### Undo track renames
```bash
raagdosa undo --tracks --session <session_id>
raagdosa undo --tracks --id <track_action_id>
raagdosa undo --tracks --folder "/path/to/Album"
```

---

## Routing explained

| Destination | When |
|---|---|
| `Clean/Albums/` | High confidence, tags look solid |
| `Clean/_Mixes/` | Detected as DJ mix or chart compilation |
| `Review/Albums/` | Low confidence, heuristic fallback, noisy tags |
| `Review/_Sets/` | DJ set prep crate (preserved intact) |
| `Review/Duplicates/` | Same proposed name already in Clean or current run |

**DJ Crates:** Folders detected as personal DJ crates (singles dumps, genre bins) can be "exploded" — each track moves to its own `Artist/_Singles/` folder instead of the whole folder moving as one. Embedded albums found within crates are kept together.

**Nothing is ever deleted.** Review is a holding area, not a bin.

---

## Track naming

| Folder type | Format |
|---|---|
| Normal album | `01 - Title.ext` |
| Multi-disc album | `1-01 - Title.ext` |
| Various Artists | `01 - Artist - Title.ext` |
| Mixed bag | `Artist - Title.ext` |
| EP | Folder gets `[EP]` label; tracks follow album pattern |

---

## Per-folder override file

Drop a `.raagdosa` file inside any folder to override detection:

```yaml
album: Correct Album Name
artist: Correct Artist
year: 1998
skip: false              # true to exclude this folder
confidence_boost: 0.10   # nudge confidence up
folder_type: crate_singles  # force crate detection
```

---

## Triage dashboard actions (default `go` path)

| Key | Action |
|-----|--------|
| `a` | Bulk-approve HIGH tier (requires typing `YES`) |
| `r` | Review ALL folders 1-by-1 |
| `h` / `m` / `p` | List folders in HIGH / MID / PROB tier |
| `q` | Quit without moving |

## Interactive review keys (`go -i` and triage review)

| Key | Action |
|-----|--------|
| `z` / Enter | Move with proposed routing |
| `x` | Reject → Review |
| `c` | Skip |
| `e` | Edit album title |
| `e3` | Edit track 3 title |
| `a` | Set artist |
| `v` | Toggle VA / album |
| `o` | Open in Finder |
| `R` | Rescan folder (after Finder changes) |
| Space / `b` | Show track preview |
| `q` | Quit |
| `?` | Help |

**Crate prompts** (when a DJ crate is detected):

| Key | Action |
|-----|--------|
| `e` | Explode — route each track to its artist folder |
| `v` | Keep as VA — treat as compilation |
| `s` | Skip |
| `d` | Show all tracks |

---

## Common flags

| Flag | What it does |
|---|---|
| `--dry-run` | Show what would happen, move nothing |
| `--interactive` / `-i` | Folder-by-folder streaming review |
| `--sort <mode>` | Sort order: `name`, `date-created`, `date-modified`, `confidence`, `confidence-desc` |
| `--threshold N` | Only review folders below this confidence |
| `--auto-above N` | Override auto-approve threshold |
| `--force` | Skip triage, process all |
| `--session-name <name>` | Custom session name (e.g. "Bandcamp Friday") |
| `--profile <name>` | Use a named profile |
| `--since last_run` | Only new music since last run |
| `--since 2026-01-01` | Only music modified after a date |
| `--genre-roots "A,B,C"` | Protect genre root folders |
| `--itunes` | Strip iTunes Genre/ layer first |
| `--verbose` | Extra detail (scoring reasons in scan output) |
| `--quiet` | Suppress non-error output |

---

## Running without installing

```bash
python3 raagdosa.py <command>
python3 raagdosa.py --config /path/to/config.yaml <command>
```

After `pip install raagdosa` (or `pip install -e .`) the `raagdosa` command is available directly.
