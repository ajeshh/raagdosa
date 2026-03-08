# RaagDosa — Command Reference (v3.81)

> **CLI-first. Safe by default. Always undoable.**
> RaagDosa never silently reorganises your music. Every action is sessioned, logged, and reversible.

---

## Quick start — the only commands you need for day-to-day use

```bash
raagdosa go --dry-run        # preview everything — nothing moves
raagdosa go                  # do the work (scan → sort folders → rename tracks)
raagdosa go --since last_run # only process music added since last time
raagdosa status              # see what's in your library at a glance
raagdosa undo --session last # oops — put everything back
```

That's it for most runs. Everything else below is for when you want more control.

---

## How it works (30-second mental model)

```
Source folder  →  [scan]  →  [proposals]  →  [apply]  →  Clean/ or Review/
```

- **Clean/** = music RaagDosa is confident about. Folders renamed, tracks renamed.
- **Review/** = music that needs your eyes. Nothing is deleted, nothing is wrong.
- **`go`** does scan + apply in one step (what you'll use most of the time).
- **`scan`** + **`apply`** gives you a chance to inspect proposals first.
- Every action is written to a session log. `undo` reverses any of it.

---

## First-time setup

```bash
raagdosa init     # interactive wizard — sets up config.yaml
raagdosa doctor   # verify your setup is ready to run
```

`init` asks for your source folder, library structure preference, and FLAC settings, then writes `config.yaml`. Run `doctor` any time to check that config is valid and dependencies are installed.

---

## Core workflow commands

### `go` — do everything in one shot _(most common)_
```bash
raagdosa go
raagdosa go --dry-run               # preview: proposals printed, nothing moves
raagdosa go --interactive           # confirm each folder before it moves
raagdosa go --since last_run        # only folders added since your last run
raagdosa go --since 2026-01-15      # only folders modified after a specific date
raagdosa go --profile bandcamp      # use a different source profile
raagdosa go --performance slow      # gentle on older machines (see Performance Tiers below)
raagdosa go --performance ultra     # full speed on M3/M4 with fast storage
```

### `run` — same as `go`, explicit alias
```bash
raagdosa run --interactive
raagdosa run --profile incoming --dry-run
```

### `folders` — folder pass only (no track renames)
```bash
raagdosa folders
raagdosa folders --dry-run
raagdosa folders --interactive
```
Moves and renames folders into `Clean/` or `Review/`. Does not touch individual track files.

### `tracks` — track rename pass only (inside Clean/)
```bash
raagdosa tracks
raagdosa tracks --dry-run
raagdosa tracks --interactive
```
Renames audio files inside folders that are already in `Clean/`. Skips `Review/`. Safe to re-run.

---

## Preview workflow (inspect before you commit)

Use this when you want to see what RaagDosa would do before anything happens.

```bash
raagdosa scan                                      # generate proposals, don't move anything
raagdosa scan --out /tmp/proposals.json            # write proposals to a specific path
raagdosa apply --last-session                      # apply the most recent scan
raagdosa apply --last-session --interactive        # apply with per-folder confirmation
raagdosa apply logs/sessions/<id>/proposals.json  # apply from a specific file
```

---

## Single-folder debug

```bash
raagdosa show "/Music/Incoming/some folder"
raagdosa show "/Music/Incoming/some folder" --tracks
```

`show` gives you a full breakdown for one folder without moving anything:
- Tags read from each file
- Voting results (which album/artist won)
- Confidence score with a per-factor bar chart
- Proposed folder name and routing decision
- Tips if it would land in Review

Add `--tracks` to also see what each individual audio file would be renamed to.

---

## Library health + auditing

### `status` — library at a glance
```bash
raagdosa status
raagdosa status --profile incoming
```
Shows folder/track counts in Clean, Review, and Duplicates; manifest entries; new folders since last run; disk space.

### `verify` — deep audit of Clean/
```bash
raagdosa verify
```
Checks for: manifest entries missing from disk, disk folders not in manifest, empty folders, track filenames not matching the expected naming pattern. Writes a timestamped report to `logs/`.

### `clean-report` — stats on your Clean library _(v3.5)_
```bash
raagdosa clean-report
```
Produces a full breakdown of Clean/Albums: album count, total tracks, tagged vs untagged ratio, format breakdown with percentages, and flags any track-number gaps or duplicates.

### `orphans` — find loose audio files _(v3.5)_
```bash
raagdosa orphans
```
Finds audio files sitting directly in `Clean/Albums/` root or at artist-folder level without an album subfolder — the "fell through the cracks" files.

---

## Review folder management

### `review-list` — see what's waiting in Review _(v3.5)_
```bash
raagdosa review-list
raagdosa review-list --older-than 30    # only folders sitting there >30 days
raagdosa review-list --older-than 90    # good for a quarterly cleanup pass
```

Shows each Review folder with:
- How many days it's been sitting there (red if >60 days)
- Last-known confidence score
- The reason it was routed to Review

### `learn` — let RaagDosa suggest config improvements
```bash
raagdosa learn
```
Analyses your recent Review folders, spots patterns, and proposes config changes: lowering the confidence threshold, adding bracket suffixes to the strip list, or flagging zero-tag folders for pre-processing.

---

## Artist tools _(v3.5)_

```bash
raagdosa artists --list                  # list every artist in Clean/
raagdosa artists --find "portishead"     # fuzzy-search for an artist
raagdosa artists --find "massive"        # partial name works fine
```

`--list` shows all artist folders with album and track counts. `--find` does a similarity search — useful when you can't remember exactly how an artist is filed.

---

## Mix and VA handling _(v3.5)_

RaagDosa automatically detects mix folders (via keywords like "Mixed By", "Presents", "Sessions", and unique-artist ratio) and routes them to `Clean/_Mixes/` instead of `Clean/Albums/`.

### `extract --by-artist` — split a VA or mix folder
```bash
raagdosa extract "/Music/Incoming/DJ Mix 2024" --by-artist
raagdosa extract "/Music/Incoming/DJ Mix 2024" --by-artist --dry-run
```
Groups tracks by their artist tag and moves each group into `Clean/Albums/Artist/_Singles/`. The `--dry-run` shows you the groups before anything moves.

### `compare --folder` — diff two folders
```bash
raagdosa compare --folder "/Music/Folder A" "/Music/Folder B"
```
Shows which tracks are in A only, B only, or both — plus a tag comparison on matching files. Useful for spotting duplicates or checking a re-rip against an existing copy.

---

## Session reports

```bash
raagdosa report                      # print last session (text)
raagdosa report --format csv         # path to CSV (for import into a spreadsheet)
raagdosa report --format html        # path to dark-theme HTML report
raagdosa report --session <id>       # specific session
```

Reports are also written automatically to `logs/sessions/<session_id>/` after every scan.

### `diff` — compare two sessions _(v3.5)_
```bash
raagdosa diff last prev              # compare last run to the one before
raagdosa diff <session_id_a> <session_id_b>
```
Shows: folders that appeared or disappeared between sessions, routing changes (Clean→Review or vice versa), and confidence score changes above 5%.

---

## History

```bash
raagdosa history                     # last 50 actions
raagdosa history --last 100
raagdosa history --session <id>      # all actions from one session
raagdosa history --match "Moon Safari"  # filter by folder name
raagdosa history --tracks            # track renames instead of folder moves
```

---

## Undo

RaagDosa keeps separate undo streams for folder moves and track renames.

### Undo folder moves
```bash
raagdosa undo --session <session_id>         # undo a whole session
raagdosa undo --id <action_id>               # undo one specific move
raagdosa undo --from-path "/original/path"   # undo by original folder path
```

### Undo track renames
```bash
raagdosa undo --tracks --session <session_id>
raagdosa undo --tracks --id <track_action_id>
raagdosa undo --tracks --folder "/path/to/Clean/Album Folder"  # all renames in one folder
```

---

## Profiles

Profiles let you run RaagDosa against different source folders (Bandcamp downloads, Beatport, USB drives, etc.) with independent settings.

```bash
raagdosa profile list
raagdosa profile show incoming
raagdosa profile add bandcamp --source "/Music/Bandcamp" --clean-mode inside_root
raagdosa profile set incoming --source "/Users/you/Music/Incoming"
raagdosa profile use bandcamp        # set as active
raagdosa profile delete bandcamp
```

Pass `--profile <name>` to any command to use a non-active profile for that run.

---

## Resume an interrupted run

```bash
raagdosa resume <session_id>
raagdosa resume <session_id> --dry-run
```

If you hit Ctrl+C during a run (once = graceful stop after current folder, twice = force quit), use `resume` to pick up from where it left off.

---

## Routing explained

| Destination | When |
|---|---|
| `Clean/Albums/` | High confidence, no duplicates, tags look solid |
| `Clean/Albums/Artist/FLAC/Album/` | FLAC version of an album already in Clean as MP3 (when `flac_segregation: true`) |
| `Clean/_Mixes/` | Detected as a DJ mix or chart folder _(v3.5)_ |
| `Review/Albums/` | Low confidence, heuristic fallback, high unreadable ratio |
| `Review/Duplicates/` | Same proposed name already exists in Clean — after content comparison |
| `Review/Artifacts/` | `.nfo`, `.sfv`, `.log`, `.png`, `.m3u` and other non-audio files _(v3.81)_ |
| _(merged into existing)_ | Incoming has tracks missing from existing Clean folder — tracks copied in, source goes to Duplicates |

**Nothing is ever deleted.** Review is a holding area, not a bin.

---

## Track naming end states

| Folder type | Format |
|---|---|
| Normal album | `01 - Title.ext` |
| Multi-disc album | `1-01 - Title.ext` |
| Various Artists | `01 - Artist - Title.ext` |
| Mixed bag | `Artist - Title.ext` |
| EP _(v3.5)_ | Folder gets `[EP]` label; tracks follow album pattern |

---

## Per-folder override file _(v3.5)_

Drop a `.raagdosa` file inside any folder to override what RaagDosa would detect:

```yaml
# .raagdosa — place this inside the album folder
album: Correct Album Name
artist: Correct Artist
year: 1998
skip: false          # set true to exclude this folder entirely
confidence_boost: 0.10  # nudge confidence upward by this amount
```

Useful for folders with bad tags where you know the answer and don't want to bother fixing the tags first.

---

## Performance tiers _(v3.81)_

Pick the tier that matches your machine. RaagDosa sets workers, lookahead buffer, and copy-path sleep automatically.

| Tier | Best for | Workers | Behaviour |
|---|---|---|---|
| `slow` | 2017 Intel MBP, 8 GB RAM, old MacBook Air | 1 | Serial. 50ms pause between copy-path moves to keep system responsive. |
| `medium` | 2019–2021 Intel MBP, 16 GB RAM | 2 | Light parallelism. 10ms pause on copy-path moves. |
| `fast` | 2021+ M1/M2, 16 GB RAM | 4 | Good parallelism. No artificial sleep. |
| `ultra` | 2023+ M3/M4, 32 GB+, NVMe | 8 | Full blast. No throttle. |

```bash
raagdosa go --performance slow    # override for one run
raagdosa doctor                   # shows your hardware + recommended tier
```

Set a default in `config.yaml`:
```yaml
performance:
  tier: medium    # slow | medium | fast | ultra
```

The pause (`sleep_between_moves_ms`) only applies on **copy-path moves** (cross-device). Same-filesystem moves are atomic renames (~1ms) and are never throttled regardless of tier.

---

## Artifact handling _(v3.81)_

Non-audio files found in album folders get an explicit policy when `go` runs.

| File type | What happens |
|---|---|
| `.jpg`, `.jpeg` | Kept — moves with the album into Clean/ |
| `.pdf` | Kept — liner notes, booklets |
| `.cue` | Kept **if** a matching `.flac`, `.ape`, or `.wav` exists in the same folder |
| `.png` | Quarantined — sent to `Review/Artifacts/<album>/` |
| `.nfo`, `.sfv`, `.log`, `.txt`, `.url`, `.m3u`, `.m3u8` | Quarantined |
| Unknown extensions | Quarantined (conservative default) |

Quarantine happens **before** the album move. The album folder is processed normally without those files. `raagdosa show` displays the artifact breakdown alongside the tag analysis.

Disable with `artifacts.enabled: false` in `config.yaml` if you want the old behaviour (all files move with the folder).

---

## Duplicate resolution _(v3.81)_

When a proposed folder name matches an existing Clean folder, RaagDosa compares contents before routing.

| Outcome | What RaagDosa does |
|---|---|
| **Missing tracks** — incoming has tracks not in existing | Copies missing tracks into the existing Clean folder, re-runs track rename. Source goes to `Review/Duplicates/` tagged `[merged_N_tracks]`. |
| **FLAC upgrade** — incoming is FLAC, existing is MP3 | With `flac_segregation: true`: routes FLAC to `Artist/FLAC/Album/`. Without: routes to Review tagged `format_upgrade`. |
| **Lower quality** — incoming is MP3, existing has FLAC | Routes to `Review/Duplicates/` tagged `lower_quality_mp3`. Never downgrades. |
| **Exact duplicate** | Routes to `Review/Duplicates/` tagged `exact_duplicate`. |
| **Partial overlap** | Routes to `Review/Duplicates/` tagged `partial_overlap` with per-track counts. |

Disable intelligent comparison and return to name-only routing with `duplicates.compare_before_routing: false`.

---

## Common flags (work on most commands)

| Flag | What it does |
|---|---|
| `--dry-run` | Show what would happen, move nothing |
| `--interactive` | Confirm each folder/track before acting |
| `--profile <name>` | Use a named profile instead of the active one |
| `--since last_run` | Only process folders new since last run |
| `--since 2026-01-01` | Only process folders modified after a date |
| `--verbose` | Extra detail in output |
| `--quiet` | Suppress non-error output |
| `--performance <tier>` | Hardware tier: `slow` / `medium` / `fast` / `ultra` (overrides config) |

---

## All commands at a glance

| Command | What it does |
|---|---|
| `init` | Interactive setup wizard |
| `doctor` | Validate config, check deps and disk |
| `go` / `run` | Full pipeline: scan → move folders → rename tracks |
| `folders` | Folder pass only |
| `tracks` | Track rename pass only (inside Clean/) |
| `scan` | Preview mode — generate proposals without acting |
| `apply` | Execute from a proposals file |
| `resume` | Continue an interrupted session |
| `show` | Full debug breakdown for one folder |
| `show --tracks` | As above, plus per-track rename preview |
| `status` | Library overview |
| `verify` | Deep audit of Clean/ |
| `clean-report` | Stats and health report for Clean library |
| `orphans` | Find loose audio files |
| `review-list` | Tabular view of Review folder contents |
| `review-list --older-than N` | Filter to long-stale Review folders |
| `artists --list` | List all artists in Clean/ |
| `artists --find` | Fuzzy-search for an artist |
| `extract --by-artist` | Split a VA/mix folder into per-artist groups |
| `compare --folder A B` | Diff two folders |
| `diff A B` | Compare two session reports |
| `report` | View session report (txt / csv / html) |
| `history` | Show action log |
| `undo` | Reverse folder moves or track renames |
| `learn` | Suggest config improvements from Review patterns |
| `profile` | Manage source profiles (list/show/add/set/use/delete) |
| `cache` | Tag cache: `status` / `clear` / `evict` |

---

## Running without installing

```bash
python3 raagdosa.py <command>          # run directly
python3 raagdosa.py --config /path/to/config.yaml <command>  # custom config location
```

After `pip install raagdosa` (or `pip install -e .`) the `raagdosa` command is available directly.
