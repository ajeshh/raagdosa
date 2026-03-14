# RaagDosa — User Guide

---

## Contents

1. [Quick start](#quick-start)
2. [How it works](#how-it-works)
3. [The triage workflow](#the-triage-workflow)
4. [Interactive review](#interactive-review)
5. [Profiles](#profiles)
6. [Library templates](#library-templates)
7. [Understanding the confidence score](#understanding-the-confidence-score)
8. [Configuration](#configuration)
9. [Sessions and history](#sessions-and-history)
10. [Undoing a run](#undoing-a-run)
11. [Debug a single folder](#debug-a-single-folder)
12. [DJ workflow notes](#dj-workflow-notes)

---

## Quick start

### 1. Install

```bash
pip install raagdosa
```

Verify it's working:

```bash
raagdosa --version
```

### 2. Configure

Run the setup wizard. It creates `config.yaml` (settings) and `paths.local.yaml` (your private folder paths):

```bash
raagdosa init
```

`paths.local.yaml` is gitignored and never shared. Set your source folder here:

```yaml
profiles:
  default:
    source_root: ~/Music/Incoming
    clean_mode: inside_root
active_profile: default
```

### 3. Start with a test folder

Before pointing RaagDosa at your real library, run it against a small test folder — 20–50 albums. This lets you see how it scores and routes things before committing to a full run.

```bash
# Edit paths.local.yaml → set source_root to your test folder
# Then preview:
raagdosa go --dry-run
```

The dry run shows you exactly what would happen — folders proposed for Clean/ or Review/, confidence scores, proposed names — without moving anything.

### 4. Run it

When the dry run looks right:

```bash
raagdosa go
```

You'll see the triage dashboard first. It splits your folders into two tiers and lets you decide what happens before anything moves. [More on triage →](#the-triage-workflow)

### 5. Check the result

```bash
raagdosa status          # overview: counts, paths, last run
raagdosa report          # session report for the last run
```

### 6. Undo if needed

```bash
raagdosa undo --session last
```

Everything from the last run is reversed. Your source folder is restored exactly as it was.

---

## How it works

RaagDosa runs the same six-step pipeline on every folder in your source:

```
  1. SCAN      Walk source tree — find candidate album folders
               Parallel workers, tag cache for warm runs

  2. READ      mutagen reads every audio tag in every file
               artist, album, year, genre, BPM, key, label, compilation flag

  3. VOTE      Plurality vote across all tracks in the folder
               Tags beat folder names — folder name is only a fallback

  4. SCORE     7-factor confidence score (0.0 → 1.0)
               dominance · coverage · title quality · completeness
               filename consistency · albumartist · folder alignment

  5. ROUTE     score ≥ threshold  →  Clean/
               score < threshold  →  Review/
               name collision     →  Review/Duplicates/

  6. MOVE      Same filesystem  →  atomic os.rename() (~1ms per folder)
               Cross-device     →  copy → verify checksum → delete source
               File timestamps (creation date, modification time) are preserved
```

**The tag is the source of truth.** If 14 of 16 tracks agree the album is "Mezzanine" and the folder is named "massive attack mezzanine 320kbps", the album name comes from the tags. The folder name is used only when tags are absent or incomplete.

**Review/ is not a failure state.** It is a deliberate holding area for anything the tool is not confident about. Expect 10–30% of a typical library to land there on the first run. Fix the tags, re-run, and they'll promote to Clean/.

### Output structure

```
~/Music/Incoming/raagdosa/
  Clean/
    Albums/
      Massive Attack/
        Mezzanine (1998)/
      DJ Shadow/
        Endtroducing..... (1996)/
      Pink Floyd/
        The Wall (1979)/
          CD1/
          CD2/
    _Mixes/
    _Singles/
  Review/
    Albums/
    Duplicates/
  logs/
    history.jsonl
    sessions/
      2026-03-09_14-22_incoming/
        report.txt
        report.csv
        report.html
```

---

## The triage workflow

Running `raagdosa go` scans everything first, then presents a dashboard before anything moves:

```
══════════════════════════════════════════════════════════════════════
  RAAGDOSA v8.0.0  ·  Triage  ·  Session 2026-03-09_14-22_a3f1
──────────────────────────────────────────────────────────────────────
  AUTO tier   (conf ≥ 0.85)   201 folders  →  will go to Clean/
  HOLD tier   (conf < 0.85)    40 folders  →  needs review
  Dry run: OFF
──────────────────────────────────────────────────────────────────────
  [a] Bulk-approve AUTO, then review HOLD
  [r] Review all folders 1-by-1
  [q] Quit without moving anything
══════════════════════════════════════════════════════════════════════
  Action: _
```

| Option | What happens |
|--------|-------------|
| `a` | AUTO tier folders are moved to Clean/ (requires typing `YES` to confirm). Then HOLD tier opens in interactive review. |
| `r` | Skip bulk-approve. All folders go to interactive review one at a time. |
| `q` | Nothing moves. Session is preserved — re-run any time. |

### Overriding the auto-approve threshold

The default auto-approve threshold is the same as `min_confidence_for_clean` (0.85). To raise it for one run:

```bash
raagdosa go --auto-above 0.95   # only auto-approve above 0.95
```

To set it permanently in `config.yaml`:

```yaml
review_rules:
  min_confidence_for_clean: 0.85
  auto_approve_threshold: 0.90
```

### Bypassing triage

```bash
raagdosa go --interactive   # skip triage, review every folder 1-by-1 as they scan
raagdosa go --force         # no triage, no review — move everything automatically
raagdosa go --dry-run       # preview only, nothing moves
```

---

## Interactive review

In interactive review, each folder gets its own review card:

```
══════════════════════════════════════════════════════════════════════
  [  3 / 40 ]                                          ROUTE: REVIEW
──────────────────────────────────────────────────────────────────────
  FROM:  va - deep house sessions vol.3 (2019) [mp3 320]
    TO:  Various Artists - Deep House Sessions, Vol. 3 (2019)/

  Artist: Various Artists        VA: Yes
  Album:  Deep House Sessions    Year: 2019
  Tracks: 24 files  ·  MP3 24

  CONFIDENCE  ██████████░░░░░░░░░░  0.48
    dominance            ██████░░░░░░░░░░░░░░  0.30  tracks disagree on artist
    tag_coverage         ████████████░░░░░░░░  0.60  6 tracks missing year tag
    title_quality        ████████████████░░░░  0.78
    folder_alignment     ████████░░░░░░░░░░░░  0.40  folder name differs from tags
──────────────────────────────────────────────────────────────────────
  [y] Approve  [s] Skip  [r] → Review  [a] Set Artist
  [v] Toggle VA  [t] Tracks  [q] Stop  [?] Help
══════════════════════════════════════════════════════════════════════
  Action [y]: _
```

| Key | Action |
|-----|--------|
| Enter / `z` | Approve — move folder using proposed routing |
| `x` | Reject — leave in source |
| `c` | Skip — skip this folder for now |
| `e` | Edit album title |
| `e<N>` | Edit track title for track N (e.g. `e3` edits track 3) |
| `a` | Override detected artist, re-route as single-artist album |
| `v` | Toggle VA / single-artist, re-derives track rename pattern |
| `o` | Open folder in Finder for manual fixes |
| `R` | Rescan folder — re-read tags after changes |
| `space` / `b` | Show track rename preview |
| `q` | Stop here — all moves already made are kept |
| `?` | Show help |

Press `q` at any point and stop. Everything moved so far stays in place. Re-run the same command to continue reviewing the remaining folders.

### Review only low-confidence folders

```bash
raagdosa go --interactive --threshold 0.8
# folders above 0.8 move automatically; only below-0.8 get review cards
```

### Sort order

```bash
raagdosa go --interactive --sort name            # alphabetical (default)
raagdosa go --interactive --sort date-modified   # most recently touched first
raagdosa go --interactive --sort date-created    # newest in folder first
```

---

## Profiles

Profiles let you run RaagDosa against different sources with different settings — different template, different destination, different thresholds.

**When you need profiles:**

- You have a downloads inbox, a vinyl rips folder, and a Bandcamp folder — each should go to different destinations or use different layouts.
- Your DJ USB uses a genre/BPM structure but your archive uses a simple artist/album layout.
- You want to process a one-off folder without changing your main config.

### Setting up a second profile

In `paths.local.yaml`:

```yaml
profiles:
  default:
    source_root: ~/Music/Incoming
    clean_mode: inside_root

  dj-usb:
    source_root: /Volumes/USB/Prep
    clean_root: /Volumes/USB/Clean
    library:
      template: "{genre}/{bpm_range}/{artist} - {album}"

active_profile: default
```

```bash
raagdosa go --profile dj-usb   # run against the dj-usb profile
raagdosa profile use dj-usb    # switch default active profile
```

### Profile commands

```bash
raagdosa profile list                               # all profiles
raagdosa profile show dj-usb                        # inspect one profile
raagdosa profile add vinyl --source ~/Music/Vinyl   # create a profile
raagdosa profile set default --template dated       # update a setting
raagdosa profile delete vinyl                       # remove a profile
```

---

## Library templates

Templates control how albums land inside `Clean/`. 9 built-in options:

| ID | Pattern | Good for |
|----|---------|----------|
| `standard` | `{artist}/{album}` | Safe default |
| `dated` | `{artist}/{year} - {album}` | Chronological discography |
| `flat` | `{artist} - {album}` | Minimal depth, fast browsing |
| `genre` | `{genre}/{artist}/{album}` | Multi-genre collections |
| `decade` | `{decade}/{genre}/{artist} - {album}` | Era-first browsing |
| `bpm` | `{bpm_range}/{artist} - {album}` | Tempo-first DJ library |
| `genre-bpm` | `{genre}/{bpm_range}/{artist} - {album}` | Open-format DJ |
| `genre-bpm-key` | `{genre}/{bpm_range}/{camelot_key}/{artist} - {album}` | Harmonic mixing |
| `label` | `{label}/{artist} - {album}` | Label-focused collectors |

```bash
raagdosa template list               # all templates
raagdosa template show genre-bpm     # details + example folder tree
```

### Template tokens

| Token | Source | Fallback |
|-------|--------|---------|
| `{artist}` | Voted albumartist tag | `_Unknown` |
| `{album}` | Voted album tag | `_Untitled` |
| `{year}` | Year tag or folder name | empty |
| `{genre}` | Voted genre tag, normalised | `_Unsorted` |
| `{decade}` | Derived from year | `_Unknown Era` |
| `{bpm_range}` | Median BPM, bucketed | `_Unknown BPM` |
| `{camelot_key}` | Key tag → Camelot notation | `_Unknown Key` |
| `{label}` | Voted label tag, suffixes stripped | `_Unknown Label` |

After a scan, RaagDosa shows how well your library's tags cover your active template:

```
Tag coverage for template: {genre}/{bpm_range}/{artist} - {album}
  genre      ████████████████████  92%
  bpm        ████████░░░░░░░░░░░░  41%   ← consider adding BPM tags before using this template
  artist     ████████████████████  99%
```

---

## Understanding the confidence score

Every folder gets a score from 0.0 to 1.0. This score drives the routing decision.

### The 7 factors

| Factor | Weight | What it measures | Low score means |
|--------|--------|-----------------|-----------------|
| `dominance` | 0.40 | Tracks agree on album + artist | Tags disagree — multiple albums or artists voted |
| `tag_coverage` | 0.15 | All key tags present across all tracks | Many tracks missing album, artist, or year |
| `title_quality` | 0.12 | Titles look like real titles | Garbage strings, missing titles, or all identical |
| `completeness` | 0.12 | Track numbers are sequential, no gaps or dupes | Duplicate or missing track numbers |
| `filename_consist` | 0.07 | Filenames match tag content | Filenames have scene suffixes or don't match tags |
| `aa_consistency` | 0.06 | Consistent albumartist tag across all tracks | Tracks have inconsistent albumartist |
| `folder_alignment` | 0.08 | Source folder name matches proposed clean name | Folder name is noisy or very different from tags |

### What you can do about a low score

The most effective improvements, in order:

1. **Fix the album/artist tag** — `dominance` is weighted 0.40. If tracks disagree, fixing the albumartist tag in your tag editor has the biggest effect.
2. **Add missing tags** — tracks without year or album tags drag `tag_coverage` down.
3. **Fix track titles** — identical titles on every track usually means a rip error.
4. **Use a `.raagdosa` override file** — drop this inside any folder to force values:

```yaml
# .raagdosa — place inside the source album folder
album: Correct Album Name
artist: Correct Artist
year: 1997
confidence_boost: 0.15
```

### Seeing the breakdown

```bash
raagdosa show "~/Music/Incoming/some folder"
```

```
══════════════════════════════════════════════════════════════════
raagdosa show — DJ Shadow - Endtroducing..... (1996)
══════════════════════════════════════════════════════════════════

Source:  ~/Music/Incoming/dj_shadow--endtroducing-WEB-1996-FTD
Files:   16 × .flac

Tag votes
  album        "Endtroducing....."  16/16  ████████████████  ✓
  albumartist  "DJ Shadow"          16/16  ████████████████  ✓
  year         "1996"               15/16  ███████████████░  ✓

Confidence  0.94  ██████████████████░░  → Clean/

  dominance         0.97  ████████████████████
  tag_coverage      1.00  ████████████████████
  title_quality     0.94  ██████████████████░░
  completeness      1.00  ████████████████████
  filename_consist  0.88  █████████████████░░░  some noisy filenames
  aa_consistency    1.00  ████████████████████
  folder_alignment  0.91  ██████████████████░░

Proposed:  DJ Shadow - Endtroducing..... (1996)
Routing:   ✦ clean  →  Clean/Albums/DJ Shadow/Endtroducing..... (1996)/
══════════════════════════════════════════════════════════════════
```

---

## Configuration

### Minimum required — `paths.local.yaml`

This is the only file you must configure. It holds your filesystem paths and is never shared:

```yaml
profiles:
  default:
    source_root: ~/Music/Incoming     # where your messy music lives
    clean_mode: inside_root           # puts Clean/ and Review/ inside source_root
active_profile: default
logging:
  root_dir: ~/Music/Incoming/raagdosa/logs
```

`clean_mode: inside_root` is the simplest setup. For a separate destination:

```yaml
profiles:
  default:
    source_root: ~/Music/Incoming
    clean_mode: separate
    clean_root: ~/Music/Clean
    review_root: ~/Music/Review
```

### Tuning `config.yaml`

The most commonly adjusted settings:

```yaml
review_rules:
  min_confidence_for_clean: 0.85    # raise this to be more conservative

artist_normalization:
  the_prefix: keep-front            # keep-front | move-to-end | strip

title_cleanup:
  strip_trailing_phrases:
    - "free download"
    - "official video"
    - "ncs release"                 # add any phrases your library has

library:
  template: "{artist}/{album}"      # change to any built-in template ID
  flac_segregation: false           # true → FLAC under Artist/FLAC/Album/
```

### Musical Reference

The `reference:` section in `config.yaml` is shareable knowledge about your library — artist aliases, known labels, and prefixes that should never be classified as Various Artists. It improves with use.

```yaml
reference:
  artist_aliases:
    "bjork":        "Bjork"
    "jay z":        "Jay-Z"
    "mos def":      "Yasiin Bey"
    "aphex twin":   "Aphex Twin"

  known_labels:
    - "Cosmovision Records"
    - "Ninja Tune"

  va_rescue_prefixes:
    - "sven wunder"      # always treat as single-artist, even if tracks vary
    - "blend mishkin"
```

Export and import reference files to share with others:

```bash
raagdosa reference export                       # → reference_export.yaml
raagdosa reference import community_ref.yaml    # merge into your reference
raagdosa reference list                         # see current entries
```

### BPM buckets

If you use the `{bpm_range}` template token, configure your zones:

```yaml
bpm_buckets:
  width: 10                     # numeric bucket width for unmatched BPMs
  named_zones:
    "Downtempo":    [60, 99]
    "House":        [120, 132]
    "Techno":       [133, 145]
    "D&B / Jungle": [160, 180]
```

A folder with a median BPM of 127 → `House/`. A folder at 152 → `150-159/`.

---

## Sessions and history

Every run is a session with a unique ID (`2026-03-09_14-22_incoming`). Session data is written to `logs/sessions/<id>/`.

Name sessions for easier recall:

```bash
raagdosa go --session-name "Bandcamp Friday"
# Session ID: 2026-03-14_10-30_bandcamp-friday
```

```bash
raagdosa sessions              # list last 20 sessions with move counts
raagdosa sessions --last 5     # just the last 5

raagdosa report                # session report (txt + csv + html) for last run
raagdosa report --session <id> # specific session

raagdosa history               # recent action log (last 50 entries)
raagdosa history --session last
raagdosa history --match "Burial"   # filter by path fragment
```

---

## Undoing a run

Every move is logged to `logs/history.jsonl`. Undo is always available.

### Undo a full session

```bash
raagdosa undo --last             # undo most recent session (shortcut)
raagdosa undo --session last     # same thing, explicit form
raagdosa undo --session -2       # undo second-to-last session
raagdosa undo --session 2026-03-09_14-22_incoming   # undo by session ID
```

### Interactive picker

Run `raagdosa undo` with no arguments to pick individual moves from the last session:

```
Last session: 2026-03-09_14-22_a3f1  (12 moves)

  #    Folder                                            Dest
  ──────────────────────────────────────────────────────────────
  1    Massive Attack - Mezzanine (1998)                 clean
  2    Portishead - Dummy (1994)                         clean
  3    DJ Shadow - Endtroducing..... (1996)              clean
  4    Unknown Artist - 2024-03-01                       review
  5    Aphex Twin - Selected Ambient Works (1992)        clean
  ...

Enter number(s) to undo (e.g. 3  or  1,4,5  or  all), or Enter to cancel:
  > 4
```

RaagDosa reverses the selected moves and reports what was restored.

### Undo track renames only

```bash
raagdosa undo --tracks --folder "Massive Attack - Mezzanine"
```

---

## Debug a single folder

```bash
raagdosa show "~/Music/Incoming/some folder"
raagdosa show "~/Music/Incoming/some folder" --tracks   # also show per-track renames
```

This shows you tag votes, confidence breakdown, proposed name, and routing decision. Use this whenever you're confused about why a folder scored low or routed unexpectedly.

---

## DJ workflow notes

**Moving files breaks DJ software library references.** This is not a caveat — it is the primary concern for any DJ using this tool. Rekordbox, Serato, and Traktor store cue points, beatgrids, hot cues, loops, and waveform data against absolute file paths. When paths change, those references break.

**The golden rule: only run RaagDosa on folders that are NOT yet imported into your DJ software.**

### Recommended workflow

1. Download new music to an intake folder (separate from your DJ library)
2. Run `raagdosa go --dry-run` to preview what will happen
3. Run `raagdosa go` — review the triage dashboard, approve moves
4. Inspect Clean/ — verify the structure looks right
5. Import Clean/ into Rekordbox / Serato / Traktor as a new collection
6. Re-analyse in your DJ software — fresh waveforms, accurate BPM, consistent cue points

### If you break DJ software links

- **Rekordbox** — "Relocate Lost Files" (right-click on a missing track or the collection). Rekordbox stores data in its internal database (`pioneer/rekordbox/master.db`); you lose hot cues, memory cues, beatgrid adjustments, phrase analysis, and play history. The "Relocate" function can handle bulk path changes if filenames stayed the same.
- **Serato** — Serato stores cue points inside the file's own ID3 tags, so those survive a move. But crate references (`.crate` files and `_Serato_` folder structure) break. Drag the new Clean/ folder into the Serato panel to re-link.
- **Traktor** — uses `collection.nml` with absolute paths. Use "Consistency Check" to relocate from the new path.

### Timestamp preservation (v8.5)

File creation dates and modification times are now preserved when folders move. If you sort by "date added" in Rekordbox or your file manager, the original dates survive the reorganisation. On macOS, creation dates require Xcode Command Line Tools (`SetFile`). On Linux/Windows, modification times are preserved; creation dates follow OS-level behaviour.

**FLAC segregation:**

```yaml
library:
  flac_segregation: true
  # FLAC masters → Artist/FLAC/Album/
  # MP3 copies  → Artist/Album/
```

**Camelot key mapping:**

The `{camelot_key}` template token converts raw key tags (Am, A minor, A min, F#m, Ebm) to Camelot notation (1A–12B). All 24 keys and enharmonic equivalents are mapped.

---

## Getting help

```bash
raagdosa doctor                    # check config, deps, disk space, DJ databases
raagdosa status                    # library overview: counts, disk, pending
raagdosa learn                     # analyse Review/ patterns, suggest config tweaks
raagdosa --help                    # all commands
raagdosa <command> --help          # flags for a specific command
```

**Commands reference:** [RaagDosa-Commands.md](../RaagDosa-Commands.md)
**Changelog:** [CHANGELOG.md](../CHANGELOG.md)
**Issues:** [GitHub Issues](https://github.com/ajeshh/raagdosa/issues)
