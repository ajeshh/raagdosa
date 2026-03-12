# Changelog

All notable changes to RaagDosa are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [8.0.0] — 2026-03-12

### Release summary

v8 is the **"Triage Before You Commit"** release. Instead of moving folders the moment they pass
confidence threshold, RaagDosa now scans everything first, presents a dashboard showing how the run
splits (auto-approvable vs needs review), lets you bulk-approve the high-confidence tier with a single
YES, then hands the rest to interactive folder-by-folder review.

This release also tightens several detection rules that caused confidence mis-scoring in real
library data: EP detection from folder names (catches EPs with non-standard track counts), identical
title detection (copy-paste tag contamination), Volume/Vol no longer stripped as disc indicator,
folder alignment v2 (noise token stripping + year-anchored cutoff), and Smart Title Case applied
to all-lowercase artist tags.

Version is now read from `pyproject.toml` via `importlib.metadata` — no more duplicate version
constants in source files.

---

### New — Triage dashboard (default `go` workflow)

The `go` / `run` command now scans all folders first, then presents a triage dashboard before
moving anything:

```
══════════════════════════════════════════════════════════════════════
  RAAGDOSA v8.0.0  ·  Triage  ·  Session 2026-03-12_a3f1
──────────────────────────────────────────────────────────────────────
  AUTO tier   (conf ≥ 0.85)   712 folders  →  will go to Clean/
  HOLD tier   (conf < 0.85)    98 folders  →  manual review needed
──────────────────────────────────────────────────────────────────────
  [a] Bulk-approve AUTO + review HOLD   [r] Review all   [q] Quit
══════════════════════════════════════════════════════════════════════
```

| Action | Behaviour |
|--------|-----------|
| `a` | Bulk-approve AUTO tier (requires typing `YES`), then interactive review for HOLD |
| `r` | Skip bulk-approve, review ALL folders 1-by-1 |
| `q` | Quit without moving anything |

Previous streaming behaviour is preserved:
- `raagdosa go --interactive` — skip triage, original folder-by-folder streaming
- `raagdosa go --force` — skip triage entirely, process all folders without confirmation

### New — `--auto-above` flag and `auto_approve_threshold` config key

Override the auto-approve threshold for a single run:

```bash
raagdosa go --auto-above 0.95   # only auto-approve folders with conf ≥ 0.95
```

Persistent setting in `config.yaml`:

```yaml
review_rules:
  min_confidence_for_clean: 0.85   # routing threshold (unchanged)
  auto_approve_threshold: 0.85     # triage: folders above this go to AUTO tier
```

The auto-approve threshold is clamped to `≥ min_confidence_for_clean` — you can't
auto-approve folders that would route to Review.

### New — `sessions` command

List recent sessions with move counts and example folder names:

```bash
raagdosa sessions           # last 20 sessions
raagdosa sessions --last 5  # last 5 sessions
```

```
Recent sessions (20 of 47 total):

  2026-03-12_14-30-00_a3f1
    712 moves  ·  690 clean  ·  22 review  ·  2026-03-12T14:30:00
    e.g. Massive Attack - Mezzanine, Portishead - Dummy, DJ Shadow - Endtroducing.....
```

### New — Undo improvements

`--session last` and shorthand `-1`, `-2` (Nth-most-recent session):

```bash
raagdosa undo --session last   # undo most recent session
raagdosa undo --session -2     # undo second-to-last session
```

Interactive picker — run `raagdosa undo` with no arguments to choose individual moves
from the last session:

```
Last session: 2026-03-12_14-30-00_a3f1
  #    Folder                                                    Dest
  ─────────────────────────────────────────────────────────────────────────
  1    Massive Attack - Mezzanine (1998)                         clean
  2    Portishead - Dummy (1994)                                 clean
  3    Unknown Artist - 2024-03-01                               review

Enter number(s) to undo (e.g. 3  or  1,3,5  or  all), or Enter to cancel:
  >
```

`history --session last` also resolves to the most recent session.

### New — `--sort` flag for interactive / folders commands

Control the order folders are presented in interactive review:

```bash
raagdosa go --interactive --sort date-modified   # most recently modified first
raagdosa go --interactive --sort date-created    # newest folders first
raagdosa go --interactive --sort name            # alphabetical (default)
raagdosa folders --sort date-modified
```

### Fixed — EP detection from folder/album name

`detect_ep()` now checks the folder name for EP keywords (`EP`, `E.P.`) in addition to
track count. A folder with 8 tracks named "Artist - Debut EP" is now correctly classified as
an EP, rather than being treated as a full album.

Previously: only track count was checked (2–6 tracks → EP).
Now: track count OR folder name contains EP keyword → EP.

### Fixed — Volume/Vol no longer stripped as disc indicator

`strip_disc_indicator()` previously stripped "Volume 2" / "Vol. 3" from album names, treating
them the same as "Disc 2" / "CD 2". This caused "Now That's What I Call Music Volume 2" to
deduplicate against "Volume 1". Fixed: only `disc` / `cd` / `disk` markers are stripped.
`volume` / `vol` are preserved as part of the album name.

### Fixed — Identical title detection (copy-paste tag contamination)

`compute_meaningful_title_ratio()` now penalises folders where all tracks share the same title
string. A folder where every track is tagged "Track 01" (a common iTunes/rip error) now scores
≤ 0.30 on title quality, pushing confidence below threshold and routing to Review.

Previously: 12/12 tracks with identical titles scored 1.0 on title quality.
Now: max 0.30 when all tracks share the same title (and track count > 2).

### Improved — Folder alignment v2 (`folder_alignment_bonus`)

The folder alignment confidence factor now uses token-coverage comparison instead of string
edit distance. Key changes:

- **Year-anchored cutoff**: everything after the first 4-digit year in the source folder name
  is discarded before comparison. Scene release suffixes (`-WEB-2023-FTD`, `-MFDOS`) no longer
  drag the score down.
- **Noise token stripping**: format markers (`flac`, `mp3`, `320`, `web`, `vinyl`), quality tags
  (`remaster`, `deluxe`, `expanded`), and scene markers are stripped before comparison.
- **Token coverage**: score = fraction of proposed tokens present in source folder.
- Weight bumped 0.05 → 0.08 (alignment is more reliable now, deserves more weight).

Configurable via `reference.folder_alignment_noise_tokens` in `config.yaml`.

### Improved — Smart Title Case applied to all-lowercase artist tags

Artist names that are entirely lowercase in the tag (e.g. `"flying lotus"`, `"four tet"`)
now get Smart Title Case applied during normalisation — the same logic used for lowercase
folder names. DJ/EP/VA/UK/US acronyms preserved.

Previously: artist tags were only alias-mapped or "The"-prefix adjusted.
Now: all-lowercase tags → Smart Title Case → then alias and normalisation rules.

### Config changes

| Key | Change |
|-----|--------|
| `review_rules.auto_approve_threshold` | New. Triage AUTO/HOLD split threshold (default: same as `min_confidence_for_clean`) |
| `title_cleanup.strip_trailing_phrases` | Added `ftd` (scene release group noise suffix) |
| `reference.folder_alignment_noise_tokens` | New. Token list for folder alignment v2 noise stripping |

### Version source

`APP_VERSION` in `raagdosa.py` is now read from the installed package metadata via
`importlib.metadata.version("raagdosa")`. The single source of truth for the version is
`pyproject.toml`. Fallback to hardcoded `"8.0.0"` if not installed as a package.

---

## [7.0.0] — 2026-03-10

### Release summary

v7 is the **"Review Before Commit"** release. Instead of batch-processing everything automatically,
you can now step through each folder one at a time — see before/after state, confidence breakdown,
and approve, reject, override, or send to Review with a note. This is the CLI implementation of the
Intake Engine PRD's Staging concept.

This release also separates private paths from shareable config, renames Brain to Musical Reference,
ships 100+ built-in artist aliases, and adds community-shareable reference import/export.

Built in four phases:
- **Phase 1** — Private paths: filesystem paths moved to `paths.local.yaml`
- **Phase 2** — Smarter Review: per-factor score breakdown, review sidecars, structured rejection notes
- **Phase 3** — Interactive review: folder-by-folder approval with override actions
- **Phase 4** — Musical Reference: import/export for community-shareable knowledge

---

### New — Interactive folder-by-folder review mode

`raagdosa go --interactive` presents each folder with a full review card showing:

- Before/after folder names (source → proposed clean name)
- Confidence bar with per-factor breakdown (7 factors, colour-coded)
- Artist, album, year, genre, VA status, track count, format
- Route reasons explaining why the system chose Clean or Review

Actions at each folder (single keystroke + Enter):

| Key | Action | Description |
|-----|--------|-------------|
| `y` / Enter | Approve | Accept proposed routing, move the folder |
| `s` | Skip | Leave in source, optionally note why |
| `r` | Review | Send to Review with a required note (structured pick-list) |
| `a` | Set Artist | Override detected artist, re-route as single-artist album |
| `v` | Toggle VA | Flip VA/single-artist status, re-derive destination |
| `t` | Tracks | Show track listing with tag coverage summary |
| `q` | Stop | End session, keep all moves done so far |

Folders are sorted by confidence ascending (hardest decisions first). Use `--threshold 0.8` to
only review folders below a confidence score. Stop at any point — partial runs are safe and can
be resumed by re-running the same command.

### New — Private paths (`paths.local.yaml`)

All filesystem paths (source roots, clean mode, logging directories) are now stored in
`paths.local.yaml`, separate from `config.yaml`. This makes `config.yaml` safe to share
publicly — no risk of accidentally exposing your music folder paths.

- On first v7.0 run, paths are auto-extracted from `config.yaml` to `paths.local.yaml`
- `paths.local.yaml` is gitignored by default
- `paths.local.yaml.example` ships as a template
- Paths in config.yaml still work during the transition (overridden by paths.local.yaml)

### New — Musical Reference (renamed from Brain)

The "Brain" section is now called "Musical Reference" (`reference:` in config). This better
reflects its purpose — accumulated knowledge about artists, labels, and naming patterns that
can be shared with the community.

Ships with **100+ built-in artist aliases** covering:
- Hip-Hop/R&B (Jay-Z, MF DOOM, Notorious B.I.G., Tupac, Wu-Tang Clan, ...)
- Electronic/DJ (Aphex Twin, deadmau5, Four Tet, Flying Lotus, Kaytranada, ...)
- Rock/Alternative (AC/DC, Led Zeppelin, Radiohead, Arctic Monkeys, ...)
- Jazz/Soul/Funk (Thelonious Monk, D'Angelo, Erykah Badu, Jamiroquai, ...)
- Pop (Bjork, Beyonce, Sigur Ros, ...)
- Techno/House (Nina Kraviz, BadBadNotGood, Kerri Chandler, ...)

Migration: `brain:` keys in existing config.yaml are automatically loaded as `reference:`.

### New — Reference import/export (community sharing)

Share your musical reference with others — like adblock filter subscriptions for music knowledge.

```bash
raagdosa reference list                       # see what's in your reference
raagdosa reference export                     # export to reference_export.yaml
raagdosa reference export --section artist_aliases  # export one section
raagdosa reference import community_ref.yaml  # merge with conflict detection
```

Import merges new entries, flags conflicts (different canonical names for the same alias),
and tracks provenance (where each entry came from).

### New — Review sidecars and score breakdown

Every folder routed to Review/ now gets a `.raagdosa_review.json` sidecar file explaining WHY
it landed in Review:

```json
{
  "confidence": 0.48,
  "review_summary": "Confidence score below threshold. Weak: Tag readability (30%), Album/artist vote consensus (50%)",
  "route_reasons": ["low_confidence"],
  "confidence_factors": {
    "tag_coverage": 0.300,
    "dominance": 0.500,
    "title_quality": 0.750,
    "completeness": 1.000
  }
}
```

### New — Structured rejection reasons

When skipping or sending a folder to Review in interactive mode, reasons are captured from
a structured pick-list: `va-misclass`, `wrong-artist`, `bad-tags`, `incomplete-release`,
`duplicate`, `not-music`, `wrong-genre`, `needs-research`, or free-text custom notes.

Session notes are stored in `logs/sessions/<id>/review_notes.jsonl` for pattern learning.

### New — `--threshold` flag

`raagdosa go --interactive --threshold 0.7` only presents folders with confidence below 0.7
for interactive review. Everything above is processed normally.

### Config changes

| Change | Details |
|--------|---------|
| `brain:` → `reference:` | Renamed. Old `brain:` keys auto-migrate |
| `reference.artist_aliases` | 100+ built-in entries (was empty) |
| Paths → `paths.local.yaml` | Auto-extracted on first v7 run |
| `.raagdosa_review.json` | Written in Review/ folders (configurable) |

### Breaking changes

- Config file split: paths now live in `paths.local.yaml` (auto-migrated)
- `brain:` key renamed to `reference:` (auto-migrated)
- `--interactive` mode now uses the new folder-by-folder review instead of simple y/N prompts

---

## [6.0.0] — 2026-03-09

### Release summary

v6 introduces **library profiles with template-based folder structures**. Instead of a single
`{artist}/{album}` layout, you can now organise your library by genre, decade, BPM, musical key,
or record label — using 9 built-in templates or your own custom pattern. Each profile can have
its own template and library settings, so your archive collection and DJ prep folder can use
completely different structures. All settings live in `config.yaml` — set once, then just run.

This release was built in three phases:
- **Phase 1** — Foundation: template system, per-profile `library:` overrides, `resolve_library_path()` refactor
- **Phase 2** — Genre & decade tokens with 150+ entry genre normalisation map
- **Phase 3** — BPM bucketing, Camelot key mapping, and label normalisation

---

### New — Library template system

9 built-in templates cover the most common folder structures for collectors and DJs:

| Template | Pattern | Use case |
|----------|---------|----------|
| `standard` | `{artist}/{album}` | Default archive layout |
| `dated` | `{artist}/{year} - {album}` | Chronological discography |
| `flat` | `{artist} - {album}` | Minimal depth |
| `genre` | `{genre}/{artist}/{album}` | Multi-genre collections |
| `decade` | `{decade}/{genre}/{artist} - {album}` | Era-first browsing |
| `bpm` | `{bpm_range}/{artist} - {album}` | Tempo-sorted DJ library |
| `genre-bpm` | `{genre}/{bpm_range}/{artist} - {album}` | Open-format DJ |
| `genre-bpm-key` | `{genre}/{bpm_range}/{camelot_key}/{artist} - {album}` | Harmonic mixing |
| `label` | `{label}/{artist} - {album}` | Label-focused collectors |

Commands: `raagdosa template list`, `raagdosa template show <id>`.

### New — Per-profile library overrides

Each profile can now include a `library:` block that overrides the global library settings.
This means your archive can use `{artist}/{album}` while your DJ prep folder uses
`{genre}/{bpm_range}/{artist} - {album}` — all in the same config file.

```yaml
profiles:
  archive:
    source_root: ~/Music/Archive
    library:
      template: "{artist}/{year} - {album}"
  dj-usb:
    source_root: ~/Music/DJ-Prep
    library:
      template: "{genre}/{bpm_range}/{artist} - {album}"
```

### New — Genre normalisation (`genre_map`)

A 150+ entry mapping in `config.yaml` normalises raw genre tags to canonical folder names.
Case-insensitive. Unmapped genres pass through as-is.

Coverage: Electronic (25 variants), House (8), Techno (6), Drum & Bass (5), Ambient (4),
Hip-Hop (8), Jazz (6), Soul & Funk (5), Reggae & Dub (4), World (5), Rock (6),
Classical (4), Soundtrack (3), Pop (4), Metal (4), Folk (3), Experimental (4), Trance (4).

### New — BPM bucketing (`{bpm_range}` token)

Tracks' BPM values are collected per folder (using median for robustness) and bucketed into
configurable ranges. Named zones are checked first, then numeric ranges:

```yaml
bpm_buckets:
  width: 10
  named_zones:
    "Downtempo":    [60, 99]
    "House":        [120, 132]
    "Techno":       [133, 145]
    "D&B / Jungle": [160, 180]
```

A 128 BPM album → `House/`. A 150 BPM album → `150-159/`. Configurable fallback: `_Unknown BPM`.

### New — Camelot key mapping (`{camelot_key}` token)

Raw musical key tags are converted to Camelot wheel notation (1A–12B), the standard system
DJs use for harmonic mixing. Handles multiple input formats:

- Standard: `Am` → `8A`, `C` → `8B`, `F#m` → `11A`
- Long form: `A minor` → `8A`, `C major` → `8B`
- Case-insensitive: `am` → `8A`, `ebm` → `2A`
- Enharmonic equivalents: `G#m` = `Abm` → `1A`, `F#` = `Gb` → `2B`

All 24 keys mapped. Configurable fallback: `_Unknown Key`.

### New — Label normalisation (`{label}` token)

Record label tags are read from `organization`, `label`, `publisher`, and `TPUB` fields.
Corporate suffixes are stripped: Records, Recordings, Music, Entertainment, Ltd, Inc, LLC.
Handles stacked suffixes: `Sub Pop Records LLC` → `Sub Pop`.

Configurable fallback: `_Unknown Label`.

### New — Tag reading expanded

`read_audio_tags()` now reads genre, BPM, key, and label from all audio formats:

| Tag | Keys searched |
|-----|---------------|
| `genre` | `genre`, `TCON` |
| `bpm` | `bpm`, `tbpm`, `TBPM` |
| `key` | `initialkey`, `key`, `tkey`, `TKEY` |
| `label` | `organization`, `label`, `publisher`, `TPUB` |

### New — Vote counting for BPM, key, and label

`build_folder_proposal()` now collects BPM (median of all tracks), key (plurality vote),
and label (plurality vote) alongside existing album/artist/genre votes. All values are
passed to `resolve_library_path()` for template substitution.

### New — Tag coverage report after scan

After scanning, RaagDosa detects which tokens your active template uses and reports per-token
tag coverage with colour-coded bars (green ≥80%, yellow ≥50%, red <50%).

### New — `--template` flag on profile commands

`raagdosa profile add --template genre` and `raagdosa profile set --template dated` bind a
built-in template to a profile. The template ID is stored in the profile's `library.template` key.

### Config keys added

| Key | Default | Description |
|-----|---------|-------------|
| `tags.label_keys` | `[organization, label, publisher, TPUB]` | Tag keys to read for label |
| `bpm_buckets.width` | `10` | Numeric BPM bucket width |
| `bpm_buckets.named_zones` | 5 zones | Named BPM ranges (checked first) |
| `genre_map` | 150+ entries | Raw genre → canonical name mapping |
| `library.bpm_fallback` | `_Unknown BPM` | Folder name when BPM tag is missing |
| `library.key_fallback` | `_Unknown Key` | Folder name when key tag is missing |
| `library.label_fallback` | `_Unknown Label` | Folder name when label tag is missing |

---

## [5.5.0] — 2026-03-08

### Release summary

v5.5 is the structural housekeeping release. All output now lives under a single
`raagdosa/` wrapper folder co-located with your source music. Logs are resolved
relative to that wrapper, not your shell's working directory — so on a separate
drive the full volume path makes every log file unambiguous. A config-schema
mismatch causing `track_history_log` / `track_skipped_log` KeyErrors is fixed.
EP detection minimum is confirmed at 2 tracks. Previously hardcoded values
(DJ database patterns, sidecar file extensions, system folder skip-list) are
now first-class config keys so you can override them without touching the script.

---

### New — raagdosa wrapper folder (`wrapper_folder_name`)

All output folders now nest under a single wrapper rather than sitting directly
inside `source_root`:

```
Before (v5.0):
  /Volumes/bass/Test/
    Clean/Albums/
    Clean/Tracks/
    Review/Albums/
    Review/Duplicates/

After (v5.5):
  /Volumes/bass/Test/
    raagdosa/
      Clean/Albums/
      Clean/Tracks/
      Review/Albums/
      Review/Duplicates/
      logs/
        history.jsonl
        skipped.jsonl
        track-history.jsonl
        track-skipped.jsonl
        sessions/
          2026-03-08_14-30_incoming_test/
            proposals.json
            report.txt / .csv / .html
```

Config key: `profiles.<n>.wrapper_folder_name` (default: `raagdosa`).
Rename it to anything you want. The wrapper folder is automatically excluded
from scanning so its contents are never processed as source music.

### New — Logs co-located with source via `setup_logging_paths()`

All logging paths are now resolved **relative to the wrapper folder**, not
relative to the shell's current working directory. A new `setup_logging_paths()`
function is called at the start of every command that has profile context. It
mutates `cfg["logging"]` in-place with absolute paths so all downstream code
works unchanged.

On a drive with a long path (`/Volumes/bass/Test/raagdosa/logs/`), the location
is unambiguous — you always know which source the logs belong to.

The config `logging.*` keys still control the folder and file **names** inside
the logs directory. You rarely need to change them.

### Fix — Config schema mismatch: `track_history_log` / `track_skipped_log`

`rename_tracks_in_clean_folder()` accessed `cfg["logging"]["track_history_log"]`
and `cfg["logging"]["track_skipped_log"]` directly. These keys were missing from
some `config.yaml` files generated before v4.3, causing a `KeyError` at runtime.

Both keys are now guaranteed present after `setup_logging_paths()` resolves them.
They are also included in the `init`-generated config template and the
`config.yaml` reference.

### Fixed — EP minimum tracks: confirmed 2

`ep_detection.min_tracks` is confirmed at `2`. A 2-track release labelled "EP"
in its folder name or tags is correctly classified as an EP and routed to
`Clean/` at normal confidence. Previously the config said 2 but the code
defaulted to 3 in some paths — now consistent everywhere.

### New — Hardcoded values moved to config

Three previously hardcoded Python sets are now first-class `config.yaml` keys:

- `scan.skip_sidecar_extensions` — files silently skipped during audio scan
  (`.sfk`, `.asd`, `.reapeaks`, `.pkf`, `.db`, `.lrc`). Add your own extensions.
- `scan.skip_system_folders` — directory names always skipped during walk
  (`__MACOSX`). Add any folder names you want globally excluded.
- `dj_safety.database_patterns` — patterns used to detect DJ library databases
  (`rekordbox.xml`, `_Serato_`, etc.). Add your own if needed.

The Python defaults are preserved as fallbacks so existing configs that don't
have these keys continue to work without change.

### Improved — Undo by folder (`--folder` flag)

`raagdosa undo --folder <name>` now works for **both** folder-level and
track-level undo (previously only track-level supported `--folder`):

```
# Undo all folder moves where source path contains "Burial - Untold"
raagdosa undo --folder "Burial - Untold"

# Undo all track renames inside that clean folder
raagdosa undo --tracks --folder "Burial - Untold"
```

History and undo commands now also call `_resolve_log_paths_from_active_profile()`
so they find the correct log files inside the wrapper folder automatically.

---



### Release summary

v5 is the intelligence consolidation release. It merges all work from v4.3 (folder
pre-processor, Smart Title Case, label-as-albumartist safeguard, config BRAIN/SETTINGS
split) with a second layer of signal improvements: domain/URL stripping at every pipeline
stage, stronger VA/mix detection using compilation and genre tags, EP detection that works
on 2-track labelled folders, year recovery from comment tags, a broader set of promo
phrase stripping, diacritic-safe artist matching with alias documentation, and the removal
of "flip" from default mix suffix keywords.

Validated against real library data: 968 folders, 13,588 audio files.

---

### New — Folder pre-processor (28-step pipeline, was 13)

Steps 1–13 were the v4.1 baseline. Steps 14–28 are new:

- **Scene release group suffix strip** — `Artist-Album-WEB-2023-FTD` → `Artist - Album`. Handles full slugs with underscores. Confirmed in 157 folders (16%) of real library.
- **Double-dash slug normalisation** — `Artist--Album_Name` → `Artist - Album Name`. Confirmed in 11 folders (1.1%) in session 2.
- **Tilde separator normalisation** — `Album ~ Remixes` → `Album - Remixes`.
- **Curly brace noise strip** — `{Digital Media}`, `{MFM031}` removed.
- **Known-label bracket strip** — `[warp 2008]`, `[zencd178]` removed. Configurable via `brain.known_labels`.
- **Format bracket/paren strip** (up to 4 stacked) — `( FLAC )`, `[MP3]`, `[16Bit-44.1kHz]`.
- **CD + bitrate slug strip** — `2012 cd 320 tmgk` tail patterns removed.
- **Trailing catalog code strip** — `(CA046)`, `[KOSA043]`, `{MFM031}` at end of name.
- **Duplicate year collapse** — `2010 - Río Arriba (2010)` → `2010 - Río Arriba`.
- **Mid-name paren year** — `Artist - (2017) Album` → `Artist - Album`, year extracted.
- **Mid-name bracket year** — `aukai.  [2016] aukai` → `aukai. - aukai`, year extracted. Handles double-space before bracket (Traxsource artefact).
- **4-dash label-year-artist-album** — `Cosmovision Records - 2024 - Cigarra - Limbica` → `Cigarra - 2024 - Limbica`. Guarded: only fires when first segment contains a label keyword or is in `brain.known_labels`.
- **Trailing type annotation strip** — `[Anthology]`, `[album]`, `[collection]` removed.
- **Domain/URL strip** (step 26, v5) — website noise removed at any position (leading, trailing, bracketed, mixed case). `Www.ElectronicFresh.Com - Artist` → `Artist`. `[www.freestep.net] Artist` → `Artist`. Covers 33 TLDs plus bare `www.` patterns.
- **Final cleanup** — orphaned trailing separators, double spaces, orphaned year digits abutted to words.

### New — Smart Title Case for all-lowercase folders

- ~10% of real-library folders are entirely lowercase (slug/NFO origin). `flying lotus - los angeles` → `Flying Lotus - Los Angeles`.
- Rules: small prepositions lowercase in mid-title; all-caps acronyms preserved (`DJ`, `EP`, `LP`, `VA`, `UK`, `US`, `LA`, `NYC`, `MC`); trailing artist dots preserved (`aukai.`); accented characters respected.
- Config: `title_case.auto_titlecase_lowercase_folders: true` (default on).

### New — Track filename parsing extensions

- **Hash/checksum tail strip** — `07-track-cd4051c3` → `07-track` (8–12 hex chars at stem tail).
- **`NNN/NN` tag-style track number** — `001/12 - Title` prefix handled.
- **`NN. –` dot-dash format** — `02. - Artist - Title` stripped correctly.

### New — Label-as-albumartist safeguard (Safeguard D)

- Detects record label keywords in albumartist tag: Records, Discos, Recordings, Label, Music Group, Inc., Ltd., Disques. Confirmed in 37 folders (3.7%) in session 2.
- When detected and track-level artist is dominant at ≥70%, re-derives albumartist from track tags.
- Config: `brain.known_labels`.

### New — Domain/URL stripping in tag voting (v5)

- Album tags and track titles have domains stripped before voting. `"Album www.electronicfresh.com"` votes as `"Album"`.
- `_PROMO_WATERMARK` expanded to catch: `free music`, `hot new music`, `ncs release`, `monstercat`, `edm sauce`, `promoted by`, `no copyright music`, `buy on beatport`, `buy on itunes`, `subscribe to our`, `download free`, `listen free`, `get it free`.

### New — Compilation tag → VA detection (v5)

- `compilation` / `TCMP` = `1` on majority of tracks now fires in `classify_folder_content()`.
- Prevents misclassification when the compilation flag is set but albumartist is absent or non-standard.

### New — Genre tag as release type signal (v5)

- `genre` tag containing "EP" on ≥⅓ of tracks → classifies folder as EP.
- `genre` tag containing "Single" on ≥⅓ of tracks → classifies folder as Single.

### New — EP detection: 2-track bypass (v5)

- Folders below `scan.min_tracks` are no longer silently dropped if the folder name contains `\b(ep|e\.p\.|single|7\s*inch)\b`. A 2-track folder named "Artist - Debut EP" is now processed and classified correctly.

### New — Year recovery from comment tag (v5)

- When year tag is empty but the `comment` / `description` field contains a plausible 4-digit year (1950–2040), that year is used as a fallback.
- Only applied when a single unambiguous year is found — prevents false matches on bitrate strings and catalog numbers.

### New — album tag as VA signal (v5)

- If the album tag itself normalises to a VA match string ("various artists", "va", etc.), it counts as an additional VA signal.

### New — Expanded mix/VA folder keywords (v5)

Added to `_MIX_FOLDER_KW`: `best tracks`, `selected tracks`, `sounds of`, `in the style of`, `tribute to`, `playlist`.

### Changed — "flip" removed from default mix suffix keywords

"Flip" is a production technique term, not a mix format variant. Removed from `mix_info.detect_keywords` defaults. Add manually if your library uses it as a format tag.

### New — Expanded strip_trailing_phrases defaults

Added: `ncs release`, `monstercat`, `edm sauce`, `promoted by`, `released by`, `buy on beatport`, `buy on itunes`, `no copyright music`, `music promotion`, `subscribe to`, `follow on`, `follow us`, `free track`, `free music`.

### New — Config SETTINGS/BRAIN split

- `brain:` section added with clear zone separator comments.
- `brain.artist_aliases` — canonical artist display form map. Fuzzy matching handles `Björk ↔ Bjork`, `MØ ↔ MO`, `Sigur Rós ↔ Sigur Ros` automatically; aliases control only the folder name display form.
- `brain.known_labels` — label names for albumartist safeguard.
- `brain.va_rescue_prefixes` — artist prefixes that should never be classified as VA.
- `brain.noise_patterns` — for future `raagdosa learn --extract` output.
- `source_root` default changed to `~/Music/Incoming` (no personal paths in shipped config).

### Config keys added

| Key | Default | Description |
|-----|---------|-------------|
| `title_case.auto_titlecase_lowercase_folders` | `true` | Smart Title Case on all-lowercase folders |
| `brain.known_labels` | `[]` | Label names for albumartist safeguard |
| `brain.artist_aliases` | `{}` | Canonical artist display forms |
| `brain.va_rescue_prefixes` | `[]` | Prefixes that should never be classified as VA |
| `brain.noise_patterns` | `[]` | Learned noise patterns |

### Golden tests (all passing)

| Input | Output |
|-------|--------|
| `13th_Ward_Social_Club-Afrobeat_Vol_1-WEB-2023-FTD` | `13th Ward Social Club - Afrobeat Vol 1` |
| `Anka Foh - 2021 - Koundary ( FLAC )` | `Anka Foh - 2021 - Koundary` |
| `2010 - Río Arriba (2010)` | `2010 - Río Arriba` |
| `aukai.  [2016] aukai` | `aukai. - aukai` (year=2016) |
| `Tinariwen--Alkhar_Dessouf_Remix-WEB-2022-OMA` | `Tinariwen - Alkhar Dessouf Remix` |
| `Tropical Twista Records - 2024 - Cigarra - Limbica` | `Cigarra - 2024 - Limbica` |
| `Www.ElectronicFresh.Com - Artist - Title` | `Artist - Title` |
| `[www.freestep.net] Artist - Title` | `Artist - Title` |
| `flying lotus - los angeles` | `Flying Lotus - Los Angeles` |
| `dj shadow - endtroducing` | `DJ Shadow - Endtroducing` |
| `07-chris_tiebo-bhedana-cd4051c3` | art=`chris tiebo` title=`bhedana` |

---

## [3.5.1] — 2026-03-06

### Release summary

Performance patch. Same-filesystem moves are now atomic renames (~1ms vs 1s/album),
scan and apply overlap via a streaming pipeline, and a persistent tag cache eliminates
mutagen reads for unchanged files on subsequent runs.

### Performance

- **Same-filesystem fast path** — `os.rename()` instead of copytree → verify → rmtree. Atomic, ~1ms per folder. 500 albums: ~8 min → ~0.5 sec.
- **Streaming scan→apply pipeline** — configurable batches (`scan.streaming_batch_size`, default 50). First folder moves within seconds.
- **Persistent tag cache** — `logs/tag_cache.json`, keyed by `(path, mtime)`. Warm runs skip mutagen entirely.
- **Parallel scan workers** — `ThreadPoolExecutor`, ~7× speedup vs sequential.
- **Thread-safe progress bar** — real-time rate (folders/s) and ETA.
- **move_method logged** — `rename|copy` and elapsed ms in history entries.

### Config

`scan.workers`, `scan.tag_cache_enabled`, `scan.streaming_batch_size`

### Commands

`cache`, `cache clear`, `cache evict`

---

## [3.5.0] — 2026-03-06

### Release summary

Intelligence layer: deeper naming logic, multi-factor confidence system, mix/EP
classification, seven new commands.

### New

- EP detection (3–6 tracks → EP). Garbage naming pipeline (bracket stripper, promo watermark). Mojibake detection. Vinyl track notation (A1/B2 → absolute track number). ALL CAPS / all-lowercase normalisation. Bracket content classifier.
- Per-folder `.raagdosa` override file. Disc indicator stripping. Display name noise stripping.
- 7-factor confidence score with named breakdown: `dominance` (0.40) · `tag_coverage` (0.15) · `title_quality` (0.12) · `completeness` (0.12) · `filename_consistency` (0.10) · `aa_consistency` (0.06) · `folder_alignment` (0.05).
- Mix/chart classifier. Mix routing to `Clean/_Mixes/`. `raagdosa extract --by-artist`. `raagdosa compare --folder A B`.
- Commands: `orphans`, `artists --list`, `artists --find`, `review-list`, `review-list --older-than`, `clean-report`, `show --tracks`, `diff`.

---

## [3.0.0] — 2026-03-05

### Release summary

First public release. Core architecture stable.

### New

- `library.template` system (`{artist}/{album}` default). FLAC segregation.
- Full artist normalisation pipeline: Unicode NFC → char map → alias map → hyphen normalisation → "The" prefix policy. Jaccard fuzzy dedup threshold.
- `raagdosa go --since last_run`. Manifest-based exclusion.
- `show`, `verify`, `learn`, `status`, `init`, `resume` commands.
- TXT + CSV + HTML session reports.
- Live progress bar, colour-coded output. Graceful Ctrl+C stop.

---

## [2.0.0] — 2026-03-04

Internal hardening release. Fixed display-case recovery, confidence formula, Clean/Review skip logic, slash sanitisation. Added copy-verify-delete, disk space check, DJ database detection, clean manifest, format duplicate detection.

---

## [1.0.0] — 2026-03-03

Initial internal release. Core scan → vote → propose → move → rename pipeline.
