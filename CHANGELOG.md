# Changelog

All notable changes to RaagDosa are documented here.  
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

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
