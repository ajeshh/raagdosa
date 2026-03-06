# RaagDosa CLI Commands (v0.2 draft)

This file documents the **human-friendly CLI commands** for RaagDosa, including the new “no explicit scan needed” workflow and folder/track cleanup options.

---

## Mental model

- **Scan** = preview / proposals (optional)
- **Apply** = execute using an existing proposals file (optional workflow)
- **Go / Run / Folders / Tracks** = **do the work** (internally scans first, then applies)

RaagDosa always writes:
- session outputs under `logs/sessions/<session_id>/`
- append-only history logs for undo
- readable session `report.txt`

---

## Common one-liners

### Do everything (folders → then tracks), with prompts
```bash
raagdosa run --interactive
```

### Do everything, mostly automatic
```bash
raagdosa go
```

### Do everything, but dry-run (generate reports, do not rename/move)
```bash
raagdosa go --dry-run
```

---

## Profiles (CRUD)

Profiles live in `config.yaml` under `profiles:` and set:
- `source_root`
- `clean_folder_name` (default `Clean`)
- `review_folder_name` (default `Review`)
- clean placement mode (`inside_root` vs `inside_parent`)

### List profiles
```bash
raagdosa profile list
```

### Show one profile
```bash
raagdosa profile show incoming
```

### Add a profile
```bash
raagdosa profile add bandcamp --source "/Music/Bandcamp" --clean-mode inside_root --clean-folder Clean
```

### Update a profile
```bash
raagdosa profile set incoming --source "/Users/ajesh/Music/Incoming"
```

### Set active profile
```bash
raagdosa profile use incoming
```

### Delete a profile
```bash
raagdosa profile delete bandcamp
```

---

## “Do the work” commands (no explicit scan required)

These commands **internally scan and then apply**, producing a new session each run.

### Go: default end-to-end
Runs:
1) folder move/rename to `Clean/` or `Review/`  
2) track renames **only inside `Clean/`**

```bash
raagdosa go
```

Common flags:
```bash
raagdosa go --interactive
raagdosa go --dry-run
raagdosa go --profile incoming
```

### Run: explicit control (same as go, but more flags)
```bash
raagdosa run --profile incoming --interactive
```

### Folders: folder pass only
Moves/renames folders into `Clean/` (or `Review/`) but does not rename files.

```bash
raagdosa folders
```

Flags:
```bash
raagdosa folders --interactive
raagdosa folders --dry-run
```

### Tracks: track pass only (operates on Clean output)
Renames audio files inside `Clean/` (skips `Review/` by default).

```bash
raagdosa tracks
```

Flags:
```bash
raagdosa tracks --interactive
raagdosa tracks --dry-run
```

---

## Preview workflow (optional)

Use this when you want to inspect proposals before applying.

### Scan (preview)
Generates:
- `logs/sessions/<session_id>/proposals.json`
- `logs/sessions/<session_id>/report.txt` (if enabled)

```bash
raagdosa scan
```

Optional:
```bash
raagdosa scan --profile incoming
raagdosa scan --out /tmp/proposals.json
```

### Apply (execute from proposals.json)
```bash
raagdosa apply --last-session --interactive
```

Or specify a proposals file:
```bash
raagdosa apply logs/sessions/<session_id>/proposals.json --interactive
```

---

## History / reporting

### Show recent applied actions
```bash
raagdosa history --last 50
```

Filter by session:
```bash
raagdosa history --session 2026-03-05_10-48-07_1653
```

Filter by substring match:
```bash
raagdosa history --match "Moon Safari"
```

---

## Undo (folders + tracks)

RaagDosa keeps **separate undo streams** so you can undo folder moves without touching track renames and vice versa.

### Undo folders (default undo stream)
By action id:
```bash
raagdosa undo --id <action_id>
```

By session:
```bash
raagdosa undo --session <session_id>
```

By original path:
```bash
raagdosa undo --from-path "/path/to/original/folder"
```

### Undo tracks only
By track action id:
```bash
raagdosa undo --tracks --id <track_action_id>
```

By track session:
```bash
raagdosa undo --tracks --session <session_id>
```

By folder (undo all track renames inside a folder):
```bash
raagdosa undo --tracks --folder "/path/to/Clean/AlbumArtist - Album (YYYY)"
```

---

## Folder → Clean vs Review routing

Folder proposals are routed to:

- `Clean/` when confident and non-duplicative
- `Review/` when questionable or ambiguous (safe default)

Typical Review triggers:
- low confidence (below `review_rules.min_confidence_for_clean`)
- duplicate target folder names in the same run (same format suffix)
- collisions (depending on policy)
- unreadable tags ratio too high
- missing/weak dominance signals

---

## Track renaming behavior summary

Track renames happen **after folder move**, and only for folders that landed in `Clean/`.

Naming end states:
- Album: `01 - Title.ext` (disc-aware if multi-disc)
- Various album: `01 - Artist - Title.ext`
- Mixed bag: `Artist - Title.ext`

Conservative defaults:
- rename only `.mp3`, `.flac`, `.m4a`
- skip `.wav` (and optionally `.aiff`)
- skip if tags insufficient / uncertain
- title cleanup removes common trailing junk (domains, “uploaded by”, “official video”, bitrate flags, etc.)
- keep meaningful DJ suffixes like **Original Mix**, **Remix**, **Edit**, **Extended Mix**, etc.

---

## Notes

- If you created an alias, you can run `raagdosa` directly.
- Otherwise you can run via:
  ```bash
  python3 raagdosa.py <command>
  ```
- Every run is sessioned under `logs/sessions/` so even 100 runs/day stay separated.

