"""
RaagDosa artists — artist normalization, connector handling, alias lookup, comparison.

Layer 2: imports from tags (L1) for normalize_unicode. No side effects, no terminal output.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, List, Optional

from raagdosa.tags import normalize_unicode

from raagdosa.naming import _smart_title_case_v43


# ─────────────────────────────────────────────
# Connector normalisation
# ─────────────────────────────────────────────
_CONNECTOR_NORM_PAT = re.compile(
    r'\s*[&+×]\s*|\s+(?:and|vs\.?|versus|feat\.?|featuring|ft\.?)\s+', re.I)


def normalize_connectors(s: str) -> str:
    """Normalise collaboration connectors: &, +, ×, and, vs, feat → ' & '."""
    return _CONNECTOR_NORM_PAT.sub(" & ", s).strip()


# ─────────────────────────────────────────────
# Full artist normalisation pipeline
# ─────────────────────────────────────────────
def normalize_artist_name(name: str, cfg: Dict[str, Any]) -> str:
    """
    Full normalisation pipeline:
    1. Unicode NFC
    2. Optional unicode char map (MØ → MO etc — user-defined)
    3. ALL-CAPS → Title Case
    4. Alias map lookup (Jay Z / JAYZ / jay-z → Jay-Z)
    5. Hyphen variant normalisation
    6. "The" prefix policy: keep-front | move-to-end | strip
    """
    if not name:
        return name
    acfg = cfg.get("artist_normalization", {})
    if not acfg.get("enabled", True):
        return name

    s = normalize_unicode(name.strip())

    # Unicode char map (opt-in)
    for src_c, dst_c in (acfg.get("unicode_map", {}) or {}).items():
        s = s.replace(src_c, dst_c)

    # ALL CAPS → Title Case
    words = s.split()
    if words and all(w.isupper() for w in words if len(w) > 2):
        small = {"a", "an", "the", "and", "but", "or", "for", "of", "in", "at", "to", "by", "vs"}
        s = " ".join(
            w.capitalize() if i == 0 or w.lower() not in small else w.lower()
            for i, w in enumerate(words)
        )
        words = s.split()

    # all-lowercase → Smart Title Case
    alpha_words = [w for w in words if any(c.isalpha() for c in w)]
    if alpha_words and all(w.islower() for w in alpha_words):
        s = _smart_title_case_v43(s, cfg)
        words = s.split()

    # Alias map (case-insensitive exact match wins immediately)
    aliases: Dict[str, str] = acfg.get("aliases", {}) or {}
    ref_aliases: Dict[str, str] = cfg.get("reference", {}).get("artist_aliases", {}) or {}
    merged_aliases = {**ref_aliases, **aliases}
    for alias_key, canonical in merged_aliases.items():
        if alias_key.lower() == s.lower():
            return canonical

    # Connector variants: +, and, ×, feat, ft → &
    s = normalize_connectors(s)
    s = re.sub(r'\s*&\s*', ' & ', s)

    # Hyphen variants → ASCII hyphen
    if acfg.get("normalize_hyphens", True):
        s = re.sub(r"[\u2013\u2014\u2010\u2212]", "-", s)

    # "The" prefix
    the_policy = acfg.get("the_prefix", "keep-front")
    m = re.match(r"^[Tt]he\s+(.+)$", s)
    if m:
        base = m.group(1)
        if the_policy == "move-to-end":
            s = f"{base}, The"
        elif the_policy == "strip":
            s = base

    return s


# ─────────────────────────────────────────────
# Artist comparison
# ─────────────────────────────────────────────
def artists_are_same(a: str, b: str, cfg: Dict[str, Any]) -> bool:
    """True if two artist names are considered the same after normalisation."""
    thresh = float(cfg.get("artist_normalization", {}).get("fuzzy_dedup_threshold", 0.92))
    an = normalize_unicode(a.strip().lower())
    bn = normalize_unicode(b.strip().lower())
    if an == bn:
        return True
    an = normalize_connectors(an)
    bn = normalize_connectors(bn)
    if an == bn:
        return True
    ac = re.sub(r"^the\s+", "", an)
    bc = re.sub(r"^the\s+", "", bn)
    if ac == bc:
        return True
    # Collab order: "A & B" == "B & A"
    if " & " in ac and " & " in bc:
        a_parts = sorted(p.strip() for p in ac.split(" & "))
        b_parts = sorted(p.strip() for p in bc.split(" & "))
        if a_parts == b_parts:
            return True
    # Comma-separated collabs
    if ("," in ac or "," in bc) and (" & " in ac or " & " in bc or "," in ac and "," in bc):
        def _split_collab(s: str) -> List[str]:
            return sorted(p.strip() for p in re.split(r'\s*[,&]\s*', s) if p.strip())
        if _split_collab(ac) == _split_collab(bc):
            return True
    aset = set(ac.split())
    bset = set(bc.split())
    if not aset or not bset:
        return False
    return len(aset & bset) / len(aset | bset) >= thresh
