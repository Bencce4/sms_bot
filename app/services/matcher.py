from __future__ import annotations
import re
import unicodedata
import pandas as pd
from rapidfuzz import fuzz

CANON_PEOPLE_COLS = {
    "city": ["Miestas", "iš kokio miesto", "is kokio miesto"],
    "prof": ["Specialybė", "kvalifikacija", "specialybe", "kvalifikacija "],
    "phone": ["Tel. nr", "Telefonas", "tel", "telefonas"],
}

CANON_PROJECT_COLS = {
    "city": ["Miestas", "miestas"],
    "prof": ["Specialybė", "specialybe"],
    "active": ["Aktualūs", "Aktualus", "aktualūs", "aktualus"],
}

NATIONWIDE = {"lietuva", "lt", "visa lietuva"}

def _strip_accents(s: str) -> str:
    s = str(s)
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

def _norm_space(s: str) -> str:
    if pd.isna(s):
        return ""
    return " ".join(str(s).strip().split())

def _norm_fold(s: str) -> str:
    return _strip_accents(_norm_space(s)).lower()

def _base_city(s: str) -> str:
    t = _norm_fold(s)

    rules = [
        ("aus", "us"),   # vilniaus -> vilnius
        ("io", "ys"),    # panevezio -> panevezys
        ("os", "a"),     # klaipedos -> klaipeda
        ("o", "as"),     # kauno -> kaunas
        ("es", "e"),     # marijampoles -> marijampole
    ]
    for suf, repl in rules:
        if t.endswith(suf) and len(t) > len(suf) + 1:
            return t[:-len(suf)] + repl
    return t

def _city_equiv(a: str, b: str) -> bool:
    if not a or not b:
        return False
    a0 = _base_city(a)
    b0 = _base_city(b)
    if a0 == b0:
        return True
    return fuzz.ratio(a0, b0) >= 92

def _city_matches(worker_city: str, project_city: str) -> bool:
    w = _norm_fold(worker_city)
    p = _norm_fold(project_city)
    if p in NATIONWIDE:
        return True
    return _city_equiv(w, p)

def _pick_first(df: pd.DataFrame, names: list[str]) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    return None

def _clean_phone(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"[^\d+]", "", s)
    return s

def _norm(s) -> str:
    if pd.isna(s):
        return ""
    return str(s).strip()

def _eq_ci(a: str, b: str) -> bool:
    return _norm_fold(a) == _norm_fold(b)

def build_matches(people: pd.DataFrame, projects: pd.DataFrame) -> pd.DataFrame:
    p_city = _pick_first(people, CANON_PEOPLE_COLS["city"])
    p_prof = _pick_first(people, CANON_PEOPLE_COLS["prof"])
    p_phone = _pick_first(people, CANON_PEOPLE_COLS["phone"])

    if not all([p_city, p_prof, p_phone]):
        return pd.DataFrame(columns=["match_id", "Miestas", "Specialybė", "Tel. nr", "sms_text"])

    pr_city = _pick_first(projects, CANON_PROJECT_COLS["city"])
    pr_prof = _pick_first(projects, CANON_PROJECT_COLS["prof"])
    pr_active = _pick_first(projects, CANON_PROJECT_COLS["active"])

    if not all([pr_city, pr_prof, pr_active]):
        return pd.DataFrame(columns=["match_id", "Miestas", "Specialybė", "Tel. nr", "sms_text"])

    proj = projects.copy()
    proj["_active"] = pd.to_numeric(proj[pr_active], errors="coerce").fillna(0).astype(int)
    proj = proj[proj["_active"] > 0].copy()

    rows = []
    match_id = 1

    for _, r in people.iterrows():
        city = _norm(r.get(p_city, ""))
        prof = _norm(r.get(p_prof, ""))
        phone = _clean_phone(r.get(p_phone, ""))

        if not prof or not phone:
            continue

        cand = proj[
            proj.apply(
                lambda x: _eq_ci(x.get(pr_prof, ""), prof)
                and _city_matches(city, x.get(pr_city, "")),
                axis=1,
            )
        ]

        if cand.empty:
            continue

        pr = cand.iloc[0]
        proj_city = _norm(pr.get(pr_city, ""))

        sms = (
            f"Sveiki! Turime darbo pasiūlymą ({prof}) "
            f"{proj_city if proj_city else ''}. Jei domina, atsakykite į šią žinutę."
        ).strip()

        rows.append(
            {
                "match_id": match_id,
                "Miestas": city,
                "Specialybė": prof,
                "Tel. nr": phone,
                "sms_text": sms,
            }
        )
        match_id += 1

    return pd.DataFrame(rows, columns=["match_id", "Miestas", "Specialybė", "Tel. nr", "sms_text"])