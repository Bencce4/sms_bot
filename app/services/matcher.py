import re
import pandas as pd

# Expected columns (LT):
# People: Vardas pavardė, Tel. nr, Iš kokio miesto, Kvalifikacija
# Projects: Užsakovas, Asmuo, Telefonas, Specialybė, Miestas, Objektas, Data, nuo, Data, iki

PEOPLE_NAME = "Vardas pavardė"
PEOPLE_PHONE = "Tel. nr"
PEOPLE_CITY = "Iš kokio miesto"
PEOPLE_PROF = "Kvalifikacija"

PROJ_CLIENT = "Užsakovas"
PROJ_CONTACT = "Asmuo"
PROJ_PHONE = "Telefonas"
PROJ_PROF = "Specialybė"
PROJ_CITY = "Miestas"
PROJ_SITE = "Objektas"
PROJ_FROM = "Data, nuo"
PROJ_TO   = "Data, iki"

def _norm(s):
    if pd.isna(s):
        return ""
    return re.sub(r"\s+", " ", str(s)).strip().casefold()

def _norm_prof(s):
    s = _norm(s)
    s = re.sub(r"[^a-ząčęėįšųūž0-9\s/+-]", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def _format_date(d):
    if pd.isna(d) or d == "":
        return ""
    try:
        return pd.to_datetime(d).date().isoformat()
    except Exception:
        return str(d)

def build_matches(people_df: pd.DataFrame, projects_df: pd.DataFrame) -> pd.DataFrame:
    p = people_df[[PEOPLE_NAME, PEOPLE_PHONE, PEOPLE_CITY, PEOPLE_PROF]].copy()
    j = projects_df[[PROJ_CLIENT, PROJ_CONTACT, PROJ_PHONE, PROJ_PROF, PROJ_CITY, PROJ_SITE, PROJ_FROM, PROJ_TO]].copy()

    p["city_n"] = p[PEOPLE_CITY].map(_norm)
    p["prof_n"] = p[PEOPLE_PROF].map(_norm_prof)
    j["city_n"] = j[PROJ_CITY].map(_norm)
    j["prof_n"] = j[PROJ_PROF].map(_norm_prof)

    exact = p.merge(j, how="inner",
                    left_on=["city_n","prof_n"], right_on=["city_n","prof_n"],
                    suffixes=("_cand","_proj"))
    exact["match_type"] = "exact"

    # fuzzy: same city, profession substring or token overlap
    def prof_match(a, b):
        if not a or not b:
            return False
        if a in b or b in a:
            return True
        sa, sb = set(a.split()), set(b.split())
        inter = len(sa & sb)
        return inter >= max(1, min(len(sa), len(sb))//2)

    fuzzy_rows = []
    for _, row_p in p.iterrows():
        pool = j[j["city_n"] == row_p["city_n"]]
        for _, row_j in pool.iterrows():
            if prof_match(row_p["prof_n"], row_j["prof_n"]):
                fuzzy_rows.append({**row_p.to_dict(), **row_j.to_dict()})
    fuzzy = pd.DataFrame(fuzzy_rows)
    if not fuzzy.empty:
        fuzzy["match_type"] = "fuzzy_prof_city_exact"

    allm = pd.concat([exact, fuzzy], ignore_index=True) if (not exact.empty or not fuzzy.empty) else pd.DataFrame(columns=list(p.columns)+list(j.columns))
    if allm.empty:
        return allm

    # human SMS preview (LT)
    def first_name(full):
        if isinstance(full, str) and full.strip():
            return full.split()[0]
        return "Sveiki"

    allm["sms_text"] = allm.apply(lambda r: (
        f"Sveiki, {first_name(r.get(PEOPLE_NAME,''))}! "
        f"Siūlomas {r.get(PROJ_PROF,'')} darbas projekte '{r.get(PROJ_SITE,'')}' ({r.get(PROJ_CITY,'')}). "
        f"Pradžia { _format_date(r.get(PROJ_FROM,'')) }. "
        f"Jei domina, parašykite arba skambinkite {r.get(PROJ_CONTACT,'')} {r.get(PROJ_PHONE,'')}."
    ), axis=1)

    # stable id for selection
    allm = allm.reset_index(drop=False).rename(columns={"index":"match_id"})
    return allm
