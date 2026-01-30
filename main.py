#!/usr/bin/env python
# coding: utf-8

# In[1]:





# In[ ]:





# In[2]:


# combined_news_only_enriched.py
# News feed only, with OpenAI enrichment + keyword scoring rules.
# First run posts everything (store empty). Cron runs post only new items.
# Writes combined_feed.json
# Optional POST to FEED_POST_URL

import os
import sys
import json
import re
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Any, Set, Optional
from email.utils import parsedate_to_datetime

import requests
import feedparser
from requests.adapters import HTTPAdapter, Retry

# OpenAI
from openai import OpenAI

# ============================================================
# CONFIG
# ============================================================

API_URL = "https://api.news.ontario.ca/api/v1/releases"
LANG = "en"

FEED_URLS = [
    "https://news.ontario.ca/moh/en",
    "https://news.ontario.ca/mltc/en",
]

PATH_TO_ACRONYM = {
    "moh": "MOH",
    "mltc": "MLTC",
}

REGISTRY_FEEDS = [
    {
        "name": "Regulatory Registry News",
        "type": "Regulatory News",
        "url": "https://www.regulatoryregistry.gov.on.ca/api/api/rss?type=news&lang=en",
    },
    {
        "name": "Regulatory Registry Deadlines",
        "type": "Regulatory Deadline",
        "url": "https://www.regulatoryregistry.gov.on.ca/api/api/rss?type=deadlines&lang=en",
    },
]

EXTRA_RSS_FEEDS = [
    {
        "name": "Born Ontario News",
        "type": "Born Ontario",
        "url": "https://www.bornontario.ca/news/rss/",
    },
    {
        "name": "IPC PHIPA Decisions",
        "type": "PHIPA Decisions",
        "url": "https://decisia.lexum.com/ipc-cipvp/phipa/en/rss.do",
    },
    {
        "name": "Ontario Health News (FetchRSS)",
        "type": "Ontario Health News",
        "url": "https://fetchrss.com/feed/1vjLZQBVP4Fm1vjLZ13Iw36I.rss",
    },
]

OUT_JSON = "combined_feed.json"

FEED_POST_URL = os.getenv(
    "FEED_POST_URL",
    "https://tcgdugdhwtbyeygdqdob.supabase.co/functions/v1/feed",
).strip()

RAILWAY_API_KEY = os.getenv("RAILWAY_API_KEY", "").strip()

# Local default: skip POST
POST_ENABLED = os.getenv("POST_ENABLED", "0").strip() == "1"

# Log payload preview and skip POST
DRY_RUN = os.getenv("DRY_RUN", "0").strip() == "1"

# Skip RSS items without a published date
SKIP_RSS_IF_NO_PUBLISHED = True

# Reset local store file before run
RESET_STORE = os.getenv("RESET_STORE", "0").strip() == "1"

# Re-enrich every stored item (not only new)
REENRICH_ALL = os.getenv("REENRICH_ALL", "0").strip() == "1"

# ============================================================
# ENRICHMENT CONFIG
# ============================================================

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
ENRICH_ENABLED = os.getenv("ENRICH_ENABLED", "1").strip() == "1"
ENRICH_MODEL = os.getenv("ENRICH_MODEL", "gpt-4o-mini").strip()

# Keyword rules file
SCORE_RULES_PATH = os.getenv("SCORE_RULES_PATH", "scoring_rules.json").strip()
APPLY_SCORE_RULES = os.getenv("APPLY_SCORE_RULES", "1").strip() == "1"

TOPICS = [
    "Billing and Funding Changes",
    "Lab Services and Community Labs",
    "Test Ordering and Utilization",
    "Point of Care Testing",
    "Primary Care and Physician Services",
    "Long Term Care",
    "Pharmacy and Medications",
    "RHPA and Scope of Practice",
    "Consultations and Draft Regulations",
    "Privacy, PHIPA, and IPC Decisions",
    "Inspections, Compliance, and Quality",
]

DOMAIN_SIGNALS = [
    "LifeLabs",
    "Dynacare",
    "LSCCLA",
    "Laboratory and Specimen Collection Centre Licensing Act",
    "O. Reg. 552",
    "Regulation 552",
    "RHPA",
    "Pharmacy Act",
    "scope of practice",
    "laboratory",
    "community lab",
    "specimen",
    "requisition",
    "test ordering",
    "utilization",
    "fee schedule",
    "billing",
    "OHIP",
    "point of care",
    "POCT",
    "long term care",
    "LTC",
    "primary care",
    "family health team",
    "Ontario Health",
    "PHIPA",
    "IPC",
]

PLACE_MAP = {
    "lambeth": "London",
    "etobicoke": "Toronto",
    "scarborough": "Toronto",
    "north york": "Toronto",
    "clarkson": "Mississauga",
    "willowdale": "Toronto",
    "downsview": "Toronto",
    "rexdale": "Toronto",
    "leaside": "Toronto",
}

MONEY_RE = re.compile(r"(?i)\$[\s]*([\d]{1,3}(?:,[\d]{3})*(?:\.\d{1,2})?|\d+(?:\.\d{1,2})?)")

ENRICH_SYSTEM = (
    "You label Ontario health policy updates for lab services and related business impact.\n"
    "Output strict JSON only, with keys exactly as requested.\n"
    "Write short, direct sentences.\n"
    "Score 0 to 100.\n"
    "Focus on lab services, test ordering, scope, billing, long term care, primary care, point of care.\n"
    "Avoid topic 'Other'. Use only allowed topics.\n"
)

def utc_iso_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def clamp_0_100(x: int) -> int:
    if x < 0:
        return 0
    if x > 100:
        return 100
    return x

def load_score_rules() -> Dict[str, Any]:
    if os.path.isfile(SCORE_RULES_PATH) is False:
        return {"boosts": [], "penalties": []}
    with open(SCORE_RULES_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) is False:
        return {"boosts": [], "penalties": []}
    data.setdefault("boosts", [])
    data.setdefault("penalties", [])
    return data

def apply_keyword_score_rules(text: str, base_score: int, rules: Dict[str, Any]) -> int:
    if APPLY_SCORE_RULES is False:
        return clamp_0_100(int(base_score))

    t = (text or "").lower()
    score = int(base_score)

    for r in rules.get("boosts", []):
        kw = str(r.get("kw") or "").lower().strip()
        pts = int(r.get("pts") or 0)
        if kw and (kw in t):
            score += pts

    for r in rules.get("penalties", []):
        kw = str(r.get("kw") or "").lower().strip()
        pts = int(r.get("pts") or 0)
        if kw and (kw in t):
            score += pts

    return clamp_0_100(score)

def extract_money_values(text: str) -> List[str]:
    vals: List[str] = []
    for m in MONEY_RE.findall(text or ""):
        v = "$" + str(m).strip()
        if v not in vals:
            vals.append(v)
    return vals[:10]

def normalize_places(text: str) -> List[str]:
    t = (text or "").lower()
    cities: List[str] = []
    for raw, city in PLACE_MAP.items():
        if raw in t and city not in cities:
            cities.append(city)
    return cities[:8]

_openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

def enrich_row_with_openai(row: Dict[str, Any], rules: Dict[str, Any]) -> Dict[str, Any]:
    title = str(row.get("title") or "").strip()
    excerpt = str(row.get("excerpt") or "").strip()
    source = str(row.get("ministry_name") or row.get("source") or "").strip()
    url = str(row.get("url") or "").strip()

    blob = f"{title}\n\n{excerpt}\n\nSource: {source}\nURL: {url}"

    money_backup = extract_money_values(blob)
    cities_backup = normalize_places(blob)

    row["ai_model"] = ENRICH_MODEL
    row["ai_enriched_at"] = utc_iso_z()

    if ENRICH_ENABLED is False or _openai_client is None:
        row["ai_summary_1s"] = title[:220]
        row["ai_why_lifelabs_cares"] = ""
        row["ai_score_0_100"] = apply_keyword_score_rules(blob, 0, rules)
        row["ai_score_reason"] = "Enrichment disabled"
        row["ai_topics"] = []
        row["ai_dollar_values"] = money_backup
        row["ai_city_labels"] = cities_backup
        row["ai_signals_found"] = []
        row["ai_entities"] = []
        return row

    payload = {
        "allowed_topics": TOPICS,
        "domain_signals": DOMAIN_SIGNALS,
        "place_map": [{"raw": "Lambeth", "city": "London"}, {"raw": "Etobicoke", "city": "Toronto"}],
        "output_schema": {
            "ai_summary_1s": "string, 1 sentence",
            "ai_why_lifelabs_cares": "string, 1 sentence, business impact",
            "ai_score_0_100": "int 0-100",
            "ai_score_reason": "string, short",
            "ai_topics": "array 1-3 from allowed_topics",
            "ai_dollar_values": "array of strings",
            "ai_city_labels": "array of Ontario city labels",
            "ai_signals_found": "array of strings from domain_signals",
            "ai_entities": "array of strings",
        },
        "item": {
            "title": title,
            "excerpt": excerpt,
            "source": source,
            "url": url,
        },
    }

    try:
        resp = _openai_client.chat.completions.create(
            model=ENRICH_MODEL,
            messages=[
                {"role": "system", "content": ENRICH_SYSTEM},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            temperature=0.1,
        )

        raw = resp.choices[0].message.content or ""
        data = json.loads(raw)

        row["ai_summary_1s"] = str(data.get("ai_summary_1s") or "").strip()
        row["ai_why_lifelabs_cares"] = str(data.get("ai_why_lifelabs_cares") or "").strip()

        base_score = int(data.get("ai_score_0_100") or 0)
        row["ai_score_0_100"] = apply_keyword_score_rules(blob, base_score, rules)

        row["ai_score_reason"] = str(data.get("ai_score_reason") or "").strip()

        topics = data.get("ai_topics") or []
        topics = [t for t in topics if t in TOPICS]
        row["ai_topics"] = topics[:3]

        dv = data.get("ai_dollar_values") or []
        merged_dv: List[str] = []
        for v in dv + money_backup:
            v2 = str(v).strip()
            if v2 and v2 not in merged_dv:
                merged_dv.append(v2)
        row["ai_dollar_values"] = merged_dv[:10]

        cities = data.get("ai_city_labels") or []
        merged_cities: List[str] = []
        for c in cities + cities_backup:
            c2 = str(c).strip()
            if c2 and c2 not in merged_cities:
                merged_cities.append(c2)
        row["ai_city_labels"] = merged_cities[:8]

        sigs = data.get("ai_signals_found") or []
        row["ai_signals_found"] = [str(s).strip() for s in sigs if str(s).strip()][:12]

        ents = data.get("ai_entities") or []
        row["ai_entities"] = [str(e).strip() for e in ents if str(e).strip()][:12]

    except Exception as e:
        row["ai_error"] = str(e)
        row["ai_summary_1s"] = title[:220]
        row["ai_why_lifelabs_cares"] = ""
        row["ai_score_0_100"] = apply_keyword_score_rules(blob, 0, rules)
        row["ai_score_reason"] = "AI error"
        row["ai_topics"] = []
        row["ai_dollar_values"] = money_backup
        row["ai_city_labels"] = cities_backup
        row["ai_signals_found"] = []
        row["ai_entities"] = []

    return row

# ============================================================
# HELPERS
# ============================================================

TAG_RE = re.compile(r"<[^>]+>")

def strip_html(s: str) -> str:
    s = s or ""
    s = s.replace("&mdash;", " - ")
    s = s.replace("&ndash;", " - ")
    s = s.replace("&nbsp;", " ")
    s = TAG_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def make_excerpt(item: Dict[str, Any], max_len: int = 420) -> str:
    raw = item.get("content_lead") or item.get("content_subtitle") or ""
    text = strip_html(raw)
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"

def parse_ministry_from_url(url: str) -> str:
    for p in url.split("/"):
        key = p.lower()
        if key in PATH_TO_ACRONYM:
            return PATH_TO_ACRONYM[key]
    raise ValueError(f"Unknown ministry in url: {url}")

def build_public_url(item: Dict[str, Any]) -> str:
    rid = item.get("release_id_translated") or item.get("id")
    slug = item.get("slug") or ""
    return f"https://news.ontario.ca/{LANG}/release/{rid}/{slug}"

def to_row(item: Dict[str, Any]) -> Dict[str, Any]:
    rid = item.get("release_id_translated") or item.get("id")
    collected_at = datetime.now(timezone.utc).isoformat()
    return {
        "id": rid,
        "date": item.get("release_date_time") or "",
        "date_display": item.get("release_date_time_formatted") or "",
        "collected_at": collected_at,
        "type": item.get("release_type_name") or item.get("release_type_label") or "",
        "ministry_acronym": (item.get("ministry_acronym") or "").strip(),
        "ministry_name": item.get("ministry_name") or "",
        "title": item.get("clean_title") or item.get("content_title") or "",
        "excerpt": make_excerpt(item, max_len=420),
        "url": build_public_url(item),
        "source": "ontario_newsroom_api",
    }

def load_existing() -> Dict[str, Any]:
    if not os.path.exists(OUT_JSON):
        return {"items": []}
    with open(OUT_JSON, "r", encoding="utf-8") as f:
        return json.load(f)

def rss_id(entry: Dict[str, Any]) -> str:
    base = entry.get("id") or entry.get("guid") or entry.get("link") or entry.get("title", "")
    return hashlib.sha256(str(base).encode("utf-8")).hexdigest()

def _iso_z_from_dt(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")

def _try_parse_any_date_string(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""

    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return _iso_z_from_dt(dt)
        dt = datetime.fromisoformat(s)
        return _iso_z_from_dt(dt)
    except Exception:
        pass

    try:
        dt = parsedate_to_datetime(s)
        return _iso_z_from_dt(dt)
    except Exception:
        return ""

def _extract_published_from_entry(e: Dict[str, Any]) -> Dict[str, str]:
    if getattr(e, "published_parsed", None):
        dt = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
        return {"iso": _iso_z_from_dt(dt), "display": e.get("published", "") or ""}

    if getattr(e, "updated_parsed", None):
        dt = datetime(*e.updated_parsed[:6], tzinfo=timezone.utc)
        return {"iso": _iso_z_from_dt(dt), "display": e.get("updated", "") or ""}

    raw = e.get("published") or e.get("updated") or ""
    parsed = _try_parse_any_date_string(raw)
    if parsed:
        return {"iso": parsed, "display": raw}

    for k in ["dc_date", "dc:date", "date", "pubDate"]:
        raw2 = e.get(k, "") if isinstance(e, dict) else ""
        parsed2 = _try_parse_any_date_string(str(raw2))
        if parsed2:
            return {"iso": parsed2, "display": str(raw2)}

    return {"iso": "", "display": ""}

def parse_rss_feed(feed_cfg: Dict[str, str]) -> List[Dict[str, Any]]:
    feed = feedparser.parse(feed_cfg["url"])
    rows: List[Dict[str, Any]] = []
    collected_at = datetime.now(timezone.utc).isoformat()

    for e in getattr(feed, "entries", []):
        rid = rss_id(e)
        pub = _extract_published_from_entry(e)
        dt_iso = pub["iso"]
        dt_display = pub["display"]

        if SKIP_RSS_IF_NO_PUBLISHED and not dt_iso:
            continue

        summary = strip_html(e.get("summary", "") or e.get("description", "") or "")
        if summary and len(summary) > 420:
            summary = summary[:419].rstrip() + "…"

        rows.append({
            "id": rid,
            "date": dt_iso,
            "date_display": dt_display,
            "collected_at": collected_at,
            "type": feed_cfg["type"],
            "ministry_acronym": feed_cfg.get("acronym", "RSS"),
            "ministry_name": feed_cfg["name"],
            "title": strip_html(e.get("title", "")),
            "excerpt": summary,
            "url": e.get("link", ""),
            "source": feed_cfg.get("source", "rss"),
        })

    return rows

def row_to_feed_item(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rid = str(row.get("id") or "").strip()
    title = str(row.get("title") or "").strip()
    link = str(row.get("url") or "").strip()
    source_name = str(row.get("ministry_name") or row.get("ministry_acronym") or row.get("source") or "Source").strip()

    published = str(row.get("date") or "").strip()
    collected = str(row.get("collected_at") or "").strip()

    if not published:
        return None

    pub_z = _try_parse_any_date_string(published)
    if not pub_z:
        return None

    ai_summary = str(row.get("ai_summary_1s") or row.get("excerpt") or "").strip()
    ai_why = str(row.get("ai_why_lifelabs_cares") or "").strip()
    ai_score = int(row.get("ai_score_0_100") or 0)
    dollars = row.get("ai_dollar_values") or []
    cities = row.get("ai_city_labels") or []
    topics = row.get("ai_topics") or []

    lines: List[str] = []
    if ai_summary:
        lines.append(ai_summary)
    if ai_why:
        lines.append(ai_why)
    lines.append(f"Score: {ai_score}/100")
    if dollars:
        lines.append("Dollars: " + ", ".join([str(x) for x in dollars[:6]]))
    if cities:
        lines.append("Cities: " + ", ".join([str(x) for x in cities[:6]]))
    if topics:
        lines.append("Topics: " + ", ".join([str(x) for x in topics[:6]]))

    content = "\n".join(lines)[:2000]

    return {
        "id": rid,
        "title": title,
        "content": content,
        "link": link,
        "pubDate": pub_z,
        "publishedDate": pub_z,
        "collectedAt": _try_parse_any_date_string(collected) or "",
        "source": source_name,
        "extra": {
            "ai_summary_1s": row.get("ai_summary_1s"),
            "ai_why_lifelabs_cares": row.get("ai_why_lifelabs_cares"),
            "ai_score_0_100": row.get("ai_score_0_100"),
            "ai_score_reason": row.get("ai_score_reason"),
            "ai_topics": row.get("ai_topics"),
            "ai_dollar_values": row.get("ai_dollar_values"),
            "ai_city_labels": row.get("ai_city_labels"),
            "ai_signals_found": row.get("ai_signals_found"),
            "ai_entities": row.get("ai_entities"),
            "ai_enriched_at": row.get("ai_enriched_at"),
            "ai_model": row.get("ai_model"),
        },
    }

def build_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

session = build_session()

def fetch_releases_for_ministry(acronym: str, limit: int = 300) -> List[Dict[str, Any]]:
    r = session.get(
        API_URL,
        params={"language": LANG, "limit": limit, "sort": "desc"},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json().get("data", [])
    return [x for x in data if (x.get("ministry_acronym") or "").strip() == acronym]

def post_to_feed_function(items: List[Dict[str, Any]]) -> Optional[requests.Response]:
    if not items:
        return None

    if DRY_RUN or (POST_ENABLED is False):
        print("POST skipped.")
        print("FEED_POST_URL:", FEED_POST_URL)
        print("Items:", len(items))
        print("Payload preview:", json.dumps({"items": items}, ensure_ascii=False)[:900])
        return None

    if RAILWAY_API_KEY == "":
        print("RAILWAY_API_KEY absent. POST skipped.")
        return None

    headers = {"x-api-key": RAILWAY_API_KEY, "Content-Type": "application/json"}
    payload = {"items": items}

    r = session.post(FEED_POST_URL, headers=headers, json=payload, timeout=30)
    print("Feed POST status:", r.status_code)
    print("Feed POST body preview:", (r.text or "")[:900])
    r.raise_for_status()
    return r

# ============================================================
# MAIN
# ============================================================

def main():
    if RESET_STORE and os.path.exists(OUT_JSON):
        os.remove(OUT_JSON)

    rules = load_score_rules()

    store = load_existing()
    existing_items = store.get("items", [])
    seen: Set[str] = {str(x.get("id")) for x in existing_items if x.get("id")}

    new_rows: List[Dict[str, Any]] = []

    # A1) Ontario Newsroom API
    acronyms = [parse_ministry_from_url(u) for u in FEED_URLS]
    for acr in acronyms:
        items = fetch_releases_for_ministry(acr)
        for it in items:
            row = to_row(it)
            if not row.get("id"):
                continue
            rid = str(row["id"])
            if rid in seen:
                continue
            seen.add(rid)
            new_rows.append(row)

    # A2) Regulatory Registry RSS
    for cfg in REGISTRY_FEEDS:
        cfg2 = dict(cfg)
        cfg2["acronym"] = "REG"
        cfg2["source"] = "regulatory_registry_rss"
        rss_items = parse_rss_feed(cfg2)
        for row in rss_items:
            rid = str(row.get("id") or "")
            if rid == "":
                continue
            if rid in seen:
                continue
            seen.add(rid)
            new_rows.append(row)

    # A3) Extra RSS feeds
    for cfg in EXTRA_RSS_FEEDS:
        cfg2 = dict(cfg)
        cfg2["acronym"] = "RSS"
        cfg2["source"] = "extra_rss"
        rss_items = parse_rss_feed(cfg2)
        for row in rss_items:
            rid = str(row.get("id") or "")
            if rid == "":
                continue
            if rid in seen:
                continue
            seen.add(rid)
            new_rows.append(row)

    # Enrich only new rows (default)
    for i in range(len(new_rows)):
        new_rows[i] = enrich_row_with_openai(new_rows[i], rules)

    # Optional re-enrich all stored items
    if REENRICH_ALL and existing_items:
        for i in range(len(existing_items)):
            existing_items[i] = enrich_row_with_openai(existing_items[i], rules)

    # Store
    if new_rows:
        existing_items.extend(new_rows)
        existing_items.sort(key=lambda x: x.get("date") or "", reverse=True)

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(
            {
                "source_urls": FEED_URLS,
                "registry_feeds": [x["url"] for x in REGISTRY_FEEDS],
                "extra_rss_feeds": [x["url"] for x in EXTRA_RSS_FEEDS],
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "count": len(existing_items),
                "items": existing_items,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    # POST only new items
    feed_items_raw = [row_to_feed_item(r) for r in new_rows]
    feed_items = [x for x in feed_items_raw if x is not None]

    print("---- FEED ----")
    print("New rows:", len(new_rows))
    print("New feed items:", len(feed_items))
    print("Output file path:", os.path.abspath(OUT_JSON))
    print("File exists:", os.path.exists(OUT_JSON))
    if os.path.exists(OUT_JSON):
        print("File size (bytes):", os.path.getsize(OUT_JSON))

    post_to_feed_function(feed_items)

    print("---- DONE ----")
    sys.stdout.flush()

if __name__ == "__main__":
    main()


# In[ ]:




