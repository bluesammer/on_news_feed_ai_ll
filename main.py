#!/usr/bin/env python
# coding: utf-8

# In[1]:





# In[1]:





# In[3]:





# In[4]:


# main.py
# News feed + OpenAI enrichment + keyword scoring rules
# Stores combined_feed_<tag>.json in Supabase Storage so cron runs resume correctly

import os
import sys
import json
import re
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from email.utils import parsedate_to_datetime

import requests
import feedparser
from requests.adapters import HTTPAdapter, Retry

from openai import OpenAI

# ============================================================
# OPENAI KEY SOURCE
# ============================================================
# Option A: Hard code here for local runs. Leave "" on Railway.
# Option B: Set OPENAI_API_KEY in Railway Variables.
HARDCODED_OPENAI_KEY = ""  # paste sk-proj-... here, or leave blank

_env_key = os.getenv("OPENAI_API_KEY", "").strip()
_hard_key = (HARDCODED_OPENAI_KEY or "").strip()

if _env_key == "" and _hard_key != "":
    os.environ["OPENAI_API_KEY"] = _hard_key

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

FEED_TAG = os.getenv("FEED_TAG", "ai").strip()
OUT_JSON_LOCAL = os.getenv("OUT_JSON_LOCAL", f"combined_feed_{FEED_TAG}.json").strip()

FEED_POST_URL = os.getenv(
    "FEED_POST_URL",
    "https://tcgdugdhwtbyeygdqdob.supabase.co/functions/v1/feed",
).strip()

def _default_ai_feed_url(base: str) -> str:
    b = (base or "").strip()
    if b.endswith("/feed"):
        return b[:-5] + "/feed_ai"
    return b.rstrip("/") + "_ai"

FEED_POST_URL_AI = os.getenv("FEED_POST_URL_AI", _default_ai_feed_url(FEED_POST_URL)).strip()

POST_TO_MAIN_FEED = os.getenv("POST_TO_MAIN_FEED", "0").strip() == "1"
POST_TO_AI_FEED = os.getenv("POST_TO_AI_FEED", "1").strip() == "1"
POST_ID_SUFFIX = os.getenv("POST_ID_SUFFIX", "1").strip() == "1"

POST_ENABLED = os.getenv("POST_ENABLED", "0").strip() == "1"
DRY_RUN = os.getenv("DRY_RUN", "0").strip() == "1"

SKIP_RSS_IF_NO_PUBLISHED = True

RESET_STORE = os.getenv("RESET_STORE", "0").strip() == "1"
REENRICH_ALL = os.getenv("REENRICH_ALL", "0").strip() == "1"

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50").strip())

# ============================================================
# SUPABASE STORAGE STORE
# ============================================================

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "").strip()

SUPABASE_OBJECT_PATH = os.getenv(
    "SUPABASE_OBJECT_PATH",
    f"feeds/combined_feed_{FEED_TAG}.json",
).strip()

STORE_REMOTE_ENABLED = bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY and SUPABASE_BUCKET and SUPABASE_OBJECT_PATH)

# ============================================================
# ENRICHMENT CONFIG
# ============================================================

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
ENRICH_ENABLED = os.getenv("ENRICH_ENABLED", "1").strip() == "1"
ENRICH_MODEL = os.getenv("ENRICH_MODEL", "gpt-4o-mini").strip()

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

# ============================================================
# LOGGING
# ============================================================

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    print(f"[{ts}] {msg}")
    sys.stdout.flush()

def utc_iso_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def clamp_0_100(x: int) -> int:
    if x < 0:
        return 0
    if x > 100:
        return 100
    return x

# ============================================================
# HTTP SESSION
# ============================================================

def build_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PUT"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

session = build_session()

# ============================================================
# OPENAI CLIENT
# ============================================================

def get_openai_client() -> Optional[OpenAI]:
    if ENRICH_ENABLED is False:
        return None
    if OPENAI_API_KEY == "":
        raise RuntimeError("Missing OPENAI_API_KEY. Set Railway variable, or set HARDCODED_OPENAI_KEY in main.py.")
    return OpenAI(api_key=OPENAI_API_KEY)

# ============================================================
# SUPABASE STORAGE HELPERS
# ============================================================

def supabase_object_url() -> str:
    return f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{SUPABASE_OBJECT_PATH}"

def supabase_headers_json() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Content-Type": "application/json",
        "x-upsert": "true",
    }

def load_store_remote() -> Dict[str, Any]:
    if STORE_REMOTE_ENABLED is False:
        return {"items": []}

    url = supabase_object_url()
    h = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
    }

    try:
        r = session.get(url, headers=h, timeout=30)
        if r.status_code == 404:
            log("Remote store missing. Starting fresh.")
            return {"items": []}
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) is False:
            return {"items": []}
        data.setdefault("items", [])
        return data
    except Exception as e:
        log(f"Remote store load error: {e}. Starting fresh.")
        return {"items": []}

def save_store_remote(store: Dict[str, Any]) -> None:
    if STORE_REMOTE_ENABLED is False:
        return
    url = supabase_object_url()
    h = supabase_headers_json()
    payload = json.dumps(store, ensure_ascii=False).encode("utf-8")
    r = session.put(url, headers=h, data=payload, timeout=30)
    r.raise_for_status()

def save_store_local(store: Dict[str, Any]) -> str:
    with open(OUT_JSON_LOCAL, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, ensure_ascii=False)
    return os.path.abspath(OUT_JSON_LOCAL)

# ============================================================
# SCORING RULES
# ============================================================

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
        if raw in t and (city in cities) is False:
            cities.append(city)
    return cities[:10]

# ============================================================
# FEED HELPERS
# ============================================================

def safe_parse_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def stable_id(*parts: str) -> str:
    raw = "|".join([p.strip() for p in parts if p is not None])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

def fetch_ontario_news_pages() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    try:
        r = session.get(API_URL, timeout=30)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            items = data["items"]
        elif isinstance(data, list):
            items = data
        else:
            items = []
        for it in items:
            if isinstance(it, dict) is False:
                continue
            title = str(it.get("title") or "").strip()
            url = str(it.get("url") or it.get("link") or "").strip()
            published = str(it.get("published") or it.get("pubDate") or it.get("date") or "").strip()
            if title == "" and url == "":
                continue
            out.append(
                {
                    "source": "Ontario News API",
                    "type": "Ontario Release",
                    "title": title,
                    "link": url,
                    "published_raw": published,
                    "summary": str(it.get("summary") or it.get("description") or "").strip(),
                }
            )
    except Exception as e:
        log(f"Ontario API fetch error: {e}")
    return out

def fetch_rss(url: str, source_name: str, source_type: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    try:
        parsed = feedparser.parse(url)
        for e in (parsed.entries or []):
            title = str(getattr(e, "title", "") or "").strip()
            link = str(getattr(e, "link", "") or "").strip()
            summary = str(getattr(e, "summary", "") or getattr(e, "description", "") or "").strip()
            published = str(getattr(e, "published", "") or getattr(e, "updated", "") or "").strip()

            if SKIP_RSS_IF_NO_PUBLISHED and published == "":
                continue

            out.append(
                {
                    "source": source_name,
                    "type": source_type,
                    "title": title,
                    "link": link,
                    "published_raw": published,
                    "summary": summary,
                }
            )
    except Exception as e:
        log(f"RSS fetch error for {source_name}: {e}")
    return out

def collect_items() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    items.extend(fetch_ontario_news_pages())

    for u in FEED_URLS:
        # For these, source name comes from URL path token
        token = u.rstrip("/").split("/")[-2] if "/" in u.rstrip("/") else u
        src = PATH_TO_ACRONYM.get(token, token.upper())
        items.extend(fetch_rss(u, src, "Ontario RSS"))

    for f in REGISTRY_FEEDS:
        items.extend(fetch_rss(f["url"], f["name"], f["type"]))

    for f in EXTRA_RSS_FEEDS:
        items.extend(fetch_rss(f["url"], f["name"], f["type"]))

    return items

# ============================================================
# ENRICHMENT
# ============================================================

def enrich_one(client: OpenAI, item: Dict[str, Any]) -> Dict[str, Any]:
    title = str(item.get("title") or "")
    summary = str(item.get("summary") or "")
    link = str(item.get("link") or "")
    text = (title + "\n\n" + summary + "\n\n" + link).strip()

    money_vals = extract_money_values(text)
    places = normalize_places(text)

    prompt = (
        "Return JSON with keys: topic, score, why, keywords.\n"
        "topic must be one of these:\n"
        + "\n".join(TOPICS)
        + "\n"
        "score must be 0 to 100.\n"
        "keywords must be a list of 3 short strings.\n"
        "why must be 1 to 2 short sentences.\n"
        "\n"
        "Text:\n"
        + text[:6000]
    )

    try:
        resp = client.chat.completions.create(
            model=ENRICH_MODEL,
            messages=[
                {"role": "system", "content": ENRICH_SYSTEM},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        raw = (resp.choices[0].message.content or "").strip()
        data = json.loads(raw)

        topic = str(data.get("topic") or "").strip()
        score = int(data.get("score") or 0)
        why = str(data.get("why") or "").strip()
        keywords = data.get("keywords") or []
        if isinstance(keywords, list) is False:
            keywords = []

        item["ai_topic"] = topic
        item["ai_score"] = clamp_0_100(score)
        item["ai_why"] = why
        item["ai_keywords"] = [str(x).strip() for x in keywords][:3]
        item["money_values"] = money_vals
        item["places"] = places
        item["enriched_at"] = utc_iso_z()
        return item
    except Exception as e:
        item["ai_topic"] = ""
        item["ai_score"] = 0
        item["ai_why"] = f"enrich_error: {e}"
        item["ai_keywords"] = []
        item["money_values"] = money_vals
        item["places"] = places
        item["enriched_at"] = utc_iso_z()
        return item

# ============================================================
# POSTING
# ============================================================

def post_items(url: str, items: List[Dict[str, Any]]) -> None:
    if POST_ENABLED is False:
        log("POST disabled.")
        return
    if DRY_RUN:
        log(f"DRY_RUN on. Skip post to {url}. Items={len(items)}")
        return

    payload = {"items": items}
    r = session.post(url, json=payload, timeout=60)
    r.raise_for_status()
    log(f"Posted {len(items)} items to {url}")

# ============================================================
# STORE + DEDUPE
# ============================================================

def index_store(store: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for it in (store.get("items") or []):
        if isinstance(it, dict) is False:
            continue
        _id = str(it.get("id") or "").strip()
        if _id:
            idx[_id] = it
    return idx

def ensure_store_shape(store: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(store, dict) is False:
        store = {}
    store.setdefault("items", [])
    store.setdefault("meta", {})
    store["meta"]["updated_at"] = utc_iso_z()
    store["meta"]["feed_tag"] = FEED_TAG
    return store

def build_item_id(source: str, link: str, title: str) -> str:
    return stable_id(source or "", link or "", title or "")

def add_post_suffix(item_id: str) -> str:
    if POST_ID_SUFFIX is False:
        return item_id
    return f"{item_id}_{FEED_TAG}"

# ============================================================
# MAIN
# ============================================================

def main() -> None:
    log("Start run.")
    log("OpenAI key present: " + ("yes" if OPENAI_API_KEY else "no"))

    if RESET_STORE:
        store = {"items": [], "meta": {"reset_at": utc_iso_z()}}
    else:
        store = load_store_remote() if STORE_REMOTE_ENABLED else {"items": []}

    store = ensure_store_shape(store)
    existing = index_store(store)

    rules = load_score_rules()

    raw_items = collect_items()
    log(f"Collected {len(raw_items)} raw items.")

    new_or_update: List[Dict[str, Any]] = []

    for ri in raw_items:
        source = str(ri.get("source") or "").strip()
        title = str(ri.get("title") or "").strip()
        link = str(ri.get("link") or "").strip()
        published_raw = str(ri.get("published_raw") or "").strip()
        published_dt = safe_parse_dt(published_raw)
        published_iso = published_dt.isoformat().replace("+00:00", "Z") if published_dt else ""

        base_id = build_item_id(source, link, title)

        prev = existing.get(base_id)
        if prev and REENRICH_ALL is False:
            continue

        item = {
            "id": base_id,
            "post_id": add_post_suffix(base_id),
            "feed_tag": FEED_TAG,
            "source": source,
            "type": str(ri.get("type") or "").strip(),
            "title": title,
            "link": link,
            "summary": str(ri.get("summary") or "").strip(),
            "published": published_iso,
            "published_raw": published_raw,
            "ingested_at": utc_iso_z(),
        }

        text_for_score = (item["title"] + "\n" + item["summary"]).strip()
        base_score = 10
        if any(sig.lower() in text_for_score.lower() for sig in DOMAIN_SIGNALS):
            base_score = 35
        item["keyword_score"] = apply_keyword_score_rules(text_for_score, base_score, rules)

        new_or_update.append(item)

    log(f"New or re-enrich items: {len(new_or_update)}")

    client = get_openai_client()
    enriched: List[Dict[str, Any]] = []

    if client is None:
        enriched = new_or_update
    else:
        for i, it in enumerate(new_or_update):
            if (i + 1) % 10 == 0:
                log(f"Enrich progress {i+1}/{len(new_or_update)}")
            enriched.append(enrich_one(client, it))

    # Update store
    by_id = index_store(store)
    for it in enriched:
        by_id[it["id"]] = it

    # Keep newest first by published, then ingested
    def _sort_key(x: Dict[str, Any]) -> str:
        p = str(x.get("published") or "")
        i = str(x.get("ingested_at") or "")
        return (p or "") + "|" + (i or "")

    all_items = list(by_id.values())
    all_items.sort(key=_sort_key, reverse=True)

    store["items"] = all_items
    store["meta"]["count"] = len(all_items)

    local_path = save_store_local(store)
    log(f"Saved local store: {local_path}")

    if STORE_REMOTE_ENABLED:
        save_store_remote(store)
        log("Saved remote store.")

    # Post only the enriched batch, not the full store
    if POST_TO_MAIN_FEED:
        post_items(FEED_POST_URL, enriched)
    if POST_TO_AI_FEED:
        post_items(FEED_POST_URL_AI, enriched)

    log("Done.")

if __name__ == "__main__":
    main()


# In[ ]:




