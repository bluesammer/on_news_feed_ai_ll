#!/usr/bin/env python
# coding: utf-8

# In[1]:





# In[1]:





# In[3]:





# In[4]:


# main.py
# RSS + Ontario News API -> store (Supabase Storage) -> enrich N per run -> post full store to Supabase Edge Function
# Built for Railway logs. DEBUG=1 prints step-by-step.

import os
import sys
import json
import re
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from email.utils import parsedate_to_datetime

import requests
import feedparser
from requests.adapters import HTTPAdapter, Retry

from openai import OpenAI


# =========================
# CONFIG
# =========================

API_URL = "https://api.news.ontario.ca/api/v1/releases"

FEED_URLS = [
    "https://news.ontario.ca/moh/en",
    "https://news.ontario.ca/mltc/en",
]

PATH_TO_ACRONYM = {"moh": "MOH", "mltc": "MLTC"}

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
    {"name": "Born Ontario News", "type": "Born Ontario", "url": "https://www.bornontario.ca/news/rss/"},
    {"name": "IPC PHIPA Decisions", "type": "PHIPA Decisions", "url": "https://decisia.lexum.com/ipc-cipvp/phipa/en/rss.do"},
    {"name": "Ontario Health News (FetchRSS)", "type": "Ontario Health News", "url": "https://fetchrss.com/feed/1vjLZQBVP4Fm1vjLZ13Iw36I.rss"},
]

FEED_TAG = (os.getenv("FEED_TAG", "ai") or "ai").strip()

MAX_STORE_ITEMS = int((os.getenv("MAX_STORE_ITEMS", "160") or "160").strip())
MAX_ENRICH_ITEMS = int((os.getenv("MAX_ENRICH_ITEMS", "5") or "5").strip())
MAX_AGE_DAYS = int((os.getenv("MAX_AGE_DAYS", "365") or "365").strip())

ENRICH_ENABLED = (os.getenv("ENRICH_ENABLED", "1") or "1").strip() == "1"
ENRICH_MODEL = (os.getenv("ENRICH_MODEL", "gpt-4o-mini") or "gpt-4o-mini").strip()

# Edge Function post (Lovable reads this)
FEED_POST_URL = (os.getenv(
    "FEED_POST_URL",
    "https://tcgdugdhwtbyeygdqdob.supabase.co/functions/v1/feed",
) or "").strip()
FEED_FUNCTION_KEY = (os.getenv("FEED_FUNCTION_KEY", "") or "").strip()
POST_ENABLED = (os.getenv("POST_ENABLED", "0") or "0").strip() == "1"
DRY_RUN = (os.getenv("DRY_RUN", "0") or "0").strip() == "1"

# Supabase Storage persistence (recommended)
SUPABASE_URL = (os.getenv("SUPABASE_URL", "") or "").strip().rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY", "") or "").strip()
SUPABASE_BUCKET = (os.getenv("SUPABASE_BUCKET", "") or "").strip()
SUPABASE_OBJECT_PATH = (os.getenv("SUPABASE_OBJECT_PATH", f"feeds/combined_feed_{FEED_TAG}.json") or "").strip()

STORE_REMOTE_ENABLED = (
    SUPABASE_URL != ""
    and SUPABASE_SERVICE_ROLE_KEY != ""
    and SUPABASE_BUCKET != ""
    and SUPABASE_OBJECT_PATH != ""
)

RESET_STORE = (os.getenv("RESET_STORE", "0") or "0").strip() == "1"
DEBUG = (os.getenv("DEBUG", "0") or "0").strip() == "1"
DEBUG_SAMPLE_PER_SOURCE = int((os.getenv("DEBUG_SAMPLE_PER_SOURCE", "0") or "0").strip())

SKIP_RSS_IF_NO_PUBLISHED = True

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

ENRICH_SYSTEM = (
    "Return ONLY valid JSON. No markdown. No code fences. No extra text.\n"
    "Keys: summary, topic, score, keywords.\n"
    "summary: 1 to 2 short sentences.\n"
    "topic must match an allowed topic.\n"
    "score must be an integer 0 to 100.\n"
    "keywords must be an array of 3 short strings.\n"
)

MONEY_RE = re.compile(r"(?i)\$[\s]*([\d]{1,3}(?:,[\d]{3})*(?:\.\d{1,2})?|\d+(?:\.\d{1,2})?)")

# Simple Ontario city list. Add more over time.
ONTARIO_CITIES = [
    "Toronto","Ottawa","Hamilton","London","Kitchener","Waterloo","Guelph","Windsor",
    "Kingston","Sudbury","Thunder Bay","Barrie","Peterborough","Sarnia","Niagara Falls",
    "St. Catharines","Mississauga","Brampton","Markham","Vaughan","Richmond Hill",
    "Oakville","Burlington","Whitby","Oshawa","Pickering","Ajax","Newmarket",
]


# =========================
# LOGGING
# =========================

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    print(f"[{ts}] {msg}")
    sys.stdout.flush()

def dbg(msg: str) -> None:
    if DEBUG:
        log(msg)

def utc_iso_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# =========================
# HTTP
# =========================

def build_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PUT"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

session = build_session()


# =========================
# HELPERS
# =========================

def safe_parse_dt(s: str) -> Optional[datetime]:
    if (s or "").strip() == "":
        return None
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def stable_id(*parts: str) -> str:
    raw = "|".join([str(p).strip() for p in parts if p is not None])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

def is_too_old(published_dt: Optional[datetime]) -> bool:
    if published_dt is None:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
    return published_dt < cutoff

def extract_money_values(text: str) -> List[str]:
    vals: List[str] = []
    for m in MONEY_RE.findall(text or ""):
        v = "$" + str(m).strip()
        if v not in vals:
            vals.append(v)
    return vals[:10]

def extract_cities(text: str) -> List[str]:
    t = (text or "").lower()
    out: List[str] = []
    for c in ONTARIO_CITIES:
        if c.lower() in t and c not in out:
            out.append(c)
    # Quick fallback: patterns like "City of X"
    m = re.findall(r"(?i)\bcity of ([a-z][a-z\s\.\-]{2,40})\b", text or "")
    for x in m:
        x2 = " ".join(x.strip().split())
        x2 = x2.title()
        if x2 not in out and len(out) < 10:
            out.append(x2)
    return out[:10]


# =========================
# FETCH
# =========================

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
            summary = str(it.get("summary") or it.get("description") or "").strip()
            if title == "" and url == "":
                continue
            out.append(
                {
                    "source": "Ontario News API",
                    "type": "Ontario Release",
                    "title": title,
                    "link": url,
                    "published_raw": published,
                    "summary": summary,
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

            if title == "" and link == "":
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
    dbg("STEP fetch.start")
    items: List[Dict[str, Any]] = []
    a = fetch_ontario_news_pages()
    dbg(f"STEP fetch.ontario_api items={len(a)}")
    items.extend(a)

    for u in FEED_URLS:
        token = u.rstrip("/").split("/")[-2] if "/" in u.rstrip("/") else u
        src = PATH_TO_ACRONYM.get(token, token.upper())
        x = fetch_rss(u, src, "Ontario RSS")
        dbg(f"STEP fetch.rss src={src} items={len(x)}")
        items.extend(x)

    for f in REGISTRY_FEEDS:
        x = fetch_rss(f["url"], f["name"], f["type"])
        dbg(f"STEP fetch.rss src={f['name']} items={len(x)}")
        items.extend(x)

    for f in EXTRA_RSS_FEEDS:
        x = fetch_rss(f["url"], f["name"], f["type"])
        dbg(f"STEP fetch.rss src={f['name']} items={len(x)}")
        items.extend(x)

    dbg(f"STEP fetch.done total={len(items)}")
    return items


# =========================
# SUPABASE STORAGE (PERSISTENCE)
# =========================

def supabase_object_url() -> str:
    return f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{SUPABASE_OBJECT_PATH}"

def supabase_headers_read() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
    }

def supabase_headers_write() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Content-Type": "application/json",
        "x-upsert": "true",
    }

def load_store_remote() -> Dict[str, Any]:
    if STORE_REMOTE_ENABLED is False:
        return {"meta": {}, "items": []}

    url = supabase_object_url()
    dbg(f"STEP store.load url={SUPABASE_OBJECT_PATH}")
    try:
        r = session.get(url, headers=supabase_headers_read(), timeout=30)
        if r.status_code == 404:
            dbg("STEP store.load miss=1")
            return {"meta": {}, "items": []}
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) is False:
            return {"meta": {}, "items": []}
        data.setdefault("meta", {})
        data.setdefault("items", [])
        dbg(f"STEP store.load ok items={len(data.get('items') or [])}")
        return data
    except Exception as e:
        log(f"Store load error: {e}")
        return {"meta": {}, "items": []}

def save_store_remote(store: Dict[str, Any]) -> None:
    if STORE_REMOTE_ENABLED is False:
        return
    url = supabase_object_url()
    dbg(f"STEP store.save url={SUPABASE_OBJECT_PATH} items={len(store.get('items') or [])}")
    payload = json.dumps(store, ensure_ascii=False).encode("utf-8")
    r = session.put(url, headers=supabase_headers_write(), data=payload, timeout=60)
    r.raise_for_status()
    dbg("STEP store.save ok=1")


# =========================
# OPENAI
# =========================

def get_openai_client() -> Optional[OpenAI]:
    if ENRICH_ENABLED is False:
        return None
    key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if key == "":
        raise RuntimeError("Missing OPENAI_API_KEY.")
    return OpenAI(api_key=key)

def enrich_one(client: OpenAI, item: Dict[str, Any]) -> Dict[str, Any]:
    title = str(item.get("title") or "")
    summary = str(item.get("summary") or "")
    link = str(item.get("link") or "")
    text = (title + "\n\n" + summary + "\n\n" + link).strip()

    money_vals = extract_money_values(text)
    cities = extract_cities(text)

    user_prompt = (
        "Return ONLY a JSON object.\n"
        "Keys: summary, topic, score, keywords.\n"
        "summary: 1 to 2 short sentences.\n"
        "topic must be one of:\n"
        + "\n".join(TOPICS)
        + "\n"
        "score integer 0 to 100.\n"
        "keywords array length 3.\n"
        "Scoring rules:\n"
        "80-100 direct Ontario lab policy, billing, test ordering, PHIPA, IPC, licensing, scope.\n"
        "40-79 adjacent healthcare policy, privacy, LTC, primary care, POCT.\n"
        "0-39 unrelated.\n"
        "\n"
        "Text:\n"
        + text[:6000]
    )

    resp = client.chat.completions.create(
        model=ENRICH_MODEL,
        messages=[
            {"role": "system", "content": ENRICH_SYSTEM},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.2,
    )

    raw = (resp.choices[0].message.content or "").strip()

    if DEBUG:
        dbg("STEP enrich.model_raw_start")
        print(raw[:1200])
        dbg("STEP enrich.model_raw_end")

    try:
        data = json.loads(raw)
    except Exception as e:
        item["ai_summary"] = ""
        item["ai_topic"] = ""
        item["ai_score"] = 0
        item["ai_keywords"] = []
        item["ai_error"] = f"json_parse_error: {e}"
        item["money_values"] = money_vals
        item["cities"] = cities
        item["enriched_at"] = utc_iso_z()
        return item

    item["ai_summary"] = str(data.get("summary") or "").strip()
    item["ai_topic"] = str(data.get("topic") or "").strip()

    try:
        item["ai_score"] = int(data.get("score") or 0)
    except Exception:
        item["ai_score"] = 0

    kws = data.get("keywords") or []
    if isinstance(kws, list) is False:
        kws = []
    item["ai_keywords"] = [str(x).strip() for x in kws][:3]
    item["ai_error"] = ""
    item["money_values"] = money_vals
    item["cities"] = cities
    item["enriched_at"] = utc_iso_z()
    return item


# =========================
# EDGE FUNCTION POST
# =========================

def feed_headers() -> Dict[str, str]:
    h: Dict[str, str] = {"Content-Type": "application/json"}
    if FEED_FUNCTION_KEY.strip() != "":
        h["x-api-key"] = FEED_FUNCTION_KEY
    return h

def post_full_store(items: List[Dict[str, Any]]) -> None:
    if POST_ENABLED is False:
        dbg("STEP post.skip reason=POST_ENABLED=0")
        return
    if DRY_RUN:
        dbg(f"STEP post.skip reason=DRY_RUN=1 items={len(items)}")
        return

    dbg(f"STEP post.start url={FEED_POST_URL} items={len(items)}")
    r = session.post(FEED_POST_URL, json={"items": items}, headers=feed_headers(), timeout=60)
    dbg(f"STEP post.status code={r.status_code}")

    if r.status_code == 401:
        log("POST 401. Check FEED_FUNCTION_KEY.")
        log((r.text or "")[:300])
        return

    if r.status_code == 404:
        log("POST 404. Check FEED_POST_URL.")
        log((r.text or "")[:300])
        return

    r.raise_for_status()
    dbg("STEP post.ok=1")


# =========================
# STORE LOGIC
# =========================

def index_by_id(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for it in items:
        if isinstance(it, dict) is False:
            continue
        _id = str(it.get("id") or "").strip()
        if _id:
            idx[_id] = it
    return idx

def sort_newest(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def k(x: Dict[str, Any]) -> str:
        p = str(x.get("published") or "")
        i = str(x.get("ingested_at") or "")
        return p + "|" + i
    items.sort(key=k, reverse=True)
    return items


# =========================
# MAIN
# =========================

def main() -> None:
    log("RUN start")
    log(f"DEBUG={1 if DEBUG else 0} DRY_RUN={1 if DRY_RUN else 0} POST_ENABLED={1 if POST_ENABLED else 0}")
    log(f"MAX_STORE_ITEMS={MAX_STORE_ITEMS} MAX_ENRICH_ITEMS={MAX_ENRICH_ITEMS} MAX_AGE_DAYS={MAX_AGE_DAYS}")
    log("STORE_REMOTE_ENABLED=" + ("1" if STORE_REMOTE_ENABLED else "0"))

    if RESET_STORE:
        store = {"meta": {"reset_at": utc_iso_z()}, "items": []}
        dbg("STEP store.reset=1")
    else:
        store = load_store_remote() if STORE_REMOTE_ENABLED else {"meta": {}, "items": []}

    store.setdefault("meta", {})
    store.setdefault("items", [])

    existing_idx = index_by_id(store["items"])

    raw = collect_items()
    dbg(f"STEP normalize.start raw={len(raw)}")

    dropped_old = 0
    merged = 0
    added = 0

    for ri in raw:
        source = str(ri.get("source") or "").strip()
        title = str(ri.get("title") or "").strip()
        link = str(ri.get("link") or "").strip()
        published_raw = str(ri.get("published_raw") or "").strip()

        published_dt = safe_parse_dt(published_raw)
        if is_too_old(published_dt):
            dropped_old += 1
            continue

        published_iso = published_dt.isoformat().replace("+00:00", "Z") if published_dt else ""

        _id = stable_id(source, link, title)

        item = {
            "id": _id,
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

        prev = existing_idx.get(_id)
        if prev:
            # Keep prior enrichment fields if present
            for k in ["ai_summary","ai_topic","ai_score","ai_keywords","ai_error","money_values","cities","enriched_at"]:
                if k in prev and k not in item:
                    item[k] = prev[k]
            existing_idx[_id] = {**prev, **item}
            merged += 1
        else:
            existing_idx[_id] = item
            added += 1

    dbg(f"STEP normalize.done dropped_old={dropped_old} merged={merged} added={added}")

    all_items = list(existing_idx.values())
    sort_newest(all_items)

    if len(all_items) > MAX_STORE_ITEMS:
        all_items = all_items[:MAX_STORE_ITEMS]
        dbg(f"STEP store.trim to={MAX_STORE_ITEMS}")

    # Debug sample per source
    if DEBUG and DEBUG_SAMPLE_PER_SOURCE > 0:
        dbg("STEP debug.sample_per_source")
        by_src: Dict[str, int] = {}
        for it in all_items:
            src = str(it.get("source") or "")
            by_src[src] = by_src.get(src, 0) + 1
        for src, cnt in sorted(by_src.items(), key=lambda x: x[0].lower()):
            dbg(f"SRC {src} count={cnt}")

    # Pick unenriched items for this run
    unenriched = [x for x in all_items if str(x.get("ai_summary") or "").strip() == ""]
    batch = unenriched[:MAX_ENRICH_ITEMS] if MAX_ENRICH_ITEMS > 0 else unenriched
    dbg(f"STEP enrich.plan store_items={len(all_items)} unenriched={len(unenriched)} batch={len(batch)}")

    client = get_openai_client()

    if client is None:
        dbg("STEP enrich.skip reason=ENRICH_ENABLED=0")
    else:
        for i, it in enumerate(batch, start=1):
            dbg(f"STEP enrich.item {i}/{len(batch)} src={it.get('source')} title={str(it.get('title') or '')[:70]}")
            updated = enrich_one(client, it)
            # Write back into all_items by id
            existing_idx[updated["id"]] = updated

    # Rebuild and sort after enrichment
    all_items = list(existing_idx.values())
    sort_newest(all_items)
    if len(all_items) > MAX_STORE_ITEMS:
        all_items = all_items[:MAX_STORE_ITEMS]

    store["meta"] = {
        "feed_tag": FEED_TAG,
        "updated_at": utc_iso_z(),
        "raw_collected": len(raw),
        "dropped_old": dropped_old,
        "store_items": len(all_items),
        "unenriched_remaining": len([x for x in all_items if str(x.get("ai_summary") or "").strip() == ""]),
        "enriched_this_run": len(batch) if ENRICH_ENABLED else 0,
    }
    store["items"] = all_items

    dbg("STEP store.save_local")
    with open(f"combined_feed_{FEED_TAG}.json", "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, ensure_ascii=False)

    if STORE_REMOTE_ENABLED:
        save_store_remote(store)
    else:
        dbg("STEP store.remote_skip reason=STORE_REMOTE_ENABLED=0")

    # Post full store so Lovable shows all rows
    post_full_store(all_items)

    log("RUN done")

if __name__ == "__main__":
    main()


# In[ ]:




