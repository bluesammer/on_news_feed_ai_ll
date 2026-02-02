#!/usr/bin/env python
# coding: utf-8

# In[1]:





# In[1]:





# In[3]:





# In[4]:


# main.py
# Feed ingest -> store (Supabase Storage) -> enrich N items -> post full store to Supabase Edge Function
# Debug heavy: prints phase steps, counts, samples, and HTTP statuses for Railway logs

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


# =====================
# CONFIG
# =====================
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

# Debug verbosity controls
DEBUG = (os.getenv("DEBUG", "1") or "1").strip() == "1"
DEBUG_SAMPLE_PER_SOURCE = (os.getenv("DEBUG_SAMPLE_PER_SOURCE", "1") or "1").strip() == "1"
DEBUG_PRINT_ITEM_JSON = (os.getenv("DEBUG_PRINT_ITEM_JSON", "0") or "0").strip() == "1"  # can get huge

# Optional page text expansion
EXPAND_PAGE_TEXT = (os.getenv("EXPAND_PAGE_TEXT", "0") or "0").strip() == "1"
PAGE_TEXT_MAX_CHARS = int((os.getenv("PAGE_TEXT_MAX_CHARS", "6000") or "6000").strip())

# Supabase Storage store
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

# Edge Function post for Lovable
FEED_POST_URL = (os.getenv(
    "FEED_POST_URL",
    "https://tcgdugdhwtbyeygdqdob.supabase.co/functions/v1/feed",
) or "").strip()

FEED_FUNCTION_KEY = (os.getenv("FEED_FUNCTION_KEY", "") or "").strip()
POST_ENABLED = (os.getenv("POST_ENABLED", "0") or "0").strip() == "1"
DRY_RUN = (os.getenv("DRY_RUN", "0") or "0").strip() == "1"

# Local save
OUT_JSON_LOCAL = (os.getenv("OUT_JSON_LOCAL", f"combined_feed_{FEED_TAG}.json") or "").strip()

# Deduping
RESET_STORE = (os.getenv("RESET_STORE", "0") or "0").strip() == "1"
REENRICH_ALL = (os.getenv("REENRICH_ALL", "0") or "0").strip() == "1"

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

MONEY_RE = re.compile(r"(?i)\$[\s]*([\d]{1,3}(?:,[\d]{3})*(?:\.\d{1,2})?|\d+(?:\.\d{1,2})?)")


# =====================
# LOGGING
# =====================
def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    print(f"[{ts}] {msg}")
    sys.stdout.flush()

def dbg(msg: str) -> None:
    if DEBUG:
        log(msg)

def utc_iso_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# =====================
# HTTP
# =====================
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


# =====================
# TEXT HELPERS
# =====================
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

def is_too_old(published_dt: Optional[datetime]) -> bool:
    if published_dt is None:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
    return published_dt < cutoff

def stable_id(*parts: str) -> str:
    raw = "|".join([str(p).strip() for p in parts if p is not None])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

def extract_money_values(text: str) -> List[str]:
    vals: List[str] = []
    for m in MONEY_RE.findall(text or ""):
        v = "$" + str(m).strip()
        if v not in vals:
            vals.append(v)
    return vals[:10]

def strip_html_to_text(html: str, max_chars: int) -> str:
    h = html or ""
    h = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", h)
    h = re.sub(r"(?is)<br\s*/?>", "\n", h)
    h = re.sub(r"(?is)</p>", "\n", h)
    txt = re.sub(r"(?is)<.*?>", " ", h)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt[:max_chars]

def fetch_page_text(url: str) -> str:
    try:
        r = session.get(url, timeout=25, headers={"User-Agent": "Mozilla/5.0"})
        dbg(f"PAGE_FETCH status={r.status_code} url={url[:110]}")
        if r.status_code >= 400:
            return ""
        return strip_html_to_text(r.text or "", PAGE_TEXT_MAX_CHARS)
    except Exception as e:
        dbg(f"PAGE_FETCH error={e} url={url[:110]}")
        return ""

def extract_json_object(text: str) -> str:
    t = (text or "").strip()
    if t == "":
        return ""
    start = t.find("{")
    end = t.rfind("}")
    if start != -1 and end != -1 and end > start:
        return t[start:end + 1]
    return ""


# =====================
# FETCH
# =====================
def fetch_ontario_news_pages() -> List[Dict[str, Any]]:
    dbg("PHASE fetch_ontario_news_pages start")
    out: List[Dict[str, Any]] = []
    try:
        r = session.get(API_URL, timeout=30)
        dbg(f"Ontario API status={r.status_code}")
        r.raise_for_status()
        data = r.json()

        if isinstance(data, dict) and isinstance(data.get("items"), list):
            items = data["items"]
        elif isinstance(data, list):
            items = data
        else:
            items = []

        dbg(f"Ontario API items={len(items)}")

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
    dbg(f"PHASE fetch_ontario_news_pages done count={len(out)}")
    return out

def fetch_rss(url: str, source_name: str, source_type: str) -> List[Dict[str, Any]]:
    dbg(f"PHASE fetch_rss start source={source_name} url={url[:110]}")
    out: List[Dict[str, Any]] = []
    try:
        parsed = feedparser.parse(url)
        entries = parsed.entries or []
        dbg(f"RSS parsed source={source_name} entries={len(entries)} bozo={getattr(parsed, 'bozo', None)}")
        if getattr(parsed, "bozo", 0) == 1:
            err = getattr(parsed, "bozo_exception", None)
            dbg(f"RSS bozo_exception source={source_name} err={err}")

        for e in entries:
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
    dbg(f"PHASE fetch_rss done source={source_name} count={len(out)}")
    return out

def collect_items() -> List[Dict[str, Any]]:
    dbg("PHASE collect_items start")
    items: List[Dict[str, Any]] = []

    api_items = fetch_ontario_news_pages()
    items.extend(api_items)

    for u in FEED_URLS:
        token = u.rstrip("/").split("/")[-2] if "/" in u.rstrip("/") else u
        src = PATH_TO_ACRONYM.get(token, token.upper())
        rss_items = fetch_rss(u, src, "Ontario RSS")
        items.extend(rss_items)

    for f in REGISTRY_FEEDS:
        rss_items = fetch_rss(f["url"], f["name"], f["type"])
        items.extend(rss_items)

    for f in EXTRA_RSS_FEEDS:
        rss_items = fetch_rss(f["url"], f["name"], f["type"])
        items.extend(rss_items)

    dbg(f"PHASE collect_items done total={len(items)}")
    return items


# =====================
# SUPABASE STORAGE
# =====================
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
        return {"items": [], "meta": {}}

    url = supabase_object_url()
    dbg(f"STORE load url={url}")

    try:
        r = session.get(url, headers=supabase_headers_read(), timeout=30)
        dbg(f"STORE load status={r.status_code}")
        if r.status_code == 404:
            dbg("STORE load 404, starting fresh")
            return {"items": [], "meta": {}}
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) is False:
            dbg("STORE load invalid json shape, starting fresh")
            return {"items": [], "meta": {}}
        data.setdefault("items", [])
        data.setdefault("meta", {})
        dbg(f"STORE load ok count={len(data.get('items') or [])}")
        return data
    except Exception as e:
        log(f"STORE load error: {e}")
        return {"items": [], "meta": {}}

def save_store_remote(store: Dict[str, Any]) -> None:
    if STORE_REMOTE_ENABLED is False:
        return

    url = supabase_object_url()
    payload = json.dumps(store, ensure_ascii=False).encode("utf-8")
    dbg(f"STORE save url={url} bytes={len(payload)}")

    r = session.put(url, headers=supabase_headers_write(), data=payload, timeout=60)
    dbg(f"STORE save status={r.status_code}")
    if r.status_code >= 400:
        dbg((r.text or "")[:400])
    r.raise_for_status()

def save_store_local(store: Dict[str, Any]) -> str:
    with open(OUT_JSON_LOCAL, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, ensure_ascii=False)
    return os.path.abspath(OUT_JSON_LOCAL)

def index_by_id(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for it in items:
        if isinstance(it, dict) is False:
            continue
        _id = str(it.get("id") or "").strip()
        if _id:
            out[_id] = it
    return out


# =====================
# OPENAI ENRICH
# =====================
def get_openai_client() -> Optional[OpenAI]:
    if ENRICH_ENABLED is False:
        return None
    key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if key == "":
        raise RuntimeError("OPENAI_API_KEY missing")
    return OpenAI(api_key=key)

ENRICH_SYSTEM = (
    "Return only valid JSON, no extra text.\n"
    "Keys: topic, score, short_summary, keywords, cities.\n"
    "topic must match allowed topics.\n"
    "score must be an integer 0 to 100.\n"
    "short_summary must be one short sentence.\n"
    "keywords must be an array of 3 short strings.\n"
    "cities must be an array of up to 5 city names, empty array if none.\n"
)

def enrich_one(client: OpenAI, item: Dict[str, Any]) -> Dict[str, Any]:
    title = str(item.get("title") or "").strip()
    summary = str(item.get("summary") or "").strip()
    link = str(item.get("link") or "").strip()

    if EXPAND_PAGE_TEXT and link.startswith("http"):
        page_text = fetch_page_text(link)
        if page_text.strip() != "":
            summary = page_text

    text = (title + "\n\n" + summary + "\n\n" + link).strip()
    money_vals = extract_money_values(text)

    user_prompt = (
        "Allowed topics:\n"
        + "\n".join(TOPICS)
        + "\n\n"
        "Scoring guide:\n"
        "80-100 direct lab policy, billing, test ordering, PHIPA, IPC, licensing, scope.\n"
        "40-79 adjacent healthcare policy, LTC, primary care, POCT.\n"
        "0-39 unrelated.\n\n"
        "Text:\n"
        + text[:6000]
    )

    try:
        resp = client.chat.completions.create(
            model=ENRICH_MODEL,
            messages=[
                {"role": "system", "content": ENRICH_SYSTEM},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
        )

        raw = (resp.choices[0].message.content or "").strip()
        dbg(f"OPENAI raw_len={len(raw)} id={item.get('id')}")
        if DEBUG_PRINT_ITEM_JSON:
            dbg("OPENAI raw=" + raw[:1200])

        j = extract_json_object(raw)
        data = json.loads(j)

        topic = str(data.get("topic") or "").strip()
        score = int(data.get("score") or 0)
        short_summary = str(data.get("short_summary") or "").strip()

        keywords = data.get("keywords") or []
        if isinstance(keywords, list) is False:
            keywords = []
        keywords = [str(x).strip() for x in keywords][:3]

        cities = data.get("cities") or []
        if isinstance(cities, list) is False:
            cities = []
        cities = [str(x).strip() for x in cities if str(x).strip() != ""][:5]

        item["ai_topic"] = topic
        item["ai_score"] = max(0, min(100, score))
        item["short_summary"] = short_summary
        item["ai_keywords"] = keywords
        item["ai_cities"] = cities
        item["money_values"] = money_vals
        item["enriched_at"] = utc_iso_z()
        item["enrich_status"] = "ok"
        return item

    except Exception as e:
        dbg(f"OPENAI error id={item.get('id')} err={e}")
        item["ai_topic"] = item.get("ai_topic", "") or ""
        item["ai_score"] = int(item.get("ai_score") or 0)
        item["short_summary"] = item.get("short_summary", "") or ""
        item["ai_keywords"] = item.get("ai_keywords", []) or []
        item["ai_cities"] = item.get("ai_cities", []) or []
        item["money_values"] = money_vals
        item["enriched_at"] = utc_iso_z()
        item["enrich_status"] = f"error: {e}"
        return item


# =====================
# EDGE FUNCTION POST
# =====================
def feed_headers() -> Dict[str, str]:
    h: Dict[str, str] = {"Content-Type": "application/json"}
    if FEED_FUNCTION_KEY.strip() != "":
        h["x-api-key"] = FEED_FUNCTION_KEY
    return h

def post_to_edge(all_items: List[Dict[str, Any]]) -> None:
    if POST_ENABLED is False:
        log("POST_ENABLED=0, skip post")
        return
    if DRY_RUN:
        log(f"DRY_RUN=1, skip post. Items={len(all_items)}")
        return

    dbg(f"POST url={FEED_POST_URL} items={len(all_items)} header_key_present={'1' if FEED_FUNCTION_KEY else '0'}")
    r = session.post(FEED_POST_URL, json={"items": all_items}, headers=feed_headers(), timeout=60)

    log(f"POST status={r.status_code}")
    if r.status_code >= 400:
        log((r.text or "")[:600])

    if r.status_code == 401:
        log("Post 401, check FEED_FUNCTION_KEY")
        return

    if r.status_code == 404:
        log("Post 404, check FEED_POST_URL")
        return

    r.raise_for_status()
    log(f"Posted items: {len(all_items)}")


# =====================
# MAIN
# =====================
def main() -> None:
    log("Run start")
    log(f"FEED_TAG={FEED_TAG}")
    log(f"MAX_STORE_ITEMS={MAX_STORE_ITEMS}")
    log(f"MAX_ENRICH_ITEMS={MAX_ENRICH_ITEMS}")
    log(f"MAX_AGE_DAYS={MAX_AGE_DAYS}")
    log(f"STORE_REMOTE_ENABLED={'1' if STORE_REMOTE_ENABLED else '0'}")
    log(f"POST_ENABLED={'1' if POST_ENABLED else '0'}")
    log(f"DRY_RUN={'1' if DRY_RUN else '0'}")
    log(f"ENRICH_ENABLED={'1' if ENRICH_ENABLED else '0'}")
    log(f"EXPAND_PAGE_TEXT={'1' if EXPAND_PAGE_TEXT else '0'}")
    log(f"DEBUG={'1' if DEBUG else '0'}")

    if RESET_STORE:
        store: Dict[str, Any] = {"meta": {"reset_at": utc_iso_z(), "feed_tag": FEED_TAG}, "items": []}
        dbg("STORE reset enabled, using empty store")
    else:
        store = load_store_remote() if STORE_REMOTE_ENABLED else {"meta": {"feed_tag": FEED_TAG}, "items": []}

    store.setdefault("meta", {})
    store.setdefault("items", [])
    dbg(f"STORE starting_count={len(store['items'])}")

    existing_idx = index_by_id(store["items"])

    raw = collect_items()
    log(f"Collected raw items: {len(raw)}")

    # Debug samples per source
    if DEBUG_SAMPLE_PER_SOURCE:
        seen_src: Dict[str, int] = {}
        for it in raw:
            s = str(it.get("source") or "")
            if s not in seen_src:
                seen_src[s] = 1
                dbg(f"SAMPLE source={s} title={str(it.get('title') or '')[:90]}")
        dbg(f"SAMPLE unique_sources={len(seen_src)}")

    dropped_old = 0
    normalized: List[Dict[str, Any]] = []

    dbg("PHASE normalize start")
    for ri in raw:
        source = str(ri.get("source") or "").strip()
        title = str(ri.get("title") or "").strip()
        link = str(ri.get("link") or "").strip()
        published_raw = str(ri.get("published_raw") or "").strip()
        summary = str(ri.get("summary") or "").strip()

        published_dt = safe_parse_dt(published_raw)
        if is_too_old(published_dt):
            dropped_old += 1
            continue

        published_iso = published_dt.isoformat().replace("+00:00", "Z") if published_dt else ""
        _id = stable_id(source, link, title)

        base = {
            "id": _id,
            "feed_tag": FEED_TAG,
            "source": source,
            "type": str(ri.get("type") or "").strip(),
            "title": title,
            "link": link,
            "summary": summary,
            "published": published_iso,
            "published_raw": published_raw,
            "ingested_at": utc_iso_z(),
        }

        prev = existing_idx.get(_id)
        if prev:
            for k in ["ai_topic", "ai_score", "short_summary", "ai_keywords", "ai_cities", "money_values", "enriched_at", "enrich_status"]:
                if k in prev and k not in base:
                    base[k] = prev[k]

        normalized.append(base)

    dbg(f"PHASE normalize done remaining={len(normalized)} dropped_old={dropped_old}")

    def sort_key(x: Dict[str, Any]) -> str:
        return (x.get("published") or "") + "|" + (x.get("ingested_at") or "")

    normalized.sort(key=sort_key, reverse=True)
    dbg("PHASE sort done")

    normalized = normalized[:MAX_STORE_ITEMS]
    dbg(f"PHASE cap_store done kept={len(normalized)}")

    # Pick items for enrichment
    to_enrich: List[Dict[str, Any]] = []
    if ENRICH_ENABLED:
        for it in normalized:
            if len(to_enrich) >= MAX_ENRICH_ITEMS:
                break
            if REENRICH_ALL:
                to_enrich.append(it)
            else:
                if (it.get("enriched_at") or "").strip() == "":
                    to_enrich.append(it)

    log(f"Enrich this run: {len(to_enrich)}")

    if DEBUG and len(to_enrich) > 0:
        dbg("ENRICH_PICK_LIST start")
        for it in to_enrich:
            dbg(f"  pick id={it.get('id')} source={it.get('source')} title={str(it.get('title') or '')[:80]}")
        dbg("ENRICH_PICK_LIST end")

    client = get_openai_client() if ENRICH_ENABLED else None

    enrich_map: Dict[str, Dict[str, Any]] = {}
    if client and len(to_enrich) > 0:
        dbg("PHASE enrich start")
        for i, it in enumerate(to_enrich, start=1):
            log(f"Enrich {i}/{len(to_enrich)} source={it.get('source')} title={str(it.get('title') or '')[:80]}")
            enrich_map[it["id"]] = enrich_one(client, it)
            if DEBUG_PRINT_ITEM_JSON:
                dbg("ENRICHED_ITEM_JSON=" + json.dumps(enrich_map[it["id"]], ensure_ascii=False)[:2000])
        dbg("PHASE enrich done")
    else:
        dbg("PHASE enrich skipped")

    # Merge back
    final_items: List[Dict[str, Any]] = []
    for it in normalized:
        if it["id"] in enrich_map:
            final_items.append(enrich_map[it["id"]])
        else:
            it.setdefault("ai_topic", "")
            it.setdefault("ai_score", 0)
            it.setdefault("short_summary", "")
            it.setdefault("ai_keywords", [])
            it.setdefault("ai_cities", [])
            it.setdefault("money_values", extract_money_values((it.get("title") or "") + " " + (it.get("summary") or "")))
            it.setdefault("enrich_status", "")
            final_items.append(it)

    store["items"] = final_items
    store["meta"]["feed_tag"] = FEED_TAG
    store["meta"]["updated_at"] = utc_iso_z()
    store["meta"]["count"] = len(final_items)
    store["meta"]["raw_collected"] = len(raw)
    store["meta"]["dropped_old"] = dropped_old
    store["meta"]["max_age_days"] = MAX_AGE_DAYS
    store["meta"]["max_store_items"] = MAX_STORE_ITEMS
    store["meta"]["max_enrich_items"] = MAX_ENRICH_ITEMS

    local_path = save_store_local(store)
    log(f"Saved local: {local_path}")

    if STORE_REMOTE_ENABLED:
        save_store_remote(store)
        log(f"Saved remote: {SUPABASE_BUCKET}/{SUPABASE_OBJECT_PATH}")
    else:
        dbg("STORE remote disabled, skipped save")

    post_to_edge(final_items)

    log("Run done")


if __name__ == "__main__":
    main()


# In[ ]:




