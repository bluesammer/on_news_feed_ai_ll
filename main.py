#!/usr/bin/env python
# coding: utf-8

# In[1]:





# In[1]:





# In[3]:





# In[4]:


# main.py
# RSS + screen-scrape -> store (Supabase Storage) -> enrich N per run -> post full store to Supabase Edge Function
# Built for Railway logs. DEBUG=1 prints step-by-step.

import os
import sys
import json
import re
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlparse

import requests
import feedparser
from requests.adapters import HTTPAdapter, Retry

from openai import OpenAI


# =========================
# CONFIG
# =========================

# Screen scrape sources
FEED_URLS = [
   # "https://news.ontario.ca/moh/en",     # JS app, handled via embedded paths
   # "https://news.ontario.ca/mltc/en",    # JS app, handled via embedded paths
    "https://www.ontariohealth.ca/news",
    "https://www.publichealthontario.ca/en/Education-and-Events/Events",
]

PATH_TO_ACRONYM = {"moh": "MOH", "mltc": "MLTC"}

# RSS sources
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
    {"name": "MOH", "type": "Ontario Newsroom", "url": "https://news.ontario.ca/moh/en/rss/news.rss"},
    {"name": "MLTC", "type": "Ontario Newsroom", "url": "https://news.ontario.ca/mltc/en/rss/news.rss"},
    {"name": "Born Ontario News", "type": "Born Ontario", "url": "https://www.bornontario.ca/news/rss/"},
    {"name": "IPC PHIPA Decisions", "type": "PHIPA Decisions", "url": "https://decisia.lexum.com/ipc-cipvp/phipa/en/rss.do"},
    {"name": "Ontario Health News", "type": "Ontario Health News", "url": "https://fetchrss.com/feed/1vjLZQBVP4Fm1vjLZ13Iw36I.rss"},
    {"name": "Google Ontario Health", "type": "Google Alert", "url": "https://www.google.ca/alerts/feeds/03113921822178662323/151430372448241348"},
    {"name": "Google LifeLabs ", "type": "Google Alert", "url": "https://www.google.com/alerts/feeds/03113921822178662323/8381571961042850572"},
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

# Supabase Storage persistence
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

SKIP_RSS_IF_NO_PUBLISHED = (os.getenv("SKIP_RSS_IF_NO_PUBLISHED", "1") or "1").strip() == "1"

SCREEN_SCRAPE_MAX_LINKS_PER_PAGE = int((os.getenv("SCREEN_SCRAPE_MAX_LINKS_PER_PAGE", "30") or "30").strip())
SCREEN_SCRAPE_TIMEOUT_SEC = int((os.getenv("SCREEN_SCRAPE_TIMEOUT_SEC", "30") or "30").strip())

# Ontario Newsroom (JS) support
NEWSROOM_MAX_PATHS = int((os.getenv("NEWSROOM_MAX_PATHS", "60") or "60").strip())
NEWSROOM_FETCH_ARTICLE_META = (os.getenv("NEWSROOM_FETCH_ARTICLE_META", "1") or "1").strip() == "1"
NEWSROOM_META_FETCH_LIMIT = int((os.getenv("NEWSROOM_META_FETCH_LIMIT", "25") or "25").strip())
NEWSROOM_META_TIMEOUT_SEC = int((os.getenv("NEWSROOM_META_TIMEOUT_SEC", "20") or "20").strip())

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
    "Unrelated",
]
ALLOWED_TOPICS = set(TOPICS)

ENRICH_SYSTEM = (
    "Return ONLY valid JSON. No markdown. No code fences. No extra text.\n"
    "Keys: summary, topic, score, keywords, city.\n"
    "summary: 1 to 2 short sentences.\n"
    "topic must match an allowed topic.\n"
    "score must be an integer 0 to 100.\n"
    "keywords must be an array of 3 short strings.\n"
    "city must be a single Ontario city name string, or empty string.\n"
    "If the text mentions a county/region/district, convert it to the main Ontario city.\n"
)

MONEY_RE = re.compile(r"(?i)\$[\s]*([\d]{1,3}(?:,[\d]{3})*(?:\.\d{1,2})?|\d+(?:\.\d{1,2})?)")

# Newsroom patterns for embedded paths and article metadata
NEWSROOM_PATH_RE = re.compile(
    r'(?i)(/en/(?:release|bulletin|backgrounder|statement|media-advisory)/[^"\'\s\\]+)'
)
OG_TITLE_RE = re.compile(r'(?is)<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']')
HTML_TITLE_RE = re.compile(r"(?is)<title>\s*(.*?)\s*</title>")
PUBLISHED_META_RE = re.compile(r'(?is)<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']')
JSONLD_DATE_RE = re.compile(r'(?is)"datePublished"\s*:\s*"([^"]+)"')


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
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; FeedCollector/1.0; +https://example.invalid)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
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

def parse_iso_dt(s: str) -> Optional[datetime]:
    t = (s or "").strip()
    if t == "":
        return None
    try:
        if t.endswith("Z"):
            t = t.replace("Z", "+00:00")
        dt = datetime.fromisoformat(t)
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

def clamp_0_100(x: int) -> int:
    if x < 0:
        return 0
    if x > 100:
        return 100
    return x

def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default

def extract_money_values(text: str) -> List[str]:
    vals: List[str] = []
    for m in MONEY_RE.findall(text or ""):
        v = "$" + str(m).strip()
        if v not in vals:
            vals.append(v)
    return vals[:10]

def extract_json_object(text: str) -> str:
    t = (text or "").strip()
    if t == "":
        return ""
    start = t.find("{")
    end = t.rfind("}")
    if start != -1 and end != -1 and end > start:
        return t[start:end + 1]
    return ""

def heuristic_city_hint(text: str) -> str:
    s = text or ""
    pats = [
        r"(?i)\bcity of\s+([a-z][a-z\s\.\-]{2,60})\b",
        r"(?i)\btown of\s+([a-z][a-z\s\.\-]{2,60})\b",
        r"(?i)\bmunicipality of\s+([a-z][a-z\s\.\-]{2,60})\b",
        r"(?i)\bregion of\s+([a-z][a-z\s\.\-]{2,60})\b",
        r"(?i)\bregional municipality of\s+([a-z][a-z\s\.\-]{2,60})\b",
        r"(?i)\bcounty of\s+([a-z][a-z\s\.\-]{2,60})\b",
        r"(?i)\bdistrict of\s+([a-z][a-z\s\.\-]{2,60})\b",
        r"(?i)\bmunicipal region of\s+([a-z][a-z\s\.\-]{2,60})\b",
    ]
    hits: List[str] = []
    for p in pats:
        found = re.findall(p, s)
        for x in found[:3]:
            x2 = " ".join(str(x).strip().split())
            if x2:
                hits.append(x2)
    if len(hits) == 0:
        return ""
    return "Location mentions: " + "; ".join(hits[:5])

def host_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def infer_source_from_url(url: str) -> str:
    u = (url or "").strip()
    if u == "":
        return "Screen Scrape"
    h = host_of(u)
    if "news.ontario.ca" in h:
        path = urlparse(u).path.strip("/").split("/")
        if len(path) >= 1:
            k = path[0].lower()
            if k in PATH_TO_ACRONYM:
                return PATH_TO_ACRONYM[k]
        return "Ontario Newsroom"
    if "ontariohealth.ca" in h:
        return "Ontario Health"
    if "publichealthontario.ca" in h:
        return "Public Health Ontario"
    return h if h else "Screen Scrape"

def infer_type_from_url(url: str) -> str:
    u = (url or "").lower()
    if "news.ontario.ca" in u:
        return "Ontario Newsroom"
    if "ontariohealth.ca/news" in u:
        return "Ontario Health News"
    if "/education-and-events/events" in u:
        return "PHO Events"
    return "Screen Scrape"

def cleanup_title(t: str) -> str:
    x = re.sub(r"(?is)<[^>]+>", " ", t or "")
    x = " ".join(x.split()).strip()
    return x

def derive_title_from_path(path: str) -> str:
    p = (path or "").strip("/")
    segs = [s for s in p.split("/") if s]
    if len(segs) == 0:
        return ""
    last = segs[-1]
    last = re.sub(r"[^a-zA-Z0-9\-]+", "", last)
    if last == "":
        return ""
    words = [w for w in last.replace("-", " ").split() if w]
    return " ".join([w[:1].upper() + w[1:] for w in words])


# =========================
# FETCH
# =========================

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

def _parse_links_bs4(html: str, base_url: str) -> List[Dict[str, str]]:
    from bs4 import BeautifulSoup  # type: ignore
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, str]] = []

    for a in soup.find_all("a"):
        href = (a.get("href") or "").strip()
        text = " ".join((a.get_text(" ") or "").split()).strip()
        if href == "":
            continue
        if href.startswith("javascript:"):
            continue
        link = urljoin(base_url, href)
        if text == "":
            continue
        out.append({"title": text, "link": link})

    return out

def _parse_links_regex(html: str, base_url: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for m in re.finditer(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html or ""):
        href = " ".join((m.group(1) or "").split()).strip()
        inner = m.group(2) or ""
        text = re.sub(r"(?is)<[^>]+>", " ", inner)
        text = " ".join(text.split()).strip()
        if href == "" or text == "":
            continue
        if href.startswith("javascript:"):
            continue
        link = urljoin(base_url, href)
        out.append({"title": text, "link": link})
    return out

def newsroom_extract_article_urls(html: str, base_url: str) -> List[str]:
    base = "https://news.ontario.ca"
    paths = NEWSROOM_PATH_RE.findall(html or "")
    uniq: List[str] = []
    seen = set()
    for p in paths:
        p2 = (p or "").strip()
        if p2 == "":
            continue
        if p2.endswith((".png", ".jpg", ".jpeg", ".svg", ".css", ".js")):
            continue
        u = urljoin(base, p2)
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
        if len(uniq) >= NEWSROOM_MAX_PATHS:
            break
    return uniq

def fetch_newsroom_article_meta(article_url: str) -> Dict[str, str]:
    out = {"title": "", "published_iso": ""}
    try:
        r = session.get(article_url, timeout=NEWSROOM_META_TIMEOUT_SEC)
        r.raise_for_status()
        html = r.text or ""

        m = OG_TITLE_RE.search(html)
        if m:
            out["title"] = cleanup_title(m.group(1))
        if out["title"] == "":
            m2 = HTML_TITLE_RE.search(html)
            if m2:
                out["title"] = cleanup_title(m2.group(1))

        mp = PUBLISHED_META_RE.search(html)
        if mp:
            dt = safe_parse_dt(mp.group(1).strip())
            if dt:
                out["published_iso"] = dt.isoformat().replace("+00:00", "Z")
        if out["published_iso"] == "":
            mj = JSONLD_DATE_RE.search(html)
            if mj:
                dt = safe_parse_dt(mj.group(1).strip())
                if dt:
                    out["published_iso"] = dt.isoformat().replace("+00:00", "Z")

    except Exception:
        return out
    return out

def fetch_screen_scrape(url: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    try:
        r = session.get(url, timeout=SCREEN_SCRAPE_TIMEOUT_SEC)
        r.raise_for_status()
        html = r.text or ""

        base_host = host_of(url)

        # Ontario Newsroom is a JS app, but HTML often includes embedded article paths.
        if "news.ontario.ca" in base_host:
            article_urls = newsroom_extract_article_urls(html, url)
            dbg(f"STEP newsroom.paths url={url} paths={len(article_urls)}")

            meta_fetched = 0
            for aurl in article_urls:
                path = urlparse(aurl).path
                title = derive_title_from_path(path)
                published_raw = ""

                if NEWSROOM_FETCH_ARTICLE_META and meta_fetched < NEWSROOM_META_FETCH_LIMIT:
                    meta = fetch_newsroom_article_meta(aurl)
                    if meta.get("title"):
                        title = meta["title"]
                    if meta.get("published_iso"):
                        published_raw = meta["published_iso"]
                    meta_fetched += 1

                if title == "":
                    title = aurl

                out.append(
                    {
                        "source": infer_source_from_url(url),
                        "type": infer_type_from_url(url),
                        "title": title,
                        "link": aurl,
                        "published_raw": published_raw,
                        "summary": "",
                    }
                )

                if len(out) >= SCREEN_SCRAPE_MAX_LINKS_PER_PAGE:
                    break

            return out

        # Normal HTML pages
        parsed: List[Dict[str, str]] = []
        try:
            parsed = _parse_links_bs4(html, url)
        except Exception:
            parsed = _parse_links_regex(html, url)

        seen: set = set()

        for it in parsed:
            title = str(it.get("title") or "").strip()
            link = str(it.get("link") or "").strip()

            if title == "" or link == "":
                continue

            link_host = host_of(link)
            if base_host and link_host and link_host != base_host:
                continue

            key = (title.lower(), link.lower())
            if key in seen:
                continue
            seen.add(key)

            out.append(
                {
                    "source": infer_source_from_url(url),
                    "type": infer_type_from_url(url),
                    "title": title,
                    "link": link,
                    "published_raw": "",
                    "summary": "",
                }
            )

            if len(out) >= SCREEN_SCRAPE_MAX_LINKS_PER_PAGE:
                break

    except Exception as e:
        log(f"Screen scrape error url={url}: {e}")

    return out

def collect_items() -> List[Dict[str, Any]]:
    dbg("STEP fetch.start")
    items: List[Dict[str, Any]] = []

    for u in FEED_URLS:
        x = fetch_screen_scrape(u)
        dbg(f"STEP fetch.screen url={u} items={len(x)}")
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
# SUPABASE STORAGE
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
    loc_hint = heuristic_city_hint(text)

    user_prompt = (
        "Return ONLY a JSON object.\n"
        "Keys: summary, topic, score, keywords, city.\n"
        "summary: 1 to 2 short sentences.\n"
        "topic must be one of:\n"
        + "\n".join(TOPICS)
        + "\n"
        "score integer 0 to 100.\n"
        "keywords array length 3.\n"
        "city rules:\n"
        "Return ONE Ontario city name.\n"
        "If text has a region/county/district, convert it to the main Ontario city (largest/admin centre).\n"
        "If multiple cities, return the most relevant.\n"
        "If no Ontario city fits, return empty string.\n"
        "\n"
        + (loc_hint + "\n" if loc_hint else "")
        + "Text:\n"
        + text[:6000]
    )

    resp = client.chat.completions.create(
        model=ENRICH_MODEL,
        messages=[
            {"role": "system", "content": ENRICH_SYSTEM},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.0,
    )

    raw = (resp.choices[0].message.content or "").strip()

    if DEBUG:
        dbg("STEP enrich.model_raw_start")
        print(raw[:1200])
        dbg("STEP enrich.model_raw_end")

    j = extract_json_object(raw)
    try:
        data = json.loads(j)
    except Exception as e:
        item["ai_summary"] = ""
        item["ai_topic"] = "Unrelated"
        item["ai_score"] = 0
        item["ai_keywords"] = []
        item["ai_city"] = ""
        item["ai_error"] = f"json_parse_error: {e}"
        item["money_values"] = money_vals
        item["enriched_at"] = utc_iso_z()
        return item

    ai_summary = str(data.get("summary") or "").strip()
    ai_topic = str(data.get("topic") or "").strip()
    ai_score = clamp_0_100(safe_int(data.get("score"), 0))

    if ai_topic not in ALLOWED_TOPICS:
        ai_topic = "Unrelated"
        ai_score = 0
    if ai_topic == "Unrelated":
        ai_score = 0

    kws = data.get("keywords") or []
    if isinstance(kws, list) is False:
        kws = []
    ai_keywords = [str(x).strip() for x in kws][:3]

    ai_city = str(data.get("city") or "").strip()
    ai_city = re.sub(r"\s+", " ", ai_city).strip()
    if len(ai_city) > 80:
        ai_city = ai_city[:80].strip()

    item["ai_summary"] = ai_summary
    item["ai_topic"] = ai_topic
    item["ai_score"] = ai_score
    item["ai_keywords"] = ai_keywords
    item["ai_city"] = ai_city
    item["ai_error"] = ""
    item["money_values"] = money_vals
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
    r = session.post(FEED_POST_URL, json={"items": items}, headers=feed_headers(), timeout=120)
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

ENRICH_FIELDS = [
    "ai_summary",
    "ai_topic",
    "ai_score",
    "ai_keywords",
    "ai_city",
    "ai_error",
    "money_values",
    "enriched_at",
]

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
    def k(x: Dict[str, Any]) -> datetime:
        p = parse_iso_dt(str(x.get("published") or ""))
        i = parse_iso_dt(str(x.get("ingested_at") or ""))
        return p or i or datetime(1970, 1, 1, tzinfo=timezone.utc)
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
    log(f"NEWSROOM_FETCH_ARTICLE_META={1 if NEWSROOM_FETCH_ARTICLE_META else 0} NEWSROOM_META_FETCH_LIMIT={NEWSROOM_META_FETCH_LIMIT}")

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
        if published_dt is None:
            published_dt = parse_iso_dt(published_raw)

        if is_too_old(published_dt):
            dropped_old += 1
            continue

        published_iso = published_dt.isoformat().replace("+00:00", "Z") if published_dt else ""

        _id = stable_id(source, link, title)

        base_item = {
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
            for k2 in ENRICH_FIELDS:
                if k2 in prev:
                    base_item[k2] = prev.get(k2)
            existing_idx[_id] = {**prev, **base_item}
            merged += 1
        else:
            existing_idx[_id] = base_item
            added += 1

    dbg(f"STEP normalize.done dropped_old={dropped_old} merged={merged} added={added}")

    all_items = list(existing_idx.values())
    sort_newest(all_items)

    if len(all_items) > MAX_STORE_ITEMS:
        all_items = all_items[:MAX_STORE_ITEMS]
        dbg(f"STEP store.trim to={MAX_STORE_ITEMS}")

    if DEBUG and DEBUG_SAMPLE_PER_SOURCE > 0:
        dbg("STEP debug.sample_per_source")
        by_src: Dict[str, int] = {}
        for it in all_items:
            src = str(it.get("source") or "")
            by_src[src] = by_src.get(src, 0) + 1
        for src, cnt in sorted(by_src.items(), key=lambda x: x[0].lower()):
            dbg(f"SRC {src} count={cnt}")

    unenriched = [x for x in all_items if str(x.get("ai_summary") or "").strip() == ""]
    batch = unenriched[:MAX_ENRICH_ITEMS] if MAX_ENRICH_ITEMS > 0 else unenriched
    dbg(f"STEP enrich.plan store_items={len(all_items)} unenriched={len(unenriched)} batch={len(batch)}")

    client = get_openai_client()

    ok_ct = 0
    err_ct = 0

    if client is None:
        dbg("STEP enrich.skip reason=ENRICH_ENABLED=0")
    else:
        for i, it in enumerate(batch, start=1):
            dbg(f"STEP enrich.item {i}/{len(batch)} src={it.get('source')} title={str(it.get('title') or '')[:70]}")
            updated = enrich_one(client, it)
            if str(updated.get("ai_error") or "").strip() == "":
                ok_ct += 1
            else:
                err_ct += 1
            dbg(f"STEP enrich.result {i}/{len(batch)} ok={1 if str(updated.get('ai_error') or '').strip()=='' else 0}")
            existing_idx[updated["id"]] = updated

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
        "enrich_ok": ok_ct,
        "enrich_err": err_ct,
    }
    store["items"] = all_items

    dbg("STEP store.save_local")
    with open(f"combined_feed_{FEED_TAG}.json", "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, ensure_ascii=False)

    if STORE_REMOTE_ENABLED:
        save_store_remote(store)
    else:
        dbg("STEP store.remote_skip reason=STORE_REMOTE_ENABLED=0")

    dbg(f"STEP post.plan items={len(all_items)}")
    post_full_store(all_items)

    log(f"RUN done store_items={len(all_items)} enrich_ok={ok_ct} enrich_err={err_ct}")

if __name__ == "__main__":
    main()


# In[ ]:




