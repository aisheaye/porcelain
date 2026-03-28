import argparse
import json
import logging
import os
import re
import sqlite3
import time
from collections import OrderedDict
from urllib.parse import parse_qs, unquote, urlparse

import requests

DB_PATH = "porcelain_auction.db"
COOKIE_PATH = "cookie.txt"
IMG_FOLDER = "images"
DEFAULT_DELAY = 3.0
DEFAULT_MAX_PAGES = 120
VESSEL_TYPES = ["盘", "碗", "瓶", "尊", "罐", "杯", "盏", "洗", "炉", "壶", "盒", "缸", "梅瓶", "玉壶春瓶", "天球瓶", "抱月瓶"]
GLAZE_TERMS = ["青花", "釉里红", "青花釉里红", "斗彩", "五彩", "粉彩", "珐琅彩", "单色釉", "颜色釉", "祭红", "霁红", "郎窑红", "天蓝釉", "青釉", "白釉", "黄釉", "豇豆红", "窑变釉"]
MOTIF_TERMS = ["龙纹", "凤纹", "缠枝莲", "花卉纹", "八吉祥", "开光", "婴戏", "山水", "人物", "折枝花", "莲纹", "云龙", "海水龙纹"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

MING_REIGNS = ["洪武", "永乐", "宣德", "成化", "弘治", "正德", "嘉靖", "隆庆", "万历", "天启", "崇祯"]
QING_REIGNS = ["顺治", "康熙", "雍正", "乾隆", "嘉庆", "道光", "咸丰", "同治", "光绪", "宣统"]
MARKERS = ["明", "明代", "大明", "清", "清代", "大清", "明清"] + MING_REIGNS + QING_REIGNS

BROAD_KEYWORDS = [
    "明代瓷器", "清代瓷器", "明清瓷器", "明代官窑", "清代官窑", "明清官窑",
    "明代御窑", "清代御窑", "明清御窑", "明代青花", "清代青花", "明清青花",
    "明代釉里红", "清代釉里红", "明清釉里红", "明代斗彩", "清代斗彩",
    "明代五彩", "清代粉彩", "清代珐琅彩", "明清单色釉", "明清颜色釉",
]
GENERIC_TERMS = [
    "瓷器", "官窑", "御窑", "青花", "釉里红", "青花釉里红", "斗彩", "五彩",
    "粉彩", "珐琅彩", "单色釉", "颜色釉", "龙纹", "花卉纹", "缠枝莲",
    "盘", "碗", "瓶", "尊", "罐", "杯", "盏", "洗", "炉",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def clean(value):
    if value is None:
        return None
    value = re.sub(r"\s+", " ", str(value)).strip()
    return value or None


def build_keywords(extra=None):
    items = list(BROAD_KEYWORDS)
    for dynasty in ("明代", "清代", "明清"):
        for term in GENERIC_TERMS:
            items.append(f"{dynasty}{term}")
    for reign in MING_REIGNS + QING_REIGNS:
        for term in ("瓷器", "官窑", "青花", "盘", "碗", "瓶", "罐"):
            items.append(f"{reign}{term}")
    if extra:
        items.extend(extra)
    out = OrderedDict()
    for item in items:
        item = clean(item)
        if item:
            out[item] = None
    return list(out.keys())


def parse_price(text):
    text = clean(text)
    if not text:
        return None
    text = text.replace(",", "").replace("，", "").replace(" ", "")
    if "万" in text:
        try:
            return int(float(re.sub(r"[^\d.]", "", text)) * 10000)
        except Exception:
            return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


def extract_provenance(text):
    if not text:
        return None, False
    patterns = [
        r"来源[：:]\s*(.{4,120}?)(?:\n|$|；|;)",
        r"(.{2,40}(?:旧藏|收藏|珍藏|旧存|家族收藏|递藏))",
        r"(?:源自|曾藏于|购自)\s*(.{4,80}?)(?:\n|$|；|;)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            return clean(m.group(1)), True
    return None, False


def ensure_columns(conn, table, specs):
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, ddl in specs:
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
    conn.commit()


def init_db(conn):
    conn.execute("DROP INDEX IF EXISTS idx_detail_status")
    conn.execute("DROP INDEX IF EXISTS idx_image_status")
    conn.execute("DROP INDEX IF EXISTS idx_is_ming_qing")
    conn.commit()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS auction_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artron_id TEXT UNIQUE,
            name TEXT,
            dynasty TEXT,
            size TEXT,
            category TEXT,
            estimate_low INTEGER,
            estimate_high INTEGER,
            sold_price INTEGER,
            is_sold INTEGER DEFAULT 0,
            auction_house TEXT,
            auction_session TEXT,
            auction_name TEXT,
            auction_date TEXT,
            auction_city TEXT,
            lot_number TEXT,
            description TEXT,
            provenance TEXT,
            has_provenance INTEGER DEFAULT 0,
            image_url TEXT,
            image_urls TEXT,
            local_image_path TEXT,
            local_image_dir TEXT,
            image_downloaded_count INTEGER DEFAULT 0,
            source_url TEXT,
            keyword TEXT,
            is_ming_qing INTEGER,
            detail_status TEXT DEFAULT 'pending',
            image_status TEXT DEFAULT 'pending',
            indexed_at TEXT DEFAULT (datetime('now')),
            detail_updated_at TEXT,
            image_updated_at TEXT,
            last_error TEXT,
            raw_state_json TEXT,
            mark_text TEXT,
            vessel_type TEXT,
            glaze_color TEXT,
            motif TEXT,
            publication_info TEXT,
            exhibition_info TEXT,
            condition_info TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS record_keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artron_id TEXT NOT NULL,
            keyword TEXT NOT NULL,
            stage TEXT NOT NULL DEFAULT 'index',
            first_seen_at TEXT DEFAULT (datetime('now')),
            UNIQUE(artron_id, keyword, stage)
        );
        CREATE TABLE IF NOT EXISTS crawl_queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT NOT NULL,
            stage TEXT NOT NULL,
            last_page INTEGER DEFAULT 0,
            pages_crawled INTEGER DEFAULT 0,
            records_seen INTEGER DEFAULT 0,
            completed INTEGER DEFAULT 0,
            updated_at TEXT DEFAULT (datetime('now')),
            UNIQUE(keyword, stage)
        );
        """
    )
    ensure_columns(
        conn,
        "auction_records",
        [
            ("image_urls", "image_urls TEXT"),
            ("local_image_dir", "local_image_dir TEXT"),
            ("image_downloaded_count", "image_downloaded_count INTEGER DEFAULT 0"),
            ("is_ming_qing", "is_ming_qing INTEGER"),
            ("detail_status", "detail_status TEXT DEFAULT 'pending'"),
            ("image_status", "image_status TEXT DEFAULT 'pending'"),
            ("indexed_at", "indexed_at TEXT"),
            ("detail_updated_at", "detail_updated_at TEXT"),
            ("image_updated_at", "image_updated_at TEXT"),
            ("last_error", "last_error TEXT"),
            ("raw_state_json", "raw_state_json TEXT"),
            ("mark_text", "mark_text TEXT"),
            ("vessel_type", "vessel_type TEXT"),
            ("glaze_color", "glaze_color TEXT"),
            ("motif", "motif TEXT"),
            ("publication_info", "publication_info TEXT"),
            ("exhibition_info", "exhibition_info TEXT"),
            ("condition_info", "condition_info TEXT"),
        ],
    )
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_detail_status ON auction_records(detail_status);
        CREATE INDEX IF NOT EXISTS idx_image_status ON auction_records(image_status);
        CREATE INDEX IF NOT EXISTS idx_is_ming_qing ON auction_records(is_ming_qing);
        """
    )
    conn.commit()


def get_cookie():
    if os.path.exists(COOKIE_PATH):
        with open(COOKIE_PATH, "r", encoding="utf-8") as f:
            cookie = f.read().strip()
            if cookie:
                return cookie
    print("Login to https://artso.artron.net and paste the Cookie header.")
    cookie = input("Cookie: ").strip()
    with open(COOKIE_PATH, "w", encoding="utf-8") as f:
        f.write(cookie)
    return cookie


def create_session(cookie):
    session = requests.Session()
    session.headers.update(HEADERS)
    session.headers["Cookie"] = cookie
    return session


def warmup(session):
    for url in ("https://artso.artron.net", "https://artso.artron.net/auction/search_auction.php"):
        try:
            session.get(url, timeout=20)
            time.sleep(1)
        except Exception as exc:
            log.warning("warmup failed: %s", exc)


def parse_initial_state(html):
    m = re.search(r"window\.__INITIAL_STATE__\s*=\s*(\{.+?\});\s*\(function", html, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None


def extract_artron_id(url):
    m = re.search(r"paimai-art(\w+)", url)
    return m.group(1) if m else None


def update_query_progress(conn, keyword, page_num, records_seen, completed):
    conn.execute(
        """
        INSERT INTO crawl_queries (keyword, stage, last_page, pages_crawled, records_seen, completed, updated_at)
        VALUES (?, 'index', ?, 1, ?, ?, datetime('now'))
        ON CONFLICT(keyword, stage) DO UPDATE SET
            last_page=excluded.last_page,
            pages_crawled=crawl_queries.pages_crawled+1,
            records_seen=crawl_queries.records_seen+excluded.records_seen,
            completed=CASE WHEN excluded.completed=1 THEN 1 ELSE crawl_queries.completed END,
            updated_at=datetime('now')
        """,
        (keyword, page_num, records_seen, 1 if completed else 0),
    )
    conn.commit()


def get_query_progress(conn, keyword):
    row = conn.execute(
        "SELECT last_page, completed FROM crawl_queries WHERE keyword=? AND stage='index'",
        (keyword,),
    ).fetchone()
    if not row:
        return 0, 0
    return int(row[0] or 0), int(row[1] or 0)


def save_index_records(conn, urls, keyword):
    inserted = 0
    for url in urls:
        artron_id = extract_artron_id(url)
        if not artron_id:
            continue
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO auction_records
            (artron_id, source_url, keyword, is_ming_qing, detail_status, image_status, indexed_at)
            VALUES (?, ?, ?, 1, 'pending', 'pending', datetime('now'))
            """,
            (artron_id, url, keyword),
        )
        inserted += 1 if cur.rowcount else 0
        conn.execute(
            "UPDATE auction_records SET source_url=COALESCE(source_url, ?), keyword=COALESCE(keyword, ?), indexed_at=datetime('now') WHERE artron_id=?",
            (url, keyword, artron_id),
        )
        conn.execute(
            "INSERT OR IGNORE INTO record_keywords (artron_id, keyword, stage) VALUES (?, ?, 'index')",
            (artron_id, keyword),
        )
    conn.commit()
    return inserted


def search_lots(session, keyword, page_num):
    resp = session.get(
        "https://artso.artron.net/auction/search_auction.php",
        params={"keyword": keyword, "ptype": "陶瓷", "page": page_num},
        headers={"Referer": "https://artso.artron.net/"},
        timeout=20,
    )
    resp.raise_for_status()
    resp.encoding = "utf-8"
    html = resp.text
    urls = sorted(set(re.findall(r"https://auction\.artron\.net/paimai-art\w+", html)))
    has_next = ("下一页" in html) or (f"page={page_num + 1}" in html)
    return urls, has_next


def crawl_index(session, conn, keywords, max_pages, delay):
    for idx, keyword in enumerate(keywords, 1):
        log.info("index [%s/%s] %s", idx, len(keywords), keyword)
        last_page, completed = get_query_progress(conn, keyword)
        if completed:
            log.info("skip completed keyword: %s", keyword)
            continue

        start_page = last_page + 1 if last_page > 0 else 1
        if start_page > max_pages:
            log.info("keyword already reached current page limit: %s (last_page=%s)", keyword, last_page)
            continue

        for page_num in range(start_page, max_pages + 1):
            try:
                urls, has_next = search_lots(session, keyword, page_num)
            except Exception as exc:
                log.error("index failed [%s] page %s: %s", keyword, page_num, exc)
                update_query_progress(conn, keyword, page_num, 0, False)
                break
            if not urls:
                update_query_progress(conn, keyword, page_num, 0, True)
                break
            inserted = save_index_records(conn, urls, keyword)
            update_query_progress(conn, keyword, page_num, len(urls), not has_next)
            log.info("page %s seen=%s inserted=%s", page_num, len(urls), inserted)
            if not has_next:
                break
            time.sleep(delay)


def normalize_image_url(url):
    if not url:
        return None
    url = url.strip()
    if "src=" in url:
        parsed = urlparse(url)
        src = parse_qs(parsed.query).get("src")
        if src:
            url = unquote(src[0])
        else:
            m = re.search(r"src=(https?://[^&\s]+)", url)
            if m:
                url = unquote(m.group(1))
    if url.startswith("//"):
        url = "https:" + url
    if url.startswith("http://"):
        url = "https://" + url[7:]
    return url


def extract_image_list(detail):
    raw = []
    pic_array = detail.get("PicUrl", [])
    if isinstance(pic_array, list):
        for item in pic_array:
            if isinstance(item, str):
                raw.append(item)
            elif isinstance(item, dict):
                for key in ("src", "url", "bigPic", "picUrl"):
                    if item.get(key):
                        raw.append(item[key])
    for key in ("bigPic", "smallPic", "midPic"):
        if detail.get(key):
            raw.append(detail[key])
    out = OrderedDict()
    for item in raw:
        item = normalize_image_url(item)
        if item:
            out[item] = None
    return list(out.keys())


def compute_is_ming_qing(record):
    text = " ".join(
        filter(None, [clean(record.get(k)) for k in ("name", "dynasty", "category", "description", "auction_session", "auction_name")])
    )
    return 1 if text and any(token in text for token in MARKERS) else 0


def pick_first_term(text, terms):
    text = clean(text) or ""
    for term in terms:
        if term in text:
            return term
    return None


def extract_mark_text(text):
    text = clean(text) or ""
    patterns = [
        r"(?:款识|底款|题款)[：:\s]*([^；;\n]{2,80})",
        r"((?:大明|大清)[^款]{0,20}(?:款|年制))",
        r"((?:康熙|雍正|乾隆|嘉庆|道光|咸丰|同治|光绪|宣统|宣德|成化|嘉靖|万历)[^款]{0,20}(?:款|年制))",
        r"([青花矾红篆书楷书隶书行书双行六字四字]{0,20}(?:款|年制))",
    ]
    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            return clean(m.group(1))
    return None


def extract_section_text(text, labels):
    text = clean(text) or ""
    for label in labels:
        m = re.search(rf"{label}[：:\s]*([^；;\n]{{4,300}})", text)
        if m:
            return clean(m.group(1))
    return None


def extract_condition_text(text):
    text = clean(text) or ""
    signal_terms = ["品相", "瑕疵", "保存", "全品", "修复", "冲线", "磕碰", "飞皮"]
    patterns = [
        r"(?:品相|品相报告|保存状况|瑕疵|有无瑕疵)[：:\s]*([^。；;\n]{4,300})",
        r"((?:品相良好|品相完好|品相完美|品相如新|保存良好|无明显瑕疵|有轻微瑕疵)[^。；;\n]{0,120})",
    ]
    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            value = clean(m.group(1))
            if value:
                value = re.split(r"(?:来源|说明|【来源】|【说明】|备注)[:：\s]*", value)[0]
                value = re.sub(r"^[】\]\s:：]+", "", value)
            value = clean(value)
            if value and any(term in value for term in signal_terms):
                return value
    return None


def derive_detail_fields(record):
    name = clean(record.get("name")) or ""
    description = clean(record.get("description")) or ""
    combined = f"{name} {description}"

    record["vessel_type"] = pick_first_term(name, VESSEL_TYPES) or pick_first_term(description, VESSEL_TYPES)
    record["glaze_color"] = pick_first_term(combined, GLAZE_TERMS)
    record["motif"] = pick_first_term(combined, MOTIF_TERMS)
    record["mark_text"] = extract_mark_text(combined)
    record["publication_info"] = extract_section_text(description, ["出版", "著录", "出版著录", "文献著录"])
    record["exhibition_info"] = extract_section_text(description, ["展览", "展出"])
    record["condition_info"] = extract_condition_text(description)
    return record


def extract_record_from_state(state, url, keyword):
    try:
        page = state.get("pageProDetail", {})
        data = page.get("data", {})
        detail = data.get("detail", {})
        extra = detail.get("extraInfo", [])
        attr = detail.get("attribute", [])
        artron_id = (detail.get("artCode", "") or "").replace("art", "") or extract_artron_id(url)
        if not artron_id:
            return None
        record = {
            "artron_id": artron_id,
            "name": clean(detail.get("workName") or data.get("show_title", "")),
            "lot_number": clean(detail.get("tlNumber", "")),
            "category": clean(detail.get("classCodeTwoName", "")),
            "source_url": url,
            "keyword": keyword,
            "raw_state_json": json.dumps(state, ensure_ascii=False),
        }
        estimate_raw = None
        sold_raw = None
        for item in extra:
            label = clean(item.get("label"))
            text = clean(item.get("text"))
            full_text = clean(item.get("fullText")) or text
            if label == "创作年代":
                record["dynasty"] = text
            elif label == "尺寸":
                record["size"] = text
            elif label == "估价":
                estimate_raw = text
            elif label == "成交价":
                sold_raw = text
            elif label == "拍品描述":
                record["description"] = full_text
        parts = [p for p in re.split(r"[-—~至]", estimate_raw or "") if clean(p)]
        record["estimate_low"] = parse_price(parts[0]) if parts else None
        record["estimate_high"] = parse_price(parts[1]) if len(parts) > 1 else None
        result_price = clean(detail.get("picAttribute", {}).get("resultPrice", ""))
        record["sold_price"] = parse_price(result_price) or parse_price(sold_raw)
        record["is_sold"] = 1 if record["sold_price"] else 0
        for item in attr:
            label = clean(item.get("label"))
            text = clean(item.get("text"))
            if label == "拍卖公司":
                record["auction_house"] = text
            elif label == "拍卖会":
                record["auction_name"] = text
            elif label == "拍卖专场":
                record["auction_session"] = text
            elif label == "拍卖日期":
                record["auction_date"] = text
            elif label == "拍卖地点" and not record.get("auction_city"):
                record["auction_city"] = text.split("\n")[0].strip() if text else None
        images = extract_image_list(detail)
        record["image_url"] = images[0] if images else None
        record["image_urls"] = json.dumps(images, ensure_ascii=False)
        record["provenance"], has_provenance = extract_provenance(record.get("description"))
        record["has_provenance"] = 1 if has_provenance else 0
        record = derive_detail_fields(record)
        record["is_ming_qing"] = compute_is_ming_qing(record)
        return record
    except Exception as exc:
        log.error("extract record failed: %s", exc)
        return None


def scrape_detail(session, url, keyword):
    resp = session.get(url, headers={"Referer": "https://artso.artron.net/"}, timeout=20)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    state = parse_initial_state(resp.text)
    return extract_record_from_state(state, url, keyword) if state else None


DETAIL_RECORD_FIELDS = [
    "artron_id",
    "name",
    "dynasty",
    "size",
    "category",
    "estimate_low",
    "estimate_high",
    "sold_price",
    "is_sold",
    "auction_house",
    "auction_session",
    "auction_name",
    "auction_date",
    "auction_city",
    "lot_number",
    "description",
    "provenance",
    "has_provenance",
    "image_url",
    "image_urls",
    "source_url",
    "keyword",
    "is_ming_qing",
    "raw_state_json",
    "mark_text",
    "vessel_type",
    "glaze_color",
    "motif",
    "publication_info",
    "exhibition_info",
    "condition_info",
]


def normalize_detail_record(record):
    normalized = {field: None for field in DETAIL_RECORD_FIELDS}
    normalized.update(record)
    return normalized


def save_detail_record(conn, record):
    record = normalize_detail_record(record)
    conn.execute(
        """
        INSERT INTO auction_records (
            artron_id, name, dynasty, size, category, estimate_low, estimate_high, sold_price, is_sold,
            auction_house, auction_session, auction_name, auction_date, auction_city, lot_number,
            description, provenance, has_provenance, image_url, image_urls, source_url, keyword,
            is_ming_qing, raw_state_json, mark_text, vessel_type, glaze_color, motif,
            publication_info, exhibition_info, condition_info, detail_status, detail_updated_at, last_error
        ) VALUES (
            :artron_id, :name, :dynasty, :size, :category, :estimate_low, :estimate_high, :sold_price, :is_sold,
            :auction_house, :auction_session, :auction_name, :auction_date, :auction_city, :lot_number,
            :description, :provenance, :has_provenance, :image_url, :image_urls, :source_url, :keyword,
            :is_ming_qing, :raw_state_json, :mark_text, :vessel_type, :glaze_color, :motif,
            :publication_info, :exhibition_info, :condition_info, 'done', datetime('now'), NULL
        )
        ON CONFLICT(artron_id) DO UPDATE SET
            name=COALESCE(excluded.name, auction_records.name),
            dynasty=COALESCE(excluded.dynasty, auction_records.dynasty),
            size=COALESCE(excluded.size, auction_records.size),
            category=COALESCE(excluded.category, auction_records.category),
            estimate_low=COALESCE(excluded.estimate_low, auction_records.estimate_low),
            estimate_high=COALESCE(excluded.estimate_high, auction_records.estimate_high),
            sold_price=COALESCE(excluded.sold_price, auction_records.sold_price),
            is_sold=COALESCE(excluded.is_sold, auction_records.is_sold),
            auction_house=COALESCE(excluded.auction_house, auction_records.auction_house),
            auction_session=COALESCE(excluded.auction_session, auction_records.auction_session),
            auction_name=COALESCE(excluded.auction_name, auction_records.auction_name),
            auction_date=COALESCE(excluded.auction_date, auction_records.auction_date),
            auction_city=COALESCE(excluded.auction_city, auction_records.auction_city),
            lot_number=COALESCE(excluded.lot_number, auction_records.lot_number),
            description=COALESCE(excluded.description, auction_records.description),
            provenance=COALESCE(excluded.provenance, auction_records.provenance),
            has_provenance=MAX(auction_records.has_provenance, excluded.has_provenance),
            image_url=COALESCE(excluded.image_url, auction_records.image_url),
            image_urls=COALESCE(excluded.image_urls, auction_records.image_urls),
            source_url=COALESCE(excluded.source_url, auction_records.source_url),
            keyword=COALESCE(auction_records.keyword, excluded.keyword),
            is_ming_qing=COALESCE(excluded.is_ming_qing, auction_records.is_ming_qing),
            raw_state_json=COALESCE(excluded.raw_state_json, auction_records.raw_state_json),
            mark_text=COALESCE(excluded.mark_text, auction_records.mark_text),
            vessel_type=COALESCE(excluded.vessel_type, auction_records.vessel_type),
            glaze_color=COALESCE(excluded.glaze_color, auction_records.glaze_color),
            motif=COALESCE(excluded.motif, auction_records.motif),
            publication_info=COALESCE(excluded.publication_info, auction_records.publication_info),
            exhibition_info=COALESCE(excluded.exhibition_info, auction_records.exhibition_info),
            condition_info=COALESCE(excluded.condition_info, auction_records.condition_info),
            detail_status='done',
            detail_updated_at=datetime('now'),
            last_error=NULL
        """,
        record,
    )
    conn.execute("INSERT OR IGNORE INTO record_keywords (artron_id, keyword, stage) VALUES (?, ?, 'detail')", (record["artron_id"], record["keyword"]))
    conn.commit()


def mark_detail_error(conn, artron_id, message):
    conn.execute(
        "UPDATE auction_records SET detail_status='error', detail_updated_at=datetime('now'), last_error=? WHERE artron_id=?",
        (str(message)[:500], artron_id),
    )
    conn.commit()


def run_detail(conn, session, limit, delay, refresh_done=False):
    sql = """
        SELECT artron_id, source_url, COALESCE(keyword, '') FROM auction_records
        WHERE source_url IS NOT NULL
    """
    if not refresh_done:
        sql += " AND (detail_status IS NULL OR detail_status != 'done')"
    sql += " ORDER BY CASE WHEN detail_status='error' THEN 1 ELSE 0 END, indexed_at, id"
    if limit > 0:
        sql += f" LIMIT {int(limit)}"
    rows = conn.execute(sql).fetchall()
    log.info("detail queue=%s", len(rows))
    for idx, (artron_id, url, keyword) in enumerate(rows, 1):
        log.info("detail [%s/%s] %s", idx, len(rows), url)
        try:
            record = scrape_detail(session, url, keyword)
            if not record:
                raise ValueError("missing __INITIAL_STATE__")
            save_detail_record(conn, record)
        except Exception as exc:
            mark_detail_error(conn, artron_id, exc)
            log.error("detail failed %s: %s", artron_id, exc)
        time.sleep(delay)


def decode_image_urls(value):
    if not value:
        return []
    try:
        data = json.loads(value)
    except Exception:
        return []
    out = OrderedDict()
    if isinstance(data, list):
        for item in data:
            if isinstance(item, str):
                url = normalize_image_url(item)
                if url:
                    out[url] = None
    return list(out.keys())


def infer_ext(url):
    try:
        ext = urlparse(url).path.rsplit(".", 1)[-1].lower()
    except Exception:
        ext = "jpg"
    return ext if ext in {"jpg", "jpeg", "png", "webp"} else "jpg"


def download_file(session, url, path):
    resp = session.get(url, timeout=30)
    if resp.status_code != 200:
        return False
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(resp.content)
    return True


def download_images_for_record(session, row, download_all):
    urls = decode_image_urls(row["image_urls"])
    if row["image_url"]:
        first = normalize_image_url(row["image_url"])
        if first and first not in urls:
            urls.insert(0, first)
    if not urls:
        return None, None, 0
    if not download_all:
        urls = urls[:1]
    if len(urls) == 1:
        path = os.path.join(IMG_FOLDER, f'{row["artron_id"]}.{infer_ext(urls[0])}')
        ok = os.path.exists(path) or download_file(session, urls[0], path)
        return (path if ok else None), None, 1 if ok else 0
    folder = os.path.join(IMG_FOLDER, row["artron_id"])
    os.makedirs(folder, exist_ok=True)
    first_path = None
    count = 0
    for idx, url in enumerate(urls, 1):
        path = os.path.join(folder, f"{idx:02d}.{infer_ext(url)}")
        ok = os.path.exists(path) or download_file(session, url, path)
        if ok:
            count += 1
            first_path = first_path or path
    return first_path, folder, count


def update_image_status(conn, artron_id, local_path, local_dir, count, success, error=None):
    conn.execute(
        """
        UPDATE auction_records
        SET local_image_path=COALESCE(?, local_image_path),
            local_image_dir=COALESCE(?, local_image_dir),
            image_downloaded_count=?,
            image_status=?,
            image_updated_at=datetime('now'),
            last_error=?
        WHERE artron_id=?
        """,
        (local_path, local_dir, count, "done" if success else "error", None if success else str(error)[:500], artron_id),
    )
    conn.commit()


def run_images(conn, session, limit, delay, download_all):
    conn.row_factory = sqlite3.Row
    sql = """
        SELECT * FROM auction_records
        WHERE detail_status='done' AND (image_url IS NOT NULL OR image_urls IS NOT NULL)
          AND (image_status IS NULL OR image_status != 'done')
        ORDER BY CASE WHEN image_status='error' THEN 1 ELSE 0 END, detail_updated_at, id
    """
    if limit > 0:
        sql += f" LIMIT {int(limit)}"
    rows = conn.execute(sql).fetchall()
    conn.row_factory = None
    log.info("image queue=%s", len(rows))
    for idx, row in enumerate(rows, 1):
        log.info("images [%s/%s] %s", idx, len(rows), row["artron_id"])
        try:
            local_path, local_dir, count = download_images_for_record(session, row, download_all)
            if count <= 0:
                raise ValueError("no image downloaded")
            update_image_status(conn, row["artron_id"], local_path, local_dir, count, True)
        except Exception as exc:
            update_image_status(conn, row["artron_id"], None, None, int(row["image_downloaded_count"] or 0), False, exc)
            log.error("image failed %s: %s", row["artron_id"], exc)
        time.sleep(delay)


def print_summary(conn):
    stats = {
        "records": "SELECT COUNT(*) FROM auction_records",
        "ming_qing_records": "SELECT COUNT(*) FROM auction_records WHERE is_ming_qing=1",
        "detail_done": "SELECT COUNT(*) FROM auction_records WHERE detail_status='done'",
        "image_done": "SELECT COUNT(*) FROM auction_records WHERE image_status='done'",
        "index_keywords": "SELECT COUNT(*) FROM crawl_queries WHERE stage='index'",
    }
    print("\n" + "=" * 60)
    print("Crawl summary")
    for name, sql in stats.items():
        print(f"{name:18s}: {conn.execute(sql).fetchone()[0]}")
    print(f"database           : {DB_PATH}")
    print(f"image folder       : {IMG_FOLDER}")
    print("=" * 60)


def load_extra_keywords(path, extra):
    items = list(extra or [])
    if path:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = clean(line)
                if line and not line.startswith("#"):
                    items.append(line)
    return items


def parse_args():
    parser = argparse.ArgumentParser(description="Broad Ming/Qing porcelain indexer for Artron.")
    parser.add_argument("--mode", choices=["index", "detail", "images", "full"], default="index")
    parser.add_argument("--max-pages-per-keyword", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--detail-limit", type=int, default=0)
    parser.add_argument("--image-limit", type=int, default=0)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    parser.add_argument("--download-all-images", action="store_true")
    parser.add_argument("--refresh-detail-done", action="store_true")
    parser.add_argument("--keyword", action="append")
    parser.add_argument("--keywords-file")
    return parser.parse_args()


def main():
    args = parse_args()
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    session = create_session(get_cookie())
    warmup(session)
    keywords = build_keywords(load_extra_keywords(args.keywords_file, args.keyword))
    if args.mode in {"index", "full"}:
        crawl_index(session, conn, keywords, args.max_pages_per_keyword, args.delay)
    if args.mode in {"detail", "full"}:
        run_detail(conn, session, args.detail_limit, args.delay, args.refresh_detail_done)
    if args.mode in {"images", "full"}:
        run_images(conn, session, args.image_limit, args.delay, args.download_all_images)
    print_summary(conn)
    conn.close()


if __name__ == "__main__":
    main()
