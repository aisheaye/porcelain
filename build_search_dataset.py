import re
import sqlite3
from typing import Iterable

DB_PATH = "porcelain_auction.db"

VESSEL_TYPES = [
    "盘",
    "碗",
    "瓶",
    "罐",
    "壶",
    "尊",
    "洗",
    "杯",
    "盏",
    "盒",
    "炉",
    "钵",
    "梅瓶",
    "天球瓶",
    "棒槌瓶",
    "笔洗",
    "琮式瓶",
    "盖罐",
]

GLAZE_TERMS = [
    "青花",
    "釉里红",
    "青花釉里红",
    "斗彩",
    "五彩",
    "粉彩",
    "珐琅彩",
    "单色釉",
    "颜色釉",
    "祭红",
    "霁红",
    "郎窑红",
    "天蓝釉",
    "青釉",
    "白釉",
    "黄釉",
    "豇豆红",
    "窑变釉",
]

MOTIF_TERMS = [
    "龙纹",
    "凤纹",
    "缠枝莲",
    "花卉纹",
    "八吉祥",
    "开光",
    "婴戏",
    "山水",
    "人物",
    "折枝花",
    "莲纹",
    "云龙",
    "海水龙纹",
]

MING_REIGNS = ["洪武", "永乐", "宣德", "成化", "弘治", "正德", "嘉靖", "隆庆", "万历", "天启", "崇祯"]
QING_REIGNS = ["顺治", "康熙", "雍正", "乾隆", "嘉庆", "道光", "咸丰", "同治", "光绪", "宣统"]

REFERENCE_KEYWORDS = [
    "图录",
    "拍卖图录",
    "瓷器史",
    "瓷器鉴定",
    "鉴定",
    "中国陶瓷",
    "陶瓷史",
    "全集",
    "文集",
    "原版",
    "精装",
    "毛装",
    "全一册",
    "全1册",
    "一册",
    "册",
    "著",
    "编著",
    "出版社",
    "出版",
    "参考数据",
    "英文",
    "线装",
]

NON_PORCELAIN_KEYWORDS = [
    "书法",
    "立轴",
    "镜心",
    "纸本",
    "绢本",
    "手卷",
    "册页",
    "油画",
    "版画",
    "信札",
]

CERAMIC_KEYWORDS = [
    "瓷",
    "瓷器",
    "窑",
    "釉",
    "盘",
    "碗",
    "瓶",
    "罐",
    "尊",
    "洗",
    "杯",
    "盏",
]

PROVENANCE_TAG_RULES = [
    ("museum_collection", ["博物馆收藏", "博物馆旧藏", "馆藏"]),
    ("family_collection", ["家族旧藏", "家族珍藏"]),
    ("private_collection", ["私人收藏", "私人旧藏", "藏家旧藏", "旧藏"]),
    ("estate_collection", ["遗产", "遗藏"]),
    ("important_collector", ["玫茵堂", "戴维德", "E&J FRANKEL", "Frankel", "Bluett", "J.J. Lally", "Eskenazi"]),
    ("auction_history", ["佳士得", "苏富比", "嘉德", "保利", "西泠", "匡时"]),
    ("published", ["著录", "出版", "图录"]),
    ("exhibited", ["展览", "展出"]),
]

PROVENANCE_ENTITY_PATTERNS = [
    r"([A-Z][A-Za-z&.\- ]{2,40}(?:Collection|collection|Family|family|Foundation|Gallery)?)",
    r"([\u4e00-\u9fff]{2,20}(?:旧藏|收藏|家族|博物馆|文物公司|基金会))",
]

PROVENANCE_SIGNAL_KEYWORDS = [
    "来源",
    "旧藏",
    "收藏",
    "递藏",
    "珍藏",
    "藏家",
    "博物馆",
    "文物公司",
    "佳士得",
    "苏富比",
    "嘉德",
    "保利",
]

PROVENANCE_NOISE_PATTERNS = [
    r"极具陈设与收藏",
    r"因此在收藏",
    r"有很高的收藏",
    r"颇为精致",
]


def clean_text(value):
    if value is None:
        return None
    value = re.sub(r"\s+", " ", str(value)).strip()
    return value or None


def contains_any(text: str | None, keywords: Iterable[str]) -> bool:
    if not text:
        return False
    return any(keyword in text for keyword in keywords)


def normalize_auction_date(value):
    value = clean_text(value)
    if not value:
        return None
    match = re.search(r"(\d{4}-\d{2}-\d{2})", value)
    return match.group(1) if match else None


def normalize_dynasty(value, fallback_text):
    value = clean_text(value)
    if value:
        if "民国" in value:
            return "民国"
        if "明" in value:
            return "明代"
        if "清" in value:
            return "清代"
        return value
    text = fallback_text or ""
    if "明代" in text or "大明" in text or any(reign in text for reign in MING_REIGNS):
        return "明代"
    if "清代" in text or "大清" in text or any(reign in text for reign in QING_REIGNS):
        return "清代"
    return None


def first_match(texts, terms):
    for text in texts:
        text = text or ""
        for term in terms:
            if term in text:
                return term
    return None


def classify_record(name, description):
    if contains_any(name, NON_PORCELAIN_KEYWORDS) and not contains_any(name, CERAMIC_KEYWORDS):
        return 1, "non_porcelain_art"
    if contains_any(name, REFERENCE_KEYWORDS):
        return 1, "reference_material"
    if "《" in (name or "") and contains_any(name, REFERENCE_KEYWORDS):
        return 1, "reference_material"
    if re.search(r"[上中下]册|全\d+册|套装|全套", name or ""):
        return 1, "reference_material"
    if not contains_any(name, CERAMIC_KEYWORDS) and contains_any(description, REFERENCE_KEYWORDS):
        return 1, "reference_material"
    return 0, None


def normalize_provenance(value, description):
    text = clean_text(value) or ""
    if not text and description:
        match = re.search(r"(?:来源|旧藏|收藏|递藏|备注)[：:\s]*([^。；;\n]{4,200})", description)
        text = clean_text(match.group(1)) if match else None
    text = clean_text(text)
    if not text:
        return None
    if any(re.search(pattern, text) for pattern in PROVENANCE_NOISE_PATTERNS):
        return None
    if not contains_any(text, PROVENANCE_SIGNAL_KEYWORDS):
        return None
    return text


def derive_provenance_tags(text):
    if not text:
        return None
    tags = []
    for tag, keywords in PROVENANCE_TAG_RULES:
        if contains_any(text, keywords):
            tags.append(tag)
    return "|".join(dict.fromkeys(tags)) or None


def derive_provenance_entities(text):
    if not text:
        return None
    found = []
    for pattern in PROVENANCE_ENTITY_PATTERNS:
        for match in re.findall(pattern, text):
            item = clean_text(match)
            if not item:
                continue
            if len(item) < 3:
                continue
            if not contains_any(item, PROVENANCE_SIGNAL_KEYWORDS) and not re.search(r"[A-Z]", item):
                continue
            found.append(item)
    deduped = list(dict.fromkeys(found))
    return "|".join(deduped[:8]) or None


def quality_score(row, normalized_date, normalized_dynasty, vessel_type, glaze_color, motif):
    score = 0
    if row["name"]:
        score += 3
    if normalized_dynasty:
        score += 2
    if normalized_date:
        score += 1
    if row["image_url"]:
        score += 2
    if row["sold_price"] is not None:
        score += 1
    if row["provenance"]:
        score += 2
    description = row["description"] or ""
    if len(description) >= 20:
        score += 1
    if vessel_type:
        score += 1
    if glaze_color:
        score += 1
    if motif:
        score += 1
    return score


def init_search_table(conn):
    conn.executescript(
        """
        DROP VIEW IF EXISTS search_records_ready;
        DROP TABLE IF EXISTS search_records;

        CREATE TABLE search_records (
            artron_id TEXT PRIMARY KEY,
            raw_name TEXT,
            search_title TEXT,
            normalized_dynasty TEXT,
            normalized_auction_date TEXT,
            auction_year INTEGER,
            lot_number TEXT,
            sold_price INTEGER,
            vessel_type TEXT,
            glaze_color TEXT,
            motif TEXT,
            provenance_raw TEXT,
            provenance_tags TEXT,
            provenance_entities TEXT,
            image_url TEXT,
            source_url TEXT,
            keyword TEXT,
            quality_score INTEGER NOT NULL DEFAULT 0,
            is_excluded INTEGER NOT NULL DEFAULT 0,
            exclusion_reason TEXT,
            built_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX idx_search_records_excluded ON search_records(is_excluded);
        CREATE INDEX idx_search_records_dynasty ON search_records(normalized_dynasty);
        CREATE INDEX idx_search_records_date ON search_records(normalized_auction_date);
        CREATE INDEX idx_search_records_price ON search_records(sold_price);
        CREATE INDEX idx_search_records_prov_tags ON search_records(provenance_tags);

        CREATE VIEW search_records_ready AS
        SELECT *
        FROM search_records
        WHERE is_excluded = 0;
        """
    )
    conn.commit()


def rebuild_search_dataset(conn):
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT artron_id, name, dynasty, description, provenance, vessel_type, glaze_color, motif,
               auction_date, lot_number, sold_price, image_url, source_url, keyword
        FROM auction_records
        WHERE detail_status='done'
        """
    ).fetchall()
    conn.row_factory = None

    payload = []
    for row in rows:
        search_title = clean_text(row["name"])
        normalized_date = normalize_auction_date(row["auction_date"])
        normalized_dynasty = normalize_dynasty(row["dynasty"], f"{row['name'] or ''} {row['description'] or ''}")

        fallback_text = " ".join(part for part in [row["name"], row["description"]] if part)
        lookup_texts = [row["name"], row["description"]]
        vessel_type = clean_text(row["vessel_type"]) or first_match(lookup_texts, VESSEL_TYPES)
        glaze_color = clean_text(row["glaze_color"]) or first_match(lookup_texts, GLAZE_TERMS)
        motif = clean_text(row["motif"]) or first_match(lookup_texts, MOTIF_TERMS)
        provenance_raw = normalize_provenance(row["provenance"], row["description"])
        provenance_tags = derive_provenance_tags(provenance_raw)
        provenance_entities = derive_provenance_entities(provenance_raw)

        is_excluded, exclusion_reason = classify_record(row["name"], row["description"])
        score = quality_score(row, normalized_date, normalized_dynasty, vessel_type, glaze_color, motif)
        auction_year = int(normalized_date[:4]) if normalized_date else None

        payload.append(
            (
                row["artron_id"],
                row["name"],
                search_title,
                normalized_dynasty,
                normalized_date,
                auction_year,
                clean_text(row["lot_number"]),
                row["sold_price"],
                vessel_type,
                glaze_color,
                motif,
                provenance_raw,
                provenance_tags,
                provenance_entities,
                clean_text(row["image_url"]),
                clean_text(row["source_url"]),
                clean_text(row["keyword"]),
                score,
                is_excluded,
                exclusion_reason,
            )
        )

    conn.executemany(
        """
        INSERT INTO search_records (
            artron_id, raw_name, search_title, normalized_dynasty, normalized_auction_date,
            auction_year, lot_number, sold_price, vessel_type, glaze_color, motif,
            provenance_raw, provenance_tags, provenance_entities,
            image_url, source_url, keyword, quality_score, is_excluded, exclusion_reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    conn.commit()

    stats = {
        "search_records": conn.execute("SELECT COUNT(*) FROM search_records").fetchone()[0],
        "search_records_ready": conn.execute("SELECT COUNT(*) FROM search_records_ready").fetchone()[0],
        "excluded_reference": conn.execute("SELECT COUNT(*) FROM search_records WHERE exclusion_reason='reference_material'").fetchone()[0],
        "excluded_non_porcelain": conn.execute("SELECT COUNT(*) FROM search_records WHERE exclusion_reason='non_porcelain_art'").fetchone()[0],
        "with_dynasty": conn.execute("SELECT COUNT(*) FROM search_records_ready WHERE normalized_dynasty IS NOT NULL").fetchone()[0],
        "with_date": conn.execute("SELECT COUNT(*) FROM search_records_ready WHERE normalized_auction_date IS NOT NULL").fetchone()[0],
        "with_image": conn.execute("SELECT COUNT(*) FROM search_records_ready WHERE image_url IS NOT NULL").fetchone()[0],
        "with_provenance": conn.execute("SELECT COUNT(*) FROM search_records_ready WHERE provenance_raw IS NOT NULL").fetchone()[0],
        "with_provenance_tags": conn.execute("SELECT COUNT(*) FROM search_records_ready WHERE provenance_tags IS NOT NULL").fetchone()[0],
    }
    return stats


def main():
    conn = sqlite3.connect(DB_PATH)
    init_search_table(conn)
    stats = rebuild_search_dataset(conn)

    print("=" * 60)
    print("Search dataset rebuilt")
    print(f"database             : {DB_PATH}")
    for key, value in stats.items():
        print(f"{key:20s}: {value}")
    print("table                : search_records")
    print("view                 : search_records_ready")
    print("=" * 60)
    conn.close()


if __name__ == "__main__":
    main()
