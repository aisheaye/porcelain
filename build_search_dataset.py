import re
import sqlite3
from typing import Iterable

DB_PATH = "porcelain_auction.db"

REFERENCE_KEYWORDS = [
    "图录",
    "拍卖图录",
    "瓷器志",
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
    "共二册",
    "一册",
    "册",
    "辑",
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

CERAMIC_KEYWORDS = ["瓷", "瓷器", "窑", "釉", "盘", "碗", "瓶", "罐", "壶", "洗", "杯", "尊"]

REIGN_TO_DYNASTY = {
    "洪武": "明代",
    "永乐": "明代",
    "宣德": "明代",
    "正统": "明代",
    "景泰": "明代",
    "天顺": "明代",
    "成化": "明代",
    "弘治": "明代",
    "正德": "明代",
    "嘉靖": "明代",
    "隆庆": "明代",
    "万历": "明代",
    "泰昌": "明代",
    "天启": "明代",
    "崇祯": "明代",
    "顺治": "清代",
    "康熙": "清代",
    "雍正": "清代",
    "乾隆": "清代",
    "嘉庆": "清代",
    "道光": "清代",
    "咸丰": "清代",
    "同治": "清代",
    "光绪": "清代",
    "宣统": "清代",
    "洪宪": "民国",
}

REIGN_TERMS = list(REIGN_TO_DYNASTY)

KILN_TERMS = [
    "景德镇窑",
    "德化窑",
    "龙泉窑",
    "宜兴窑",
    "磁州窑",
    "耀州窑",
    "定窑",
    "汝窑",
    "官窑",
    "哥窑",
    "钧窑",
]

BASE_GLAZE_TERMS = [
    "白釉",
    "青釉",
    "蓝釉",
    "红釉",
    "绿釉",
    "黄釉",
    "黑釉",
    "紫釉",
    "单色釉",
    "颜色釉",
]

BASE_GLAZE_SUBTYPE_TERMS = [
    "甜白",
    "卵白釉",
    "牙白",
    "粉青",
    "冬青",
    "梅子青",
    "豆青",
    "龙泉青釉",
    "娇黄",
    "柠檬黄",
    "明黄",
    "天蓝釉",
    "霁蓝",
    "洒蓝",
    "孔雀蓝",
    "祭红",
    "霁红",
    "郎窑红",
    "豇豆红",
    "宝石红",
    "鲜红",
    "孔雀绿",
    "翠绿",
    "乌金釉",
    "兔毫",
    "油滴",
    "紫金釉",
    "茄皮紫",
    "窑变釉",
    "结晶釉",
    "炉钧釉",
    "仿官釉",
    "仿哥釉",
    "茶叶末釉",
]

PAINTED_DECORATION_TERMS = [
    "青花釉里红",
    "釉里红",
    "青花",
    "斗彩",
    "五彩",
    "粉彩",
    "珐琅彩",
    "素三彩",
    "三彩",
    "描金",
    "金彩",
]

NON_PAINTED_DECORATION_TERMS = [
    "刻划花",
    "刻花",
    "划花",
    "印花",
    "堆塑",
    "镂空",
    "开光",
]

MOTIF_TERMS = [
    "龙纹",
    "凤纹",
    "缠枝莲",
    "花卉纹",
    "八吉祥",
    "婴戏",
    "山水",
    "人物",
    "折枝花",
    "莲纹",
    "云龙",
    "海水龙纹",
]

VESSEL_CATEGORY_TERMS = [
    "盘",
    "碗",
    "瓶",
    "罐",
    "壶",
    "尊",
    "杯",
    "盏",
    "洗",
    "盒",
    "炉",
    "钵",
]

VESSEL_TYPE_TERMS = [
    "折沿盘",
    "花口盘",
    "菱口盘",
    "圆盘",
    "撇口碗",
    "折腰碗",
    "深腹碗",
    "浅腹碗",
    "梅瓶",
    "玉壶春瓶",
    "天球瓶",
    "棒槌瓶",
    "蒜头瓶",
    "盘口瓶",
    "琮式瓶",
    "贯耳瓶",
    "长颈瓶",
    "双耳瓶",
    "抱月瓶",
    "盖罐",
    "将军罐",
    "直口罐",
    "鼓腹罐",
    "执壶",
    "提梁壶",
    "鼻烟壶",
    "茶壶",
    "觚式尊",
    "仿古尊",
    "高足杯",
    "压手杯",
    "爵杯",
    "酒杯",
    "茶盏",
    "笔洗",
    "圆洗",
]

PROVENANCE_TAG_RULES = [
    ("important_collector", ["玫茵堂", "戴维德", "E&J FRANKEL", "Frankel", "Bluett", "J.J. Lally", "Eskenazi"]),
    ("institution_stock", ["文物公司旧藏", "文物公司库出", "文物商店库出", "文物商店旧藏", "公司旧藏", "公司库出", "库出"]),
    ("museum_collection", ["博物馆收藏", "博物馆旧藏", "馆藏"]),
    ("auction_history", ["佳士得", "苏富比", "嘉德", "保利", "西泠", "匡时", "邦瀚斯", "富艺斯"]),
    ("household_release", ["私人收藏", "私人旧藏", "家族旧藏", "家族珍藏", "藏家旧藏", "旧藏"]),
]

PROVENANCE_ENTITY_PATTERNS = [
    r"([A-Z][A-Za-z&.\- ]{2,40}(?:Collection|collection|Family|family|Foundation|Gallery)?)",
    r"([\u4e00-\u9fff]{2,24}(?:旧藏|收藏|家族|博物馆|文物公司|文物商店))",
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
    "文物商店",
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

GENERIC_PROVENANCE_WORDS = [
    "私人",
    "家族",
    "华侨",
    "香港",
    "东南亚",
    "海外",
    "欧洲",
    "日本",
    "藏家",
    "旧藏",
    "收藏",
    "珍藏",
]

CONDITION_TAXONOMY = [
    ("全品 / 品相良好", 1, ["品相完美", "品相如新", "保存极佳", "全品", "完好如初", "品相良好", "保存良好", "品相完好", "品相较好"]),
    ("飞皮 / 缺口 / 磕口", 2, ["飞皮", "缺口", "磕口", "崩", "小缺", "磕"]),
    ("冲线 / 裂", 3, ["冲线", "裂", "鸡爪纹", "惊釉"]),
    ("修复 / 补配 / 粘修", 4, ["修复", "补配", "后配", "粘修", "冲线修", "复烧"]),
    ("磨损 / 失釉", 5, ["磨损", "失釉", "失彩", "磨痕", "轻微磨损"]),
]

LOT_GROUP_PATTERNS = [
    ("pair", 2, [r"一对", r"成对", r"对瓶", r"对杯", r"对碗", r"对盘", r"对罐", r"对洗", r"对炉"]),
    ("two_items", 2, [r"两件", r"二件", r"2件", r"二只", r"两只", r"二个", r"两个"]),
    ("set", None, [r"一套", r"成套", r"套组", r"组器", r"套"]),
]

CHINESE_NUMERALS = {
    "一": 1,
    "二": 2,
    "两": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "十": 10,
}


def clean_text(value):
    if value is None:
        return None
    value = re.sub(r"\s+", " ", str(value)).strip()
    return value or None


def ensure_column(conn, table, name, ddl):
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if name not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
        conn.commit()


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
    if "明代" in text or "大明" in text or any(reign in text for reign in REIGN_TERMS[:15]):
        return "明代"
    if "清代" in text or "大清" in text or any(reign in text for reign in REIGN_TERMS[15:]):
        return "清代"
    if "民国" in text or "洪宪" in text:
        return "民国"
    return None


def first_match(texts, terms):
    for text in texts:
        text = text or ""
        for term in terms:
            if term in text:
                return term
    return None


def collect_matches(text, terms):
    if not text:
        return []
    return [term for term in terms if term in text]


def join_tags(values):
    deduped = [item for item in dict.fromkeys(values) if item]
    return "|".join(deduped) or None


def classify_record(name, description):
    if contains_any(name, NON_PORCELAIN_KEYWORDS) and not contains_any(name, CERAMIC_KEYWORDS):
        return 1, "non_porcelain_art"
    if contains_any(name, REFERENCE_KEYWORDS):
        return 1, "reference_material"
    if "《" in (name or "") and contains_any(name, REFERENCE_KEYWORDS):
        return 1, "reference_material"
    if re.search(r"[上中下]册|\d+册|套装|全套", name or ""):
        return 1, "reference_material"
    if not contains_any(name, CERAMIC_KEYWORDS) and contains_any(description, REFERENCE_KEYWORDS):
        return 1, "reference_material"
    return 0, None


def derive_reign_period(texts):
    return first_match(texts, REIGN_TERMS)


def derive_taxonomy_value(texts, terms):
    return first_match(texts, terms)


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


def derive_provenance_entities(text):
    if not text:
        return None
    found = []
    for pattern in PROVENANCE_ENTITY_PATTERNS:
        for match in re.findall(pattern, text):
            item = clean_text(match)
            if not item or len(item) < 3:
                continue
            found.append(item)
    deduped = list(dict.fromkeys(found))
    return join_tags(deduped[:8])


def is_generic_household_release(text, entities):
    if not text:
        return False
    if entities:
        return False
    if contains_any(text, ["博物馆", "文物公司", "文物商店", "基金会", "佳士得", "苏富比", "嘉德", "保利"]):
        return False
    stripped = text
    for word in GENERIC_PROVENANCE_WORDS:
        stripped = stripped.replace(word, "")
    stripped = re.sub(r"[，,、；;:：\s]", "", stripped)
    return len(stripped) <= 4


def derive_provenance_category(text, entities):
    if not text:
        return None
    matches = []
    for tag, keywords in PROVENANCE_TAG_RULES:
        if contains_any(text, keywords):
            matches.append(tag)
    if not matches:
        return None
    if "important_collector" in matches:
        return "important_collector"
    if "institution_stock" in matches:
        return "institution_stock"
    if "museum_collection" in matches:
        return "museum_collection"
    if "auction_history" in matches:
        return "auction_history"
    if "household_release" in matches and is_generic_household_release(text, entities):
        return "household_release"
    return None


def normalize_condition(value, description):
    signal_terms = ["品相", "瑕疵", "保存", "全品", "修复", "冲线", "磕碰", "飞皮", "失釉"]
    text = clean_text(value) or ""
    if not text and description:
        match = re.search(r"(?:品相|品相报告|保存状况|瑕疵|有无瑕疵)[：:\s]*([^。；;\n]{4,200})", description)
        text = clean_text(match.group(1)) if match else None
    if text:
        text = re.split(r"(?:来源|说明|【来源】|【说明】|备注)[：:\s]*", text)[0]
        text = re.sub(r"^[【\[]?\s*[:：\]]+", "", text)
    text = clean_text(text)
    if text and any(term in text for term in signal_terms):
        return text
    return None


def derive_condition_rank(text):
    if not text:
        return None, None
    best_match = None
    for label, order, keywords in CONDITION_TAXONOMY:
        if contains_any(text, keywords):
            best_match = (label, order)
    return best_match or (None, None)


def derive_glaze_tags(raw_value, texts):
    tags = []
    for text in texts:
        tags.extend(collect_matches(text, PAINTED_DECORATION_TERMS))
    if not tags and raw_value:
        tags.extend(collect_matches(raw_value, PAINTED_DECORATION_TERMS))
        if not tags:
            fallback = clean_text(raw_value)
            return fallback
    return join_tags(tags)


def derive_size_fields(size_text):
    text = clean_text(size_text)
    if not text:
        return None, None, None, None, None
    normalized = text.replace("厘米", "cm").replace("公分", "cm").replace("ＣＭ", "cm").replace("CM", "cm")
    height_cm = None
    diameter_cm = None
    aperture_cm = None

    height_match = re.search(r"(?:通高|高)\s*([0-9]+(?:\.[0-9]+)?)\s*cm", normalized, re.IGNORECASE)
    if height_match:
        height_cm = float(height_match.group(1))

    diameter_match = re.search(r"(?:直径|直徑|腹径|底径|最大直径)\s*([0-9]+(?:\.[0-9]+)?)\s*cm", normalized, re.IGNORECASE)
    if diameter_match:
        diameter_cm = float(diameter_match.group(1))

    aperture_match = re.search(r"(?:口径|口徑)\s*([0-9]+(?:\.[0-9]+)?)\s*cm", normalized, re.IGNORECASE)
    if aperture_match:
        aperture_cm = float(aperture_match.group(1))

    if diameter_cm is None and aperture_cm is not None:
        diameter_cm = aperture_cm

    matched_fragments = re.findall(r"(?:通高|高|直径|直徑|口径|口徑|腹径|底径|最大直径)\s*[0-9]+(?:\.[0-9]+)?\s*cm", normalized, re.IGNORECASE)
    other_size_notes = clean_text(re.sub(r"[，,；; ]+", " ", normalized))
    for fragment in matched_fragments:
        other_size_notes = other_size_notes.replace(fragment, "").strip()
    other_size_notes = clean_text(re.sub(r"^[，,；; ]+|[，,；; ]+$", "", other_size_notes or ""))
    return text, height_cm, diameter_cm, aperture_cm, other_size_notes


def parse_piece_count(raw):
    if not raw:
        return None
    raw = clean_text(raw) or ""
    if raw.isdigit():
        return int(raw)
    if len(raw) == 1:
        return CHINESE_NUMERALS.get(raw)
    if raw == "十":
        return 10
    return None


def derive_lot_group(name, description):
    text = " ".join(part for part in [name, description] if part)
    for tag, piece_count, patterns in LOT_GROUP_PATTERNS:
        if any(re.search(pattern, text) for pattern in patterns):
            return tag, piece_count
    match = re.search(r"([一二两三四五六七八九十\d]+)件", text)
    if match:
        piece_count = parse_piece_count(match.group(1))
        if piece_count == 2:
            return "two_items", 2
        return "multi_piece", piece_count
    match = re.search(r"([一二两三四五六七八九十\d]+)只", text)
    if match:
        piece_count = parse_piece_count(match.group(1))
        if piece_count == 2:
            return "two_items", 2
        return "multi_piece", piece_count
    return None, None


def quality_score(row, normalized_date, normalized_dynasty, vessel_category, vessel_type, painted_decoration, motif, provenance_category, condition_rank):
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
    if provenance_category:
        score += 2
    if condition_rank:
        score += 1
    description = row["description"] or ""
    if len(description) >= 20:
        score += 1
    if vessel_category:
        score += 1
    if vessel_type:
        score += 1
    if painted_decoration:
        score += 1
    if motif:
        score += 1
    return score


def init_search_table(conn):
    ensure_column(conn, "auction_records", "condition_info", "condition_info TEXT")
    conn.executescript(
        """
        DROP VIEW IF EXISTS search_records_ready;
        DROP TABLE IF EXISTS search_records;

        CREATE TABLE search_records (
            artron_id TEXT PRIMARY KEY,
            raw_name TEXT,
            search_title TEXT,
            normalized_dynasty TEXT,
            reign_period TEXT,
            normalized_auction_date TEXT,
            auction_year INTEGER,
            lot_number TEXT,
            sold_price INTEGER,
            size_raw TEXT,
            height_cm REAL,
            diameter_cm REAL,
            aperture_cm REAL,
            other_size_notes TEXT,
            kiln TEXT,
            base_glaze TEXT,
            base_glaze_subtype TEXT,
            painted_decoration TEXT,
            non_painted_decoration TEXT,
            vessel_category TEXT,
            vessel_type TEXT,
            motif TEXT,
            provenance_raw TEXT,
            provenance_category TEXT,
            provenance_tags TEXT,
            provenance_entities TEXT,
            condition_raw TEXT,
            condition_rank TEXT,
            condition_rank_order INTEGER,
            condition_tags TEXT,
            lot_group_tag TEXT,
            piece_count INTEGER,
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
        CREATE INDEX idx_search_records_reign ON search_records(reign_period);
        CREATE INDEX idx_search_records_date ON search_records(normalized_auction_date);
        CREATE INDEX idx_search_records_price ON search_records(sold_price);
        CREATE INDEX idx_search_records_prov_category ON search_records(provenance_category);
        CREATE INDEX idx_search_records_condition_rank ON search_records(condition_rank_order);
        CREATE INDEX idx_search_records_lot_group ON search_records(lot_group_tag);

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
        SELECT artron_id, name, dynasty, size, description, provenance, condition_info, vessel_type, glaze_color, motif,
               auction_date, lot_number, sold_price, image_url, source_url, keyword
        FROM auction_records
        WHERE detail_status='done'
        """
    ).fetchall()
    conn.row_factory = None

    payload = []
    for row in rows:
        search_title = clean_text(row["name"])
        fallback_text = f"{row['name'] or ''} {row['description'] or ''} {row['dynasty'] or ''}"
        normalized_date = normalize_auction_date(row["auction_date"])
        normalized_dynasty = normalize_dynasty(row["dynasty"], fallback_text)
        reign_period = derive_reign_period([row["name"], row["dynasty"], row["description"]])
        size_raw, height_cm, diameter_cm, aperture_cm, other_size_notes = derive_size_fields(row["size"])

        lookup_texts = [row["name"], row["description"]]
        kiln = derive_taxonomy_value(lookup_texts, KILN_TERMS)
        base_glaze = derive_taxonomy_value(lookup_texts, BASE_GLAZE_TERMS)
        base_glaze_subtype = derive_taxonomy_value(lookup_texts, BASE_GLAZE_SUBTYPE_TERMS)
        painted_decoration = derive_glaze_tags(row["glaze_color"], lookup_texts)
        non_painted_decoration = derive_taxonomy_value(lookup_texts, NON_PAINTED_DECORATION_TERMS)
        vessel_category = derive_taxonomy_value(lookup_texts, VESSEL_CATEGORY_TERMS)
        vessel_type = clean_text(row["vessel_type"]) or derive_taxonomy_value(lookup_texts, VESSEL_TYPE_TERMS)
        motif = clean_text(row["motif"]) or derive_taxonomy_value(lookup_texts, MOTIF_TERMS)

        provenance_raw = normalize_provenance(row["provenance"], row["description"])
        provenance_entities = derive_provenance_entities(provenance_raw)
        provenance_category = derive_provenance_category(provenance_raw, provenance_entities)
        provenance_tags = provenance_category

        condition_raw = normalize_condition(row["condition_info"], row["description"])
        condition_rank, condition_rank_order = derive_condition_rank(condition_raw)
        condition_tags = condition_rank

        lot_group_tag, piece_count = derive_lot_group(row["name"], row["description"])
        is_excluded, exclusion_reason = classify_record(row["name"], row["description"])
        score = quality_score(
            row,
            normalized_date,
            normalized_dynasty,
            vessel_category,
            vessel_type,
            painted_decoration,
            motif,
            provenance_category,
            condition_rank,
        )
        auction_year = int(normalized_date[:4]) if normalized_date else None

        payload.append(
            (
                row["artron_id"],
                row["name"],
                search_title,
                normalized_dynasty,
                reign_period,
                normalized_date,
                auction_year,
                clean_text(row["lot_number"]),
                row["sold_price"],
                size_raw,
                height_cm,
                diameter_cm,
                aperture_cm,
                other_size_notes,
                kiln,
                base_glaze,
                base_glaze_subtype,
                painted_decoration,
                non_painted_decoration,
                vessel_category,
                vessel_type,
                motif,
                provenance_raw,
                provenance_category,
                provenance_tags,
                provenance_entities,
                condition_raw,
                condition_rank,
                condition_rank_order,
                condition_tags,
                lot_group_tag,
                piece_count,
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
            artron_id, raw_name, search_title, normalized_dynasty, reign_period, normalized_auction_date,
            auction_year, lot_number, sold_price, size_raw, height_cm, diameter_cm, aperture_cm, other_size_notes,
            kiln, base_glaze, base_glaze_subtype, painted_decoration, non_painted_decoration,
            vessel_category, vessel_type, motif,
            provenance_raw, provenance_category, provenance_tags, provenance_entities,
            condition_raw, condition_rank, condition_rank_order, condition_tags,
            lot_group_tag, piece_count, image_url, source_url, keyword, quality_score,
            is_excluded, exclusion_reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        "with_reign": conn.execute("SELECT COUNT(*) FROM search_records_ready WHERE reign_period IS NOT NULL").fetchone()[0],
        "with_kiln": conn.execute("SELECT COUNT(*) FROM search_records_ready WHERE kiln IS NOT NULL").fetchone()[0],
        "with_painted_decoration": conn.execute("SELECT COUNT(*) FROM search_records_ready WHERE painted_decoration IS NOT NULL").fetchone()[0],
        "with_vessel_category": conn.execute("SELECT COUNT(*) FROM search_records_ready WHERE vessel_category IS NOT NULL").fetchone()[0],
        "with_vessel_type": conn.execute("SELECT COUNT(*) FROM search_records_ready WHERE vessel_type IS NOT NULL").fetchone()[0],
        "with_provenance_category": conn.execute("SELECT COUNT(*) FROM search_records_ready WHERE provenance_category IS NOT NULL").fetchone()[0],
        "with_condition_rank": conn.execute("SELECT COUNT(*) FROM search_records_ready WHERE condition_rank IS NOT NULL").fetchone()[0],
        "with_size_dimensions": conn.execute("SELECT COUNT(*) FROM search_records_ready WHERE height_cm IS NOT NULL OR diameter_cm IS NOT NULL OR aperture_cm IS NOT NULL").fetchone()[0],
        "with_lot_group": conn.execute("SELECT COUNT(*) FROM search_records_ready WHERE lot_group_tag IS NOT NULL").fetchone()[0],
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
