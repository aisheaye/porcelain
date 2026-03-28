import json
import sqlite3
from datetime import datetime
from pathlib import Path

DB_PATH = "porcelain_auction.db"
OUTPUT_PATH = Path("generated") / "search_records_ready.js"


def split_tags(value):
    if not value:
        return []
    return [item for item in str(value).split("|") if item]


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT
            s.artron_id,
            s.search_title,
            s.raw_name,
            s.normalized_dynasty,
            s.normalized_auction_date,
            s.auction_year,
            s.lot_number,
            s.sold_price,
            s.vessel_type,
            s.glaze_color,
            s.motif,
            s.provenance_raw,
            s.provenance_tags,
            s.provenance_entities,
            s.condition_raw,
            s.condition_tags,
            s.lot_group_tag,
            s.piece_count,
            s.image_url,
            s.source_url,
            s.keyword,
            s.quality_score,
            a.auction_house,
            a.auction_session,
            a.auction_name,
            a.auction_city,
            a.description,
            a.size
        FROM search_records_ready AS s
        JOIN auction_records AS a ON a.artron_id = s.artron_id
        ORDER BY s.quality_score DESC, s.normalized_auction_date DESC, s.artron_id
        """
    ).fetchall()
    conn.close()

    records = []
    dynasty_counts = {}
    vessel_counts = {}
    glaze_counts = {}
    provenance_counts = {}
    condition_counts = {}
    lot_group_counts = {}

    for row in rows:
        item = {
            "artron_id": row["artron_id"],
            "search_title": row["search_title"],
            "raw_name": row["raw_name"],
            "normalized_dynasty": row["normalized_dynasty"],
            "normalized_auction_date": row["normalized_auction_date"],
            "auction_year": row["auction_year"],
            "lot_number": row["lot_number"],
            "sold_price": row["sold_price"],
            "vessel_type": row["vessel_type"],
            "glaze_color": split_tags(row["glaze_color"]),
            "motif": row["motif"],
            "provenance_raw": row["provenance_raw"],
            "provenance_tags": split_tags(row["provenance_tags"]),
            "provenance_entities": split_tags(row["provenance_entities"]),
            "condition_raw": row["condition_raw"],
            "condition_tags": split_tags(row["condition_tags"]),
            "lot_group_tag": row["lot_group_tag"],
            "piece_count": row["piece_count"],
            "image_url": row["image_url"],
            "source_url": row["source_url"],
            "keyword": row["keyword"],
            "quality_score": row["quality_score"],
            "auction_house": row["auction_house"],
            "auction_session": row["auction_session"],
            "auction_name": row["auction_name"],
            "auction_city": row["auction_city"],
            "description": row["description"],
            "size": row["size"],
        }
        records.append(item)

        if item["normalized_dynasty"]:
            dynasty_counts[item["normalized_dynasty"]] = dynasty_counts.get(item["normalized_dynasty"], 0) + 1
        if item["vessel_type"]:
            vessel_counts[item["vessel_type"]] = vessel_counts.get(item["vessel_type"], 0) + 1
        for tag in item["glaze_color"]:
            glaze_counts[tag] = glaze_counts.get(tag, 0) + 1
        for tag in item["provenance_tags"]:
            provenance_counts[tag] = provenance_counts.get(tag, 0) + 1
        for tag in item["condition_tags"]:
            condition_counts[tag] = condition_counts.get(tag, 0) + 1
        if item["lot_group_tag"]:
            lot_group_counts[item["lot_group_tag"]] = lot_group_counts.get(item["lot_group_tag"], 0) + 1

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "record_count": len(records),
        "stats": {
            "dynasties": dynasty_counts,
            "vessel_types": vessel_counts,
            "glaze_tags": glaze_counts,
            "provenance_tags": provenance_counts,
            "condition_tags": condition_counts,
            "lot_groups": lot_group_counts,
        },
        "records": records,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        "window.SEARCH_DATASET = " + json.dumps(payload, ensure_ascii=False, indent=2) + ";\n",
        encoding="utf-8",
    )

    print("=" * 60)
    print("Search dataset exported")
    print(f"database             : {DB_PATH}")
    print(f"output               : {OUTPUT_PATH}")
    print(f"record_count         : {len(records)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
