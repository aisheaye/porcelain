import argparse
import sqlite3

DB_PATH = "porcelain_auction.db"

SECTIONS = {
    "excluded": """
        SELECT artron_id, raw_name, exclusion_reason
        FROM search_records
        WHERE is_excluded = 1
        ORDER BY exclusion_reason, raw_name
        LIMIT ?
    """,
    "top_ready": """
        SELECT artron_id, search_title, normalized_dynasty, normalized_auction_date,
               vessel_type, glaze_color, motif, quality_score
        FROM search_records_ready
        ORDER BY quality_score DESC, normalized_auction_date DESC, artron_id
        LIMIT ?
    """,
    "missing_dynasty": """
        SELECT artron_id, search_title, normalized_dynasty, normalized_auction_date
        FROM search_records_ready
        WHERE normalized_dynasty IS NULL
        ORDER BY normalized_auction_date DESC, artron_id
        LIMIT ?
    """,
    "missing_features": """
        SELECT artron_id, search_title, vessel_type, glaze_color, motif
        FROM search_records_ready
        WHERE vessel_type IS NULL OR glaze_color IS NULL OR motif IS NULL
        ORDER BY quality_score DESC, normalized_auction_date DESC, artron_id
        LIMIT ?
    """,
    "low_quality": """
        SELECT artron_id, search_title, normalized_dynasty, normalized_auction_date,
               vessel_type, glaze_color, quality_score
        FROM search_records_ready
        ORDER BY quality_score ASC, normalized_auction_date DESC, artron_id
        LIMIT ?
    """,
    "provenance": """
        SELECT s.artron_id, s.search_title, a.description, s.provenance_raw, s.provenance_tags, s.provenance_entities
        FROM search_records_ready AS s
        JOIN auction_records AS a ON a.artron_id = s.artron_id
        WHERE provenance_raw IS NOT NULL
        ORDER BY s.quality_score DESC, s.normalized_auction_date DESC, s.artron_id
        LIMIT ?
    """,
    "condition": """
        SELECT s.artron_id, s.search_title, a.description, s.condition_raw, s.condition_tags
        FROM search_records_ready AS s
        JOIN auction_records AS a ON a.artron_id = s.artron_id
        WHERE s.condition_raw IS NOT NULL
        ORDER BY s.quality_score DESC, s.normalized_auction_date DESC, s.artron_id
        LIMIT ?
    """,
}

DEFAULT_SECTIONS = ["excluded", "top_ready", "missing_dynasty", "missing_features", "provenance", "condition"]


def parse_args():
    parser = argparse.ArgumentParser(description="Sample the search dataset for manual QA.")
    parser.add_argument("--db", default=DB_PATH, help="SQLite database path")
    parser.add_argument("--limit", type=int, default=10, help="Rows per section")
    parser.add_argument(
        "--section",
        action="append",
        choices=sorted(SECTIONS),
        help="Only print selected section(s). Can be used multiple times.",
    )
    return parser.parse_args()


def print_section(conn, name, sql, limit):
    rows = conn.execute(sql, (limit,)).fetchall()
    print("=" * 60)
    print(f"{name} ({len(rows)} rows)")
    print("-" * 60)
    if not rows:
        print("none")
        return
    for row in rows:
        print(tuple(row))


def main():
    args = parse_args()
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    sections = args.section or DEFAULT_SECTIONS
    print(f"database: {args.db}")
    print(f"limit   : {args.limit}")
    for name in sections:
        print_section(conn, name, SECTIONS[name], args.limit)
    print("=" * 60)
    conn.close()


if __name__ == "__main__":
    main()
