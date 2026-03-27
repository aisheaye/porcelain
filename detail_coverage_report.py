import sqlite3

DB_PATH = "porcelain_auction.db"


def scalar(conn, sql):
    return conn.execute(sql).fetchone()[0]


def pct(part, total):
    if not total:
        return "0.0%"
    return f"{part * 100.0 / total:.1f}%"


def main():
    conn = sqlite3.connect(DB_PATH)

    total_records = scalar(conn, "SELECT COUNT(*) FROM auction_records")
    detail_done = scalar(conn, "SELECT COUNT(*) FROM auction_records WHERE detail_status='done'")
    ming_qing = scalar(conn, "SELECT COUNT(*) FROM auction_records WHERE is_ming_qing=1")

    fields = [
        ("raw_state_json", "SELECT COUNT(*) FROM auction_records WHERE detail_status='done' AND raw_state_json IS NOT NULL AND raw_state_json <> ''"),
        ("description", "SELECT COUNT(*) FROM auction_records WHERE detail_status='done' AND description IS NOT NULL AND description <> ''"),
        ("provenance", "SELECT COUNT(*) FROM auction_records WHERE detail_status='done' AND provenance IS NOT NULL AND provenance <> ''"),
        ("mark_text", "SELECT COUNT(*) FROM auction_records WHERE detail_status='done' AND mark_text IS NOT NULL AND mark_text <> ''"),
        ("vessel_type", "SELECT COUNT(*) FROM auction_records WHERE detail_status='done' AND vessel_type IS NOT NULL AND vessel_type <> ''"),
        ("glaze_color", "SELECT COUNT(*) FROM auction_records WHERE detail_status='done' AND glaze_color IS NOT NULL AND glaze_color <> ''"),
        ("motif", "SELECT COUNT(*) FROM auction_records WHERE detail_status='done' AND motif IS NOT NULL AND motif <> ''"),
        ("publication_info", "SELECT COUNT(*) FROM auction_records WHERE detail_status='done' AND publication_info IS NOT NULL AND publication_info <> ''"),
        ("exhibition_info", "SELECT COUNT(*) FROM auction_records WHERE detail_status='done' AND exhibition_info IS NOT NULL AND exhibition_info <> ''"),
        ("image_urls", "SELECT COUNT(*) FROM auction_records WHERE detail_status='done' AND image_urls IS NOT NULL AND image_urls <> ''"),
        ("sold_price", "SELECT COUNT(*) FROM auction_records WHERE detail_status='done' AND sold_price IS NOT NULL"),
        ("auction_date", "SELECT COUNT(*) FROM auction_records WHERE detail_status='done' AND auction_date IS NOT NULL AND auction_date <> ''"),
    ]

    print("=" * 60)
    print("Detail Coverage Report")
    print(f"total_records        : {total_records}")
    print(f"ming_qing_records    : {ming_qing}")
    print(f"detail_done          : {detail_done}")
    print("-" * 60)

    for name, sql in fields:
        count = scalar(conn, sql)
        print(f"{name:18s}: {count:6d}  ({pct(count, detail_done)})")

    print("-" * 60)
    print("sample rows:")
    for row in conn.execute(
        """
        SELECT artron_id, name, dynasty, vessel_type, glaze_color, motif, mark_text
        FROM auction_records
        WHERE detail_status='done'
        ORDER BY detail_updated_at DESC, id DESC
        LIMIT 10
        """
    ):
        print(row)

    print("=" * 60)
    conn.close()


if __name__ == "__main__":
    main()
