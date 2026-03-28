import sqlite3

DB_PATH = "porcelain_auction.db"


def scalar(conn, sql):
    return conn.execute(sql).fetchone()[0]


def main():
    conn = sqlite3.connect(DB_PATH)

    total = scalar(conn, "SELECT COUNT(*) FROM auction_records")
    detail_done = scalar(conn, "SELECT COUNT(*) FROM auction_records WHERE detail_status='done'")
    detail_error = scalar(conn, "SELECT COUNT(*) FROM auction_records WHERE detail_status='error'")
    detail_pending = scalar(conn, "SELECT COUNT(*) FROM auction_records WHERE detail_status='pending' OR detail_status IS NULL")
    image_done = scalar(conn, "SELECT COUNT(*) FROM auction_records WHERE image_status='done'")
    image_error = scalar(conn, "SELECT COUNT(*) FROM auction_records WHERE image_status='error'")
    image_pending = scalar(conn, "SELECT COUNT(*) FROM auction_records WHERE image_status='pending' OR image_status IS NULL")

    print("=" * 60)
    print("Crawler Status")
    print(f"database           : {DB_PATH}")
    print(f"total_records      : {total}")
    print("-" * 60)
    print(f"detail_done        : {detail_done}")
    print(f"detail_error       : {detail_error}")
    print(f"detail_pending     : {detail_pending}")
    print("-" * 60)
    print(f"image_done         : {image_done}")
    print(f"image_error        : {image_error}")
    print(f"image_pending      : {image_pending}")
    print("-" * 60)
    print("recent detail errors:")

    rows = list(
        conn.execute(
            """
            SELECT artron_id, COALESCE(last_error, ''), COALESCE(detail_updated_at, '')
            FROM auction_records
            WHERE detail_status='error'
            ORDER BY detail_updated_at DESC, id DESC
            LIMIT 10
            """
        )
    )
    if not rows:
        print("none")
    else:
        for artron_id, err, updated_at in rows:
            print(f"{updated_at}  {artron_id}  {err[:160]}")

    print("=" * 60)
    conn.close()


if __name__ == "__main__":
    main()
