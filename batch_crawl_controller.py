import argparse
import json
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

DB_PATH = "porcelain_auction.db"
SCRAPER_PATH = "artron_scraper5.py"
SEARCH_DATASET_PATH = "build_search_dataset.py"
DEFAULT_LOG_PATH = Path("generated") / "controller_runs.jsonl"


def scalar(conn, sql):
    return conn.execute(sql).fetchone()[0]


def get_stats(db_path):
    conn = sqlite3.connect(db_path)
    stats = {
        "records": scalar(conn, "SELECT COUNT(*) FROM auction_records"),
        "detail_done": scalar(conn, "SELECT COUNT(*) FROM auction_records WHERE detail_status='done'"),
        "detail_error": scalar(conn, "SELECT COUNT(*) FROM auction_records WHERE detail_status='error'"),
        "detail_pending": scalar(conn, "SELECT COUNT(*) FROM auction_records WHERE detail_status='pending' OR detail_status IS NULL"),
        "image_done": scalar(conn, "SELECT COUNT(*) FROM auction_records WHERE image_status='done'"),
        "image_error": scalar(conn, "SELECT COUNT(*) FROM auction_records WHERE image_status='error'"),
        "image_pending": scalar(conn, "SELECT COUNT(*) FROM auction_records WHERE image_status='pending' OR image_status IS NULL"),
        "index_keywords": scalar(conn, "SELECT COUNT(*) FROM crawl_queries WHERE stage='index'"),
        "index_completed_keywords": scalar(conn, "SELECT COUNT(*) FROM crawl_queries WHERE stage='index' AND completed=1"),
        "index_incomplete_keywords": scalar(conn, "SELECT COUNT(*) FROM crawl_queries WHERE stage='index' AND completed!=1"),
    }
    rows = conn.execute(
        """
        SELECT artron_id, COALESCE(last_error, ''), COALESCE(detail_updated_at, ''), COALESCE(image_updated_at, '')
        FROM auction_records
        WHERE detail_status='error' OR image_status='error'
        ORDER BY COALESCE(image_updated_at, detail_updated_at) DESC, id DESC
        LIMIT 5
        """
    ).fetchall()
    conn.close()
    stats["recent_errors"] = [{"artron_id": row[0], "error": row[1][:160], "updated_at": row[2] or row[3]} for row in rows]
    return stats


def diff_stats(before, after):
    deltas = {}
    for key, value in after.items():
        if isinstance(value, int):
            deltas[key] = value - int(before.get(key, 0))
    return deltas


def build_scraper_command(args):
    command = [sys.executable, SCRAPER_PATH, "--mode", args.mode, "--delay", str(args.delay)]
    if args.mode == "detail":
        command.extend(["--detail-limit", str(args.limit_per_round)])
        if args.refresh_detail_done:
            command.append("--refresh-detail-done")
    elif args.mode == "images":
        command.extend(["--image-limit", str(args.limit_per_round)])
        if args.download_all_images:
            command.append("--download-all-images")
    return command


def maybe_rebuild_search_dataset(args):
    if not args.rebuild_search_dataset:
        return 0
    command = [sys.executable, SEARCH_DATASET_PATH]
    print(f"$ {' '.join(command)}")
    result = subprocess.run(command, check=False)
    return result.returncode


def should_continue(args, deltas):
    progress_key = "detail_done" if args.mode == "detail" else "image_done"
    if deltas.get(progress_key, 0) < args.min_progress:
        return False, f"{progress_key} delta {deltas.get(progress_key, 0)} < min_progress {args.min_progress}"
    error_key = "detail_error" if args.mode == "detail" else "image_error"
    if deltas.get(error_key, 0) > args.max_new_errors:
        return False, f"{error_key} delta {deltas.get(error_key, 0)} > max_new_errors {args.max_new_errors}"
    return True, "continue"


def print_round_summary(round_no, before, after, deltas, reason):
    print("=" * 60)
    print(f"Round {round_no} summary")
    print(f"records               : {after['records']}  (delta {deltas.get('records', 0):+d})")
    print(f"detail_done           : {after['detail_done']}  (delta {deltas.get('detail_done', 0):+d})")
    print(f"detail_error          : {after['detail_error']}  (delta {deltas.get('detail_error', 0):+d})")
    print(f"detail_pending        : {after['detail_pending']}  (delta {deltas.get('detail_pending', 0):+d})")
    print(f"image_done            : {after['image_done']}  (delta {deltas.get('image_done', 0):+d})")
    print(f"image_error           : {after['image_error']}  (delta {deltas.get('image_error', 0):+d})")
    print(f"image_pending         : {after['image_pending']}  (delta {deltas.get('image_pending', 0):+d})")
    print(f"index_incomplete_keys : {after['index_incomplete_keywords']}")
    print(f"decision              : {reason}")
    if after["recent_errors"]:
        print("recent_errors         :")
        for row in after["recent_errors"]:
            print(f"  {row['updated_at']}  {row['artron_id']}  {row['error']}")
    else:
        print("recent_errors         : none")
    print("=" * 60)


def append_log(log_path, payload):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def parse_args():
    parser = argparse.ArgumentParser(description="Batch controller for resumable crawl rounds.")
    parser.add_argument("--mode", choices=["detail", "images"], required=True)
    parser.add_argument("--rounds", type=int, default=5, help="Maximum rounds to run")
    parser.add_argument("--limit-per-round", type=int, default=200, help="detail/image limit for each round")
    parser.add_argument("--delay", type=float, default=5.0, help="Delay passed to artron_scraper5.py")
    parser.add_argument("--sleep-between-rounds", type=float, default=3.0, help="Pause between rounds")
    parser.add_argument("--min-progress", type=int, default=1, help="Stop if a round adds fewer than this many done rows")
    parser.add_argument("--max-new-errors", type=int, default=20, help="Stop if a round creates more than this many new errors")
    parser.add_argument("--db", default=DB_PATH, help="SQLite database path")
    parser.add_argument("--log-file", default=str(DEFAULT_LOG_PATH), help="JSONL log path")
    parser.add_argument("--no-log", action="store_true", help="Do not write JSONL round logs")
    parser.add_argument("--rebuild-search-dataset", action="store_true", help="Run build_search_dataset.py after each successful round")
    parser.add_argument("--download-all-images", action="store_true", help="Pass through to image mode")
    parser.add_argument("--refresh-detail-done", action="store_true", help="Pass through to detail mode")
    return parser.parse_args()


def main():
    args = parse_args()
    before_all = get_stats(args.db)
    if args.mode == "detail" and before_all["index_incomplete_keywords"] > 0:
        print(
            f"warning: index keywords still incomplete: {before_all['index_incomplete_keywords']}."
            " detail can continue, but new index records may still appear later."
        )

    for round_no in range(1, args.rounds + 1):
        before = get_stats(args.db)
        command = build_scraper_command(args)
        print(f"$ {' '.join(command)}")
        result = subprocess.run(command, check=False)
        after = get_stats(args.db)
        deltas = diff_stats(before, after)

        rebuild_rc = 0
        if result.returncode == 0:
            rebuild_rc = maybe_rebuild_search_dataset(args)

        should_keep_going, reason = should_continue(args, deltas)
        if result.returncode != 0:
            reason = f"scraper exited with code {result.returncode}"
            should_keep_going = False
        elif rebuild_rc != 0:
            reason = f"build_search_dataset.py exited with code {rebuild_rc}"
            should_keep_going = False

        print_round_summary(round_no, before, after, deltas, reason)

        if not args.no_log:
            append_log(
                Path(args.log_file),
                {
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "round": round_no,
                    "mode": args.mode,
                    "command": command,
                    "before": before,
                    "after": after,
                    "delta": deltas,
                    "decision": reason,
                    "scraper_returncode": result.returncode,
                    "search_dataset_returncode": rebuild_rc,
                },
            )

        if not should_keep_going:
            break
        if round_no < args.rounds and args.sleep_between_rounds > 0:
            time.sleep(args.sleep_between_rounds)

    final_stats = get_stats(args.db)
    print("Final snapshot")
    print(f"detail_done           : {final_stats['detail_done']}")
    print(f"detail_error          : {final_stats['detail_error']}")
    print(f"detail_pending        : {final_stats['detail_pending']}")
    print(f"image_done            : {final_stats['image_done']}")
    print(f"image_error           : {final_stats['image_error']}")
    print(f"image_pending         : {final_stats['image_pending']}")
    print(f"index_incomplete_keys : {final_stats['index_incomplete_keywords']}")
    if not args.no_log:
        print(f"log_file              : {args.log_file}")


if __name__ == "__main__":
    main()
