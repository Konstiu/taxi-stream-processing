import argparse
import csv
import json
import time
import glob
import sys
import hashlib
import heapq
from datetime import datetime, timezone
from typing import Iterator, Dict, Any, List, Tuple, Optional
from kafka import KafkaProducer


def to_epoch_ms(s):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return int(
                datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp()
                * 1000
            )
        except ValueError:
            pass
    v = float(s)
    return int(v * 1000) if v < 10_000_000_000 else int(v)


def row_to_rec(row):
    if len(row) < 4:
        return None
    try:
        taxi_id = int(float(row[0].strip()))
    except ValueError:
        return None
    ts = to_epoch_ms(row[1].strip())
    try:
        lon = float(row[2].strip())
        lat = float(row[3].strip())
    except ValueError:
        return None
    return {"taxiId": taxi_id, "ts": int(ts), "lat": lat, "lon": lon}


def iter_file(path) -> Iterator[Dict[str, Any]]:
    with open(path, "r", newline="") as f:
        rdr = csv.reader(f, delimiter=",")
        for row in rdr:
            rec = row_to_rec(row)
            if rec:
                yield rec


def iter_file_with_eof(path) -> Iterator[Dict[str, Any]]:
    last = None
    for rec in iter_file(path):
        last = rec
        yield rec
    # EOF -> emit a sentinel so the merge knows this taxi is done
    if last is not None:
        yield {
            "taxiId": last["taxiId"],
            # put the stop event *after* the last data ts to keep ordering
            "ts": last["ts"] + 1,
            "__eof__": True,
        }


def pick_shard(paths, i, n):
    if n <= 1:
        return paths
    out = []
    for p in paths:
        h = int(hashlib.md5(p.encode()).hexdigest(), 16)
        if (h % n) == i:
            out.append(p)
    return out


def multi_source_merge(
    sources: List[Iterator[Dict[str, Any]]],
) -> Iterator[List[Dict[str, Any]]]:
    """
    Merge many per-file iterators by ascending ts.
    Yield *batches* where all records share the same ts.
    """
    heap: List[Tuple[int, int, Dict[str, Any]]] = []
    for idx, it in enumerate(sources):
        try:
            rec = next(it)
            heap.append((rec["ts"], idx, rec))
        except StopIteration:
            pass
    heapq.heapify(heap)

    while heap:
        ts, idx, rec = heapq.heappop(heap)
        batch = [rec]

        # drain other queued records with the same ts
        while heap and heap[0][0] == ts:
            _, jdx, rec2 = heapq.heappop(heap)
            batch.append(rec2)
            try:
                nxt = next(sources[jdx])
                heapq.heappush(heap, (nxt["ts"], jdx, nxt))
            except StopIteration:
                pass

        # push next from the source we popped
        try:
            nxt = next(sources[idx])
            heapq.heappush(heap, (nxt["ts"], idx, nxt))
        except StopIteration:
            pass

        yield batch


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--glob", required=True)
    ap.add_argument("--shard-index", type=int, default=0)
    ap.add_argument("--shard-total", type=int, default=1)
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument(
        "--compression", default="gzip", choices=["gzip", "lz4", "snappy", "none"]
    )
    ap.add_argument("--linger-ms", type=int, default=50)
    ap.add_argument("--batch-bytes", type=int, default=65536)
    ap.add_argument("--max-files", type=int, default=0)
    ap.add_argument(
        "--transactional-id",
        default=None,
        help="If set, begin/commit a Kafka transaction per timestamp batch (atomic visibility).",
    )
    ap.add_argument(
        "--pace",
        type=float,
        default=0.0,
        help="0=no sleep; 1=recorded timing; <1=faster; >1=slower. Sleep = (next_ts-prev_ts)*pace.",
    )
    args = ap.parse_args()

    comp = None if args.compression == "none" else args.compression
    producer_kwargs = dict(
        bootstrap_servers=args.bootstrap,
        acks="all",
        compression_type=comp,
        linger_ms=args.linger_ms,
        batch_size=args.batch_bytes,
        key_serializer=lambda k: str(k).encode(),
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode(),
    )
    if args.transactional_id:
        producer_kwargs["transactional_id"] = args.transactional_id

    producer = KafkaProducer(**producer_kwargs)
    if args.transactional_id:
        producer.init_transactions()

    files = sorted(glob.glob(args.glob, recursive=True))
    files = pick_shard(files, args.shard_index, args.shard_total)
    if args.max_files and args.max_files > 0:
        files = files[: args.max_files]
    if not files:
        print("No files matched for", args.glob, file=sys.stderr)
        sys.exit(1)

    sources = [iter_file_with_eof(p) for p in files]
    sent = 0
    prev_ts: Optional[int] = None
    pace = float(args.pace)

    for b_idx, batch in enumerate(multi_source_merge(sources), 1):
        # pacing occurs ONLY between timestamp batches
        ts = batch[0]["ts"]
        if prev_ts is not None and pace > 0:
            delta_ms = ts - prev_ts
            if delta_ms > 0:
                time.sleep(delta_ms / 1000.0 * pace)
        prev_ts = ts

        # optional stable order inside batch (not required)
        batch.sort(key=lambda r: r["taxiId"])

        try:
            if args.transactional_id:
                producer.begin_transaction()

            for rec in batch:
                if rec.get("__eof__"):
                    stop_evt = {
                        "taxiId": rec["taxiId"],
                        "ts": rec["ts"],
                        "status": "stopped",
                    }
                    producer.send(args.topic, key=rec["taxiId"], value=stop_evt)
                else:
                    producer.send(args.topic, key=rec["taxiId"], value=rec)

                # producer.send(args.topic, key=rec["taxiId"], value=rec)
                sent += 1

            if args.transactional_id:
                producer.commit_transaction()

            # flush immediately after each timestamp batch
            producer.flush(5)
        except Exception as e:
            if args.transactional_id:
                producer.abort_transaction()
            print(f"[ts={ts}] error: {e}", file=sys.stderr)

        if b_idx % 200 == 0:
            print(f"[{b_idx} ts-batches] sent={sent} last_ts={ts}", file=sys.stderr)

    producer.flush()
    print(f"Done. files={len(files)} records={sent}", file=sys.stderr)


if __name__ == "__main__":
    main()
