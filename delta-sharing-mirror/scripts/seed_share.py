"""Seed the docker-compose provider bucket with two Delta tables (one CDF-enabled).

Uses deltalake (delta-rs) only, so the provider side needs no Spark either. The reference
server (Spark-based) reads these tables and serves them over Delta Sharing.

    python scripts/seed_share.py                 # create tables
    python scripts/seed_share.py --change        # apply an update/insert/delete round (CDF)
"""

from __future__ import annotations

import argparse
import datetime as dt
import os

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

MINIO = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
STORAGE = {
    "AWS_ENDPOINT_URL": MINIO,
    "AWS_ALLOW_HTTP": "true",
    "AWS_ACCESS_KEY_ID": os.environ.get("MINIO_ROOT_USER", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
    "AWS_REGION": "us-east-1",
}
CDF_URI = "s3://provider/people_cdf"
PLAIN_URI = "s3://provider/people_plain"


def people(rows: list[tuple[int, str, int | None]]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array([r[0] for r in rows], pa.int64()),
            "name": pa.array([r[1] for r in rows], pa.string()),
            "qty": pa.array([r[2] for r in rows], pa.int32()),
            "seen": pa.array(
                [dt.datetime(2026, 1, 1, 12, tzinfo=dt.UTC)] * len(rows),
                pa.timestamp("us", tz="UTC"),
            ),
        }
    )


def seed() -> None:
    write_deltalake(
        CDF_URI,
        people([(1, "ann", 1), (2, "bob", None), (3, "cy", 3)]),
        mode="overwrite",
        storage_options=STORAGE,
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    write_deltalake(
        PLAIN_URI, people([(10, "x", 1), (11, "y", 2)]), mode="overwrite", storage_options=STORAGE
    )
    print("seeded", CDF_URI, DeltaTable(CDF_URI, storage_options=STORAGE).version())
    print("seeded", PLAIN_URI, DeltaTable(PLAIN_URI, storage_options=STORAGE).version())


def change() -> None:
    table = DeltaTable(CDF_URI, storage_options=STORAGE)
    (
        table.merge(
            people([(2, "bob2", 22), (4, "dee", 4)]),
            't."id" = s."id"',
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    DeltaTable(CDF_URI, storage_options=STORAGE).delete('"id" = 1')
    write_deltalake(
        PLAIN_URI, people([(10, "x", 1), (12, "z", 3)]), mode="overwrite", storage_options=STORAGE
    )
    print("changed", CDF_URI, "->", DeltaTable(CDF_URI, storage_options=STORAGE).version())
    print("changed", PLAIN_URI, "->", DeltaTable(PLAIN_URI, storage_options=STORAGE).version())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--change", action="store_true")
    args = parser.parse_args()
    change() if args.change else seed()
