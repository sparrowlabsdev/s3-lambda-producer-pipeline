"""An in-process Delta Sharing server speaking the subset of the protocol the client uses.

It is **not** a reference implementation. It exists so the real ``delta-sharing`` client and the
real ``deltalake`` writer can be exercised end to end in CI without Docker. The docker-compose
stack (``docker-compose.test.yml``) runs the same tests against the open-source reference server.

Protocol surface (parquet response format):
  GET  /shares/{s}/schemas/{sc}/tables/{t}/version   -> header delta-table-version
  GET  /shares/{s}/schemas/{sc}/tables/{t}/metadata  -> protocol + metaData lines
  POST /shares/{s}/schemas/{sc}/tables/{t}/query     -> protocol, metaData, file lines
  GET  /shares/{s}/schemas/{sc}/tables/{t}/changes   -> protocol, metaData, add/cdf/remove lines
  GET  /files/{id}                                   -> parquet bytes ("pre-signed URL")
"""

from __future__ import annotations

import io
import json
import re
import threading
import uuid
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

_ARROW_TO_SPARK: list[tuple[Any, str]] = [
    (pa.types.is_string, "string"),
    (pa.types.is_int64, "long"),
    (pa.types.is_int32, "integer"),
    (pa.types.is_int16, "short"),
    (pa.types.is_int8, "byte"),
    (pa.types.is_float64, "double"),
    (pa.types.is_float32, "float"),
    (pa.types.is_boolean, "boolean"),
    (pa.types.is_date, "date"),
    (pa.types.is_timestamp, "timestamp"),
    (pa.types.is_binary, "binary"),
]


def spark_schema_string(schema: pa.Schema) -> str:
    fields = []
    for f in schema:
        spark_type = "string"
        if pa.types.is_decimal(f.type):
            spark_type = f"decimal({f.type.precision},{f.type.scale})"
        else:
            for pred, name in _ARROW_TO_SPARK:
                if pred(f.type):
                    spark_type = name
                    break
        fields.append({"name": f.name, "type": spark_type, "nullable": True, "metadata": {}})
    return json.dumps({"type": "struct", "fields": fields})


@dataclass
class Action:
    kind: str  # add | cdf | remove
    data: pa.Table
    version: int
    timestamp: int


@dataclass
class FakeTable:
    schema: pa.Schema
    keys: list[str]
    cdf_enabled: bool = True
    cdf_fail: bool = False  # simulate a provider that rejects CDF queries
    snapshots: dict[int, pa.Table] = field(default_factory=dict)
    actions: dict[int, list[Action]] = field(default_factory=dict)
    version: int = -1
    table_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def _bump(self) -> int:
        self.version += 1
        self.actions[self.version] = []
        return self.version

    def _ts(self) -> int:
        return 1_700_000_000_000 + self.version * 1000

    def _cdc(self, rows: pa.Table, change_type: str) -> pa.Table:
        return rows.append_column("_change_type", pa.array([change_type] * rows.num_rows))

    def current(self) -> pa.Table:
        return self.snapshots.get(self.version, self.schema.empty_table())

    def overwrite(self, rows: pa.Table) -> int:
        old = self.current()
        v = self._bump()
        self.snapshots[v] = rows
        if v == 0:
            self.actions[v].append(Action("add", rows, v, self._ts()))
        elif self.cdf_enabled:
            # Delta emits delete + insert pairs for an overwrite; the collapse must resolve them.
            self.actions[v].append(Action("cdf", self._cdc(old, "delete"), v, self._ts()))
            self.actions[v].append(Action("cdf", self._cdc(rows, "insert"), v, self._ts()))
        return v

    def _key_mask(self, table: pa.Table, keys_table: pa.Table) -> pa.ChunkedArray:
        key_strings = [
            "|".join(str(r[k]) for k in self.keys) for r in keys_table.select(self.keys).to_pylist()
        ]
        own = pa.array(
            ["|".join(str(r[k]) for k in self.keys) for r in table.to_pylist()], pa.string()
        )
        return pa.chunked_array([pc.is_in(own, value_set=pa.array(key_strings, pa.string()))])

    def upsert(self, rows: pa.Table) -> int:
        old = self.current()
        mask = self._key_mask(old, rows)
        existing = old.filter(mask)
        untouched = old.filter(pc.invert(mask))
        new_mask = self._key_mask(rows, existing)
        updated = rows.filter(new_mask)
        inserted = rows.filter(pc.invert(new_mask))
        v = self._bump()
        self.snapshots[v] = pa.concat_tables([untouched, rows.cast(self.schema)])
        parts = [
            self._cdc(existing, "update_preimage"),
            self._cdc(updated, "update_postimage"),
            self._cdc(inserted, "insert"),
        ]
        self.actions[v].append(
            Action("cdf", pa.concat_tables([p for p in parts if p.num_rows]), v, self._ts())
        )
        return v

    def delete(self, keys: pa.Table) -> int:
        old = self.current()
        mask = self._key_mask(old, keys)
        removed = old.filter(mask)
        v = self._bump()
        self.snapshots[v] = old.filter(pc.invert(mask))
        self.actions[v].append(Action("cdf", self._cdc(removed, "delete"), v, self._ts()))
        return v

    def metadata_only_version(self) -> int:
        """A commit without row changes (OPTIMIZE / property change)."""
        v = self._bump()
        self.snapshots[v] = self.snapshots[v - 1]
        return v


@dataclass
class FakeShare:
    token: str
    tables: dict[tuple[str, str, str], FakeTable] = field(default_factory=dict)
    files: dict[str, bytes] = field(default_factory=dict)
    requests: list[str] = field(default_factory=list)

    def add(self, share: str, schema: str, name: str, table: FakeTable) -> FakeTable:
        self.tables[(share, schema, name)] = table
        return table

    def stash(self, table: pa.Table) -> str:
        buf = io.BytesIO()
        pq.write_table(table, buf)
        file_id = uuid.uuid4().hex
        self.files[file_id] = buf.getvalue()
        return file_id


_TABLE_RE = re.compile(r"^/delta-sharing/shares/([^/]+)/schemas/([^/]+)/tables/([^/]+)/(\w+)$")


class _Handler(BaseHTTPRequestHandler):
    share: FakeShare

    def log_message(self, *_: Any) -> None:  # keep test output quiet
        return

    def _base(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}"

    def _json_lines(
        self, lines: list[dict[str, Any]], headers: dict[str, str], status: int = 200
    ) -> None:
        body = "\n".join(json.dumps(line) for line in lines).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/x-ndjson; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        for k, v in headers.items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status: int, message: str) -> None:
        body = json.dumps({"errorCode": "BAD_REQUEST", "message": message}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _file(self, file_id: str, head: bool = False) -> None:
        data = self.share.files.get(file_id)
        if data is None:
            self._error(404, "no such file")
            return
        rng = self.headers.get("Range")
        status, payload = 200, data
        if rng and rng.startswith("bytes="):
            start_s, end_s = rng[6:].split("-")
            start = int(start_s) if start_s else max(0, len(data) - int(end_s))
            end = int(end_s) if end_s and start_s else len(data) - 1
            payload, status = data[start : end + 1], 206
        self.send_response(status)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(len(payload)))
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        self.end_headers()
        if not head:
            self.wfile.write(payload)

    def _table(self) -> tuple[FakeTable, str] | None:
        path = urlparse(self.path).path
        if self.headers.get("Authorization") != f"Bearer {self.share.token}":
            self._error(401, "unauthorized")
            return None
        match = _TABLE_RE.match(path)
        if not match:
            self._error(404, f"unknown path {path}")
            return None
        key = (match.group(1), match.group(2), match.group(3))
        table = self.share.tables.get(key)
        if table is None:
            self._error(404, f"unknown table {key}")
            return None
        return table, match.group(4)

    def _proto_meta(self, table: FakeTable) -> list[dict[str, Any]]:
        config = {"delta.enableChangeDataFeed": "true"} if table.cdf_enabled else {}
        return [
            {"protocol": {"minReaderVersion": 1}},
            {
                "metaData": {
                    "id": table.table_id,
                    "format": {"provider": "parquet"},
                    "schemaString": spark_schema_string(table.schema),
                    "partitionColumns": [],
                    "configuration": config,
                }
            },
        ]

    def do_HEAD(self) -> None:
        path = urlparse(self.path).path
        if path.startswith("/files/"):
            self._file(path.split("/files/", 1)[1], head=True)
        else:
            self._error(404, "not found")

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        self.share.requests.append(f"GET {parsed.path}")
        if parsed.path.startswith("/files/"):
            self._file(parsed.path.split("/files/", 1)[1])
            return
        resolved = self._table()
        if resolved is None:
            return
        table, op = resolved
        headers = {"delta-table-version": str(table.version)}
        if op == "version":
            self._json_lines([], headers)
        elif op == "metadata":
            self._json_lines(self._proto_meta(table), headers)
        elif op == "changes":
            if not table.cdf_enabled or table.cdf_fail:
                self._error(400, "cdf is not enabled on table")
                return
            params = parse_qs(parsed.query)
            start = int(params.get("startingVersion", ["0"])[0])
            end = int(params.get("endingVersion", [str(table.version)])[0])
            if start < 0 or end > table.version or start > end:
                self._error(400, f"invalid version range {start}-{end}")
                return
            lines = self._proto_meta(table)
            for v in range(start, end + 1):
                for action in table.actions.get(v, []):
                    file_id = self.share.stash(action.data)
                    lines.append(
                        {
                            action.kind: {
                                "url": f"{self._base()}/files/{file_id}",
                                "id": file_id,
                                "partitionValues": {},
                                "size": len(self.share.files[file_id]),
                                "timestamp": action.timestamp,
                                "version": action.version,
                            }
                        }
                    )
            self._json_lines(lines, headers)
        else:
            self._error(404, f"unsupported GET {op}")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        self.share.requests.append(f"POST {parsed.path}")
        resolved = self._table()
        if resolved is None:
            return
        table, op = resolved
        if op != "query":
            self._error(404, f"unsupported POST {op}")
            return
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length) or b"{}")
        version = body.get("version")
        version = table.version if version is None else int(version)
        if version not in table.snapshots:
            self._error(400, f"version {version} not available")
            return
        snapshot = table.snapshots[version]
        lines = self._proto_meta(table)
        if snapshot.num_rows:
            file_id = self.share.stash(snapshot)
            lines.append(
                {
                    "file": {
                        "url": f"{self._base()}/files/{file_id}",
                        "id": file_id,
                        "partitionValues": {},
                        "size": len(self.share.files[file_id]),
                    }
                }
            )
        self._json_lines(lines, {"delta-table-version": str(version)})


class FakeSharingServer:
    def __init__(self, share: FakeShare) -> None:
        handler = type("BoundHandler", (_Handler,), {"share": share})
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self._server.server_port}/delta-sharing"

    def start(self) -> FakeSharingServer:
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
