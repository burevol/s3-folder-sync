# -*- coding: utf-8 -*-
"""Минимальный S3-совместимый сервер для тестов s3_sync.py.

Реализует ровно тот набор операций, который нужен скрипту:
HEAD bucket, ListObjectsV2 (с постраничностью), PutObject, GetObject,
DeleteObject, DeleteObjects, GetBucketLocation. Подписи не проверяются.

Используется только в тестах; в работе скрипта не участвует.
"""

from __future__ import annotations

import hashlib
import threading
import urllib.parse
import xml.sax.saxutils as saxutils
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


class Store:
    """Хранилище объектов в памяти."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.buckets: dict[str, dict[str, dict]] = {}
        self.uploads: dict[str, dict] = {}

    # -- multipart ----------------------------------------------------------

    def start_upload(self, bucket: str, key: str, upload_id: str) -> None:
        with self.lock:
            self.uploads[upload_id] = {"bucket": bucket, "key": key, "parts": {}}

    def put_part(self, upload_id: str, number: int, body: bytes) -> str | None:
        with self.lock:
            upload = self.uploads.get(upload_id)
            if upload is None:
                return None
            upload["parts"][number] = body
            return hashlib.md5(body).hexdigest()

    def complete_upload(self, upload_id: str) -> tuple[str, str, str] | None:
        with self.lock:
            upload = self.uploads.pop(upload_id, None)
            if upload is None:
                return None
            digest = hashlib.md5()
            for number in sorted(upload["parts"]):
                digest.update(bytes.fromhex(hashlib.md5(upload["parts"][number]).hexdigest()))
            body = b"".join(upload["parts"][number] for number in sorted(upload["parts"]))
            etag = f"{digest.hexdigest()}-{len(upload['parts'])}"
            self.buckets.setdefault(upload["bucket"], {})[upload["key"]] = {
                "body": body,
                "etag": etag,
                "mtime": _now(),
            }
            return upload["bucket"], upload["key"], etag

    def abort_upload(self, upload_id: str) -> None:
        with self.lock:
            self.uploads.pop(upload_id, None)

    def create_bucket(self, bucket: str) -> None:
        with self.lock:
            self.buckets.setdefault(bucket, {})

    def has_bucket(self, bucket: str) -> bool:
        return bucket in self.buckets

    def put(self, bucket: str, key: str, body: bytes) -> str:
        etag = hashlib.md5(body).hexdigest()
        with self.lock:
            self.buckets.setdefault(bucket, {})[key] = {"body": body, "etag": etag, "mtime": _now()}
        return etag

    def get(self, bucket: str, key: str):
        with self.lock:
            return self.buckets.get(bucket, {}).get(key)

    def delete(self, bucket: str, key: str) -> None:
        with self.lock:
            self.buckets.get(bucket, {}).pop(key, None)

    def list(self, bucket: str, prefix: str) -> list[tuple[str, dict]]:
        with self.lock:
            items = self.buckets.get(bucket, {})
            return sorted(((key, dict(value)) for key, value in items.items() if key.startswith(prefix)))


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "FakeS3/1.0"

    # -- утилиты ------------------------------------------------------------

    def log_message(self, *args):  # noqa: D102 - тишина в тестах
        pass

    @property
    def store(self) -> Store:
        return self.server.store  # type: ignore[attr-defined]

    def _read_body(self) -> bytes:
        transfer_encoding = (self.headers.get("Transfer-Encoding") or "").lower()
        if "chunked" in transfer_encoding:
            chunks = []
            while True:
                line = self.rfile.readline().strip()
                if not line:
                    break
                size = int(line.split(b";")[0], 16)
                if size == 0:
                    self.rfile.readline()  # завершающий CRLF
                    break
                chunks.append(self.rfile.read(size))
                self.rfile.read(2)  # CRLF после чанка
            return b"".join(chunks)
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def _send(self, status: int, body: bytes = b"", content_type: str = "application/xml", extra: dict | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for name, value in (extra or {}).items():
            self.send_header(name, value)
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _error(self, status: int, code: str, message: str = "") -> None:
        body = (
            f'<?xml version="1.0" encoding="UTF-8"?><Error><Code>{code}</Code>'
            f"<Message>{saxutils.escape(message)}</Message></Error>"
        ).encode("utf-8")
        self._send(status, body)

    def _split_path(self) -> tuple[str, str]:
        parsed = urllib.parse.urlsplit(self.path)
        segments = parsed.path.lstrip("/").split("/", 1)
        bucket = urllib.parse.unquote(segments[0]) if segments and segments[0] else ""
        key = urllib.parse.unquote(segments[1]) if len(segments) > 1 else ""
        return bucket, key

    def _query(self) -> dict[str, list[str]]:
        return urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query, keep_blank_values=True)

    # -- HTTP-методы --------------------------------------------------------

    def do_HEAD(self) -> None:  # noqa: N802
        bucket, key = self._split_path()
        if not self.store.has_bucket(bucket):
            self._error(404, "NoSuchBucket", bucket)
            return
        if key:
            item = self.store.get(bucket, key)
            if item is None:
                self._error(404, "NoSuchKey", key)
                return
            self._send(200, b"", extra={"ETag": f'"{item["etag"]}"', "Last-Modified": item["mtime"]})
            return
        self._send(200, b"")

    def do_GET(self) -> None:  # noqa: N802
        bucket, key = self._split_path()
        query = self._query()
        if not self.store.has_bucket(bucket):
            self._error(404, "NoSuchBucket", bucket)
            return
        if "location" in query:
            body = f'<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="{XMLNS}">spb</LocationConstraint>'
            self._send(200, body.encode("utf-8"))
            return
        if key:
            item = self.store.get(bucket, key)
            if item is None:
                self._error(404, "NoSuchKey", key)
                return
            self._send(200, item["body"], "application/octet-stream", {"ETag": f'"{item["etag"]}"'})
            return
        self._list_objects(bucket, query)

    def _list_objects(self, bucket: str, query: dict[str, list[str]]) -> None:
        prefix = (query.get("prefix") or [""])[0]
        max_keys = int((query.get("max-keys") or ["1000"])[0])
        token = (query.get("continuation-token") or [""])[0]
        start = int(token) if token else 0

        items = self.store.list(bucket, prefix)
        page = items[start : start + max_keys]
        truncated = start + max_keys < len(items)

        chunks = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            f'<ListBucketResult xmlns="{XMLNS}">',
            f"<Name>{saxutils.escape(bucket)}</Name>",
            f"<Prefix>{saxutils.escape(prefix)}</Prefix>",
            f"<KeyCount>{len(page)}</KeyCount>",
            f"<MaxKeys>{max_keys}</MaxKeys>",
            f"<IsTruncated>{'true' if truncated else 'false'}</IsTruncated>",
        ]
        if truncated:
            chunks.append(f"<NextContinuationToken>{start + max_keys}</NextContinuationToken>")
        for key, item in page:
            chunks.append(
                "<Contents>"
                f"<Key>{saxutils.escape(key)}</Key>"
                f"<LastModified>{item['mtime']}</LastModified>"
                f"<ETag>&quot;{item['etag']}&quot;</ETag>"
                f"<Size>{len(item['body'])}</Size>"
                "<StorageClass>STANDARD</StorageClass>"
                "</Contents>"
            )
        chunks.append("</ListBucketResult>")
        self._send(200, "".join(chunks).encode("utf-8"))

    def do_PUT(self) -> None:  # noqa: N802
        bucket, key = self._split_path()
        query = self._query()
        body = self._read_body()
        if not key:
            self.store.create_bucket(bucket)
            self._send(200, b"")
            return
        if "uploadId" in query:
            upload_id = query["uploadId"][0]
            number = int((query.get("partNumber") or ["1"])[0])
            etag = self.store.put_part(upload_id, number, body)
            if etag is None:
                self._error(404, "NoSuchUpload", upload_id)
                return
            self._send(200, b"", extra={"ETag": f'"{etag}"'})
            return
        etag = self.store.put(bucket, key, body)
        self._send(200, b"", extra={"ETag": f'"{etag}"'})

    def do_POST(self) -> None:  # noqa: N802
        bucket, key = self._split_path()
        query = self._query()

        if "uploads" in query:
            upload_id = f"upload-{abs(hash((bucket, key, _now()))) % 10**9}"
            self.store.start_upload(bucket, key, upload_id)
            body = (
                f'<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult xmlns="{XMLNS}">'
                f"<Bucket>{saxutils.escape(bucket)}</Bucket><Key>{saxutils.escape(key)}</Key>"
                f"<UploadId>{upload_id}</UploadId></InitiateMultipartUploadResult>"
            )
            self._send(200, body.encode("utf-8"))
            return

        if "uploadId" in query:
            self._read_body()
            completed = self.store.complete_upload(query["uploadId"][0])
            if completed is None:
                self._error(404, "NoSuchUpload", query["uploadId"][0])
                return
            done_bucket, done_key, etag = completed
            body = (
                f'<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult xmlns="{XMLNS}">'
                f"<Location>/{saxutils.escape(done_bucket)}/{saxutils.escape(done_key)}</Location>"
                f"<Bucket>{saxutils.escape(done_bucket)}</Bucket><Key>{saxutils.escape(done_key)}</Key>"
                f"<ETag>&quot;{etag}&quot;</ETag></CompleteMultipartUploadResult>"
            )
            self._send(200, body.encode("utf-8"))
            return

        if "delete" not in query or not self.store.has_bucket(bucket):
            self._error(404, "NoSuchBucket", bucket)
            return
        body = self._read_body().decode("utf-8", "replace")
        keys = []
        while "<Key>" in body:
            _, _, rest = body.partition("<Key>")
            key, _, body = rest.partition("</Key>")
            keys.append(urllib.parse.unquote(key))
        for key in keys:
            self.store.delete(bucket, key)
        self._send(200, f'<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="{XMLNS}"></DeleteResult>'.encode("utf-8"))

    def do_DELETE(self) -> None:  # noqa: N802
        bucket, key = self._split_path()
        query = self._query()
        self._read_body()
        if "uploadId" in query:
            self.store.abort_upload(query["uploadId"][0])
            self._send(204, b"")
            return
        if not self.store.has_bucket(bucket):
            self._error(404, "NoSuchBucket", bucket)
            return
        if key:
            self.store.delete(bucket, key)
        self._send(204, b"")


class FakeS3Server:
    """Сервер для тестов: start()/stop() плюс адрес и хранилище."""

    def __init__(self, port: int = 0) -> None:
        self.store = Store()
        self.httpd = ThreadingHTTPServer(("127.0.0.1", port), _Handler)
        self.httpd.daemon_threads = True
        self.httpd.store = self.store  # type: ignore[attr-defined]
        self.port = self.httpd.server_address[1]
        self.endpoint = f"http://127.0.0.1:{self.port}"
        self._thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def start(self) -> "FakeS3Server":
        self._thread.start()
        return self

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self._thread.join(timeout=5)
