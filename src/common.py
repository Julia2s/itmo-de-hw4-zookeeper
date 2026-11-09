from __future__ import annotations

import codecs
import csv
import hashlib
import io
import json
import os
import platform
import sys
import time
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, Iterator

S3_BACKEND      = os.environ.get("S3_BACKEND",      "boto3")
S3_ENDPOINT     = os.environ.get("S3_ENDPOINT",     "http://localhost:9010")
S3_ACCESS_KEY   = os.environ.get("S3_ACCESS_KEY")
S3_SECRET_KEY   = os.environ.get("S3_SECRET_KEY")
S3_BUCKET       = os.environ.get("S3_BUCKET",       "hh-lake")
S3_REGION       = os.environ.get("S3_REGION",       "us-east-1")
LOCAL_FS_ROOT   = os.environ.get("LOCAL_FS_ROOT",   "./data/s3")

VACANCIES_KEY   = os.environ.get("VACANCIES_KEY",   "raw/vacancies.csv")
CURATED_KEY     = os.environ.get("CURATED_KEY",     "curated/devops_skills.csv")

READ_CHUNK_BYTES = int(os.environ.get("READ_CHUNK_BYTES", str(8 * 1024 * 1024)))

VARIANT_ROLE = "DevOps-инженер"
VARIANT_NAME_KEYWORDS = (
    "devops", "dev-ops", "dev ops", "sre", "site reliability",
    "release engineer", "релиз-инженер",
    "platform engineer", "платформенный инженер",
    "infrastructure engineer", "инфраструктурный инженер",
    "cloud engineer", "ci/cd", "ci cd",
    "ops engineer", "automation engineer", "инженер автоматизации",
    "эксплуатация",
)

FILTER_MODES = ("role", "role_or_name", "name")
VARIANT_FILTER_MODE = os.environ.get("VARIANT_FILTER_MODE", "role").strip().lower()
if VARIANT_FILTER_MODE not in FILTER_MODES:
    raise ValueError(
        f"VARIANT_FILTER_MODE={VARIANT_FILTER_MODE!r}; expected one of {FILTER_MODES}"
    )

VACANCY_FIELDS = [
    "source", "vacancy_id", "status", "snapshot_date", "published_at",
    "area", "employer_id", "employer_inn", "employer_ogrn", "employer_name",
    "name", "role", "sphere", "salary_from", "salary_to", "currency", "gross",
    "experience", "schedule", "employment", "key_skills", "description", "url",
]

CURRENCY_TO_RUB = {
    "RUR": 1.0, "RUB": 1.0,
    "USD": 90.0, "EUR": 100.0,
    "KZT": 0.19,
}

def is_devops_role(row: dict) -> bool:
    return (row.get("role") or "").strip() == VARIANT_ROLE

def is_devops_name(row: dict) -> bool:
    name = (row.get("name") or "").lower()
    return any(kw in name for kw in VARIANT_NAME_KEYWORDS)

def matches_variant(row: dict, mode: str | None = None) -> bool:
    mode = mode or VARIANT_FILTER_MODE
    role = is_devops_role(row)
    if mode == "role":
        return role
    if mode == "name":
        return is_devops_name(row)
    if mode == "role_or_name":
        return role or is_devops_name(row)
    raise ValueError(f"unknown filter mode {mode!r}")

def is_devops(row: dict, fallback_to_name: bool | None = None) -> bool:
    """Backwards-compatible wrapper.

    fallback_to_name=None  -> use VARIANT_FILTER_MODE (default config)
    fallback_to_name=True  -> role OR name (legacy)
    fallback_to_name=False -> role only
    """
    if fallback_to_name is None:
        return matches_variant(row)
    return is_devops_role(row) or (fallback_to_name and is_devops_name(row))

def detect_filter_mode(rows_sample: list[dict]) -> str:
    if not rows_sample:
        return "none"
    if next((r for r in rows_sample if is_devops_role(r)), None) is not None:
        return "role"
    if next((r for r in rows_sample if is_devops_name(r)), None) is not None:
        return "name"
    return "none"

def parse_skills(raw_field: str, sep: str = ";") -> list[str]:
    if not raw_field:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for token in raw_field.split(sep):
        s = unicodedata.normalize("NFC", token.strip().lower())
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out

def salary_to_rub(row: dict) -> float | None:
    """Normalise salary_from/salary_to to a monthly RUB gross->net estimate.

    Returns None for missing/zero/invalid values or unsupported currency.
    """
    cur = (row.get("currency") or "RUB").strip().upper()
    gross = (row.get("gross") or "").strip().lower() in ("true", "1", "yes", "да")
    rate = CURRENCY_TO_RUB.get(cur)
    if rate is None:
        return None

    def _num(value) -> float | None:
        if value is None:
            return None
        text = str(value).strip().replace(" ", "").replace(",", ".")
        if not text:
            return None
        try:
            number = float(text)
        except ValueError:
            return None
        return number if number > 0 else None

    sf = _num(row.get("salary_from"))
    st = _num(row.get("salary_to"))
    if sf is not None and st is not None:
        base = (sf + st) / 2
    elif sf is not None:
        base = sf
    elif st is not None:
        base = st
    else:
        return None
    rub = base * rate
    if gross:
        rub *= 0.87
    return rub

def date_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

@dataclass
class ReadStats:
    lines_total: int = 0
    rows_parsed: int = 0
    skipped_blank: int = 0
    skipped_malformed: int = 0
    skipped_header: int = 0

    def as_dict(self) -> dict:
        return {
            "lines_total": self.lines_total,
            "rows_parsed": self.rows_parsed,
            "skipped_blank": self.skipped_blank,
            "skipped_malformed": self.skipped_malformed,
            "skipped_header": self.skipped_header,
        }

def iter_object_lines(
    cli: "S3Client",
    key: str = VACANCIES_KEY,
    chunk_bytes: int = READ_CHUNK_BYTES,
) -> Iterator[str]:
    """Yield text lines from an S3 object using ranged GETs.

    The object is never fully materialised in memory. UTF-8 multibyte
    sequences are safe across range boundaries because ranges are decoded
    with an incremental decoder and lines are split on '\\n'.
    """
    info = cli.head_object(key)
    if info is None:
        raise FileNotFoundError(
            f"S3-объект s3://{S3_BUCKET}/{key} не найден "
            f"(backend={S3_BACKEND}, endpoint={S3_ENDPOINT})"
        )
    size = info.size
    decoder = codecs.getincrementaldecoder("utf-8")("replace")
    buf = ""
    offset = 0
    while offset < size:
        end = min(offset + chunk_bytes, size) - 1
        raw = cli.get_object(key, start=offset, end=end)
        if not raw:
            break
        offset = end + 1
        buf += decoder.decode(raw)
        start = 0
        while True:
            nl = buf.find("\n", start)
            if nl < 0:
                break
            yield buf[start:nl].rstrip("\r")
            start = nl + 1
        buf = buf[start:]
    buf += decoder.decode(b"", final=True)
    if buf:
        yield buf.rstrip("\r")

def parse_vacancy_line(line: str) -> tuple[dict | None, str | None]:
    """Parse one physical CSV line into a 23-field dict.

    Returns (row, None) on success, (None, reason) otherwise.
    reason is one of: 'blank', 'header', 'malformed'.
    """
    if not line or not line.strip():
        return None, "blank"
    try:
        cells = next(csv.reader([line]))
    except (csv.Error, StopIteration):
        return None, "malformed"
    if len(cells) != len(VACANCY_FIELDS):
        return None, "malformed"
    if cells[0].strip().lower() == "source" or cells[11].strip().lower() == "role":
        return None, "header"
    return dict(zip(VACANCY_FIELDS, cells)), None

def iter_vacancy_rows(
    cli: "S3Client",
    stats: ReadStats | None = None,
    key: str = VACANCIES_KEY,
    chunk_bytes: int = READ_CHUNK_BYTES,
) -> Iterator[dict]:
    """Iterate valid vacancy rows, skipping blanks, headers and broke rows."""
    for line in iter_object_lines(cli, key, chunk_bytes):
        if stats is not None:
            stats.lines_total += 1
        row, reason = parse_vacancy_line(line)
        if row is None:
            if stats is not None:
                if reason == "blank":
                    stats.skipped_blank += 1
                elif reason == "header":
                    stats.skipped_header += 1
                else:
                    stats.skipped_malformed += 1
            continue
        if stats is not None:
            stats.rows_parsed += 1
        yield row

def iter_variant_rows(
    cli: "S3Client",
    stats: ReadStats | None = None,
    mode: str | None = None,
) -> Iterator[dict]:
    for row in iter_vacancy_rows(cli, stats):
        if matches_variant(row, mode):
            yield row

@dataclass
class S3ObjectInfo:
    key: str
    size: int
    etag: str | None = None
    last_modified: str | None = None

class S3Client:
    def put_object(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        raise NotImplementedError

    def get_object(self, key: str, start: int | None = None, end: int | None = None) -> bytes:
        raise NotImplementedError

    def head_object(self, key: str) -> S3ObjectInfo | None:
        raise NotImplementedError

    def list_objects(self, prefix: str = "") -> list[S3ObjectInfo]:
        raise NotImplementedError

    def delete_object(self, key: str) -> None:
        raise NotImplementedError

class Boto3S3Client(S3Client):
    def __init__(self) -> None:
        import boto3
        from botocore.client import Config as BotoConfig
        from botocore.exceptions import ClientError
        self._boto = boto3
        self._client_error = ClientError
        self._cfg = BotoConfig(
            connect_timeout=10,
            read_timeout=180,
            retries={"max_attempts": 3, "mode": "standard"},
            s3={"addressing_style": "path"},
        )
        self._client = None

    def _client_get(self):
        if self._client is None:
            self._client = self._boto.client(
                "s3",
                endpoint_url=S3_ENDPOINT,
                aws_access_key_id=S3_ACCESS_KEY or None,
                aws_secret_access_key=S3_SECRET_KEY or None,
                region_name=S3_REGION,
                config=self._cfg,
            )
        return self._client

    def put_object(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        self._client_get().put_object(
            Bucket=S3_BUCKET, Key=key, Body=data, ContentType=content_type
        )

    def get_object(self, key: str, start: int | None = None, end: int | None = None) -> bytes:
        kw: dict = {"Bucket": S3_BUCKET, "Key": key}
        if start is not None or end is not None:
            s = start if start is not None else 0
            e = end if end is not None else ""
            kw["Range"] = f"bytes={s}-{e}"
        resp = self._client_get().get_object(**kw)
        return resp["Body"].read()

    def head_object(self, key: str) -> S3ObjectInfo | None:
        try:
            r = self._client_get().head_object(Bucket=S3_BUCKET, Key=key)
        except self._client_error as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in ("404", "NoSuchKey", "NotFound") or status == 404:
                return None
            raise
        return S3ObjectInfo(
            key=key,
            size=int(r.get("ContentLength", 0)),
            etag=str(r.get("ETag", "")).strip('"') or None,
            last_modified=str(r.get("LastModified", "")) or None,
        )

    def list_objects(self, prefix: str = "") -> list[S3ObjectInfo]:
        out: list[S3ObjectInfo] = []
        paginator = self._client_get().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for o in page.get("Contents", []):
                out.append(S3ObjectInfo(
                    key=o["Key"],
                    size=int(o["Size"]),
                    etag=str(o.get("ETag", "")).strip('"') or None,
                    last_modified=str(o.get("LastModified", "")) or None,
                ))
        return out

    def copy_object(self, src_key: str, dst_key: str, src_bucket: str | None = None) -> None:
        sb = src_bucket or S3_BUCKET
        self._client_get().copy_object(
            Bucket=S3_BUCKET, Key=dst_key,
            CopySource={"Bucket": sb, "Key": src_key},
        )

    def delete_object(self, key: str) -> None:
        # S3 DeleteObject is idempotent: a missing key is not an error.
        self._client_get().delete_object(Bucket=S3_BUCKET, Key=key)

class LocalFSClient(S3Client):
    """Filesystem-backed stand-in used by unit tests and offline runs."""

    def __init__(self) -> None:
        self._root = os.path.join(LOCAL_FS_ROOT, S3_BUCKET)
        os.makedirs(self._root, exist_ok=True)

    def _path(self, key: str) -> str:
        return os.path.join(self._root, key)

    def put_object(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)

    def get_object(self, key: str, start: int | None = None, end: int | None = None) -> bytes:
        path = self._path(key)
        with open(path, "rb") as f:
            if start is None and end is None:
                return f.read()
            if start is None:
                start = 0
            f.seek(start)
            if end is None:
                return f.read()
            return f.read(end - start + 1)

    def head_object(self, key: str) -> S3ObjectInfo | None:
        path = self._path(key)
        if not os.path.exists(path):
            return None
        st = os.stat(path)
        return S3ObjectInfo(
            key=key,
            size=st.st_size,
            last_modified=datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat(),
        )

    def list_objects(self, prefix: str = "") -> list[S3ObjectInfo]:
        out: list[S3ObjectInfo] = []
        if not os.path.isdir(self._root):
            return out
        for dirpath, _d, filenames in os.walk(self._root):
            for fname in filenames:
                full = os.path.join(dirpath, fname)
                rel = os.path.relpath(full, self._root).replace(os.sep, "/")
                if not rel.startswith(prefix):
                    continue
                out.append(S3ObjectInfo(key=rel, size=os.path.getsize(full)))
        return sorted(out, key=lambda x: x.key)

    def copy_object(self, src_key: str, dst_key: str, src_bucket: str | None = None) -> None:
        data = self.get_object(src_key)
        self.put_object(dst_key, data)

    def delete_object(self, key: str) -> None:
        path = self._path(key)
        if os.path.exists(path):
            os.remove(path)

def get_s3_client() -> S3Client:
    if S3_BACKEND == "boto3":
        return Boto3S3Client()
    if S3_BACKEND == "local":
        return LocalFSClient()
    raise ValueError(f"unknown S3_BACKEND={S3_BACKEND!r}; expected 'boto3' or 'local'")

def write_frequency_results(
    cli: S3Client,
    counters: dict[int, Counter],
    output_prefix: str,
    local_dir: str,
) -> dict:
    """Write skills_{k}.csv locally and to S3, sorted by frequency desc."""
    os.makedirs(local_dir, exist_ok=True)
    written: dict[int, dict] = {}
    for k in sorted(counters):
        c = counters[k]
        rows_sorted = sorted(c.items(), key=lambda kv: (-kv[1], kv[0]))
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(["combo", "frequency"])
        for combo, freq in rows_sorted:
            w.writerow([";".join(combo), freq])
        data = buf.getvalue().encode("utf-8")
        key = f"{output_prefix}/skills_{k}.csv"
        cli.put_object(key, data, content_type="text/csv; charset=utf-8")
        local_path = os.path.join(local_dir, f"skills_{k}.csv")
        with open(local_path, "wb") as f:
            f.write(data)
        written[k] = {
            "n_combinations": len(c),
            "total_frequency": int(sum(c.values())),
            "top": rows_sorted[0] if rows_sorted else None,
            "s3_key": key,
            "local_path": local_path,
        }
    return written

def object_manifest(cli: S3Client, key: str = VACANCIES_KEY) -> dict:
    info = cli.head_object(key)
    return {
        "backend": S3_BACKEND,
        "endpoint": S3_ENDPOINT if S3_BACKEND == "boto3" else LOCAL_FS_ROOT,
        "bucket": S3_BUCKET,
        "key": key,
        "exists": info is not None,
        "size_bytes": info.size if info else 0,
        "etag": info.etag if info else None,
        "last_modified": info.last_modified if info else None,
    }

def build_manifest(tool: str, cli: S3Client | None = None, extra: dict | None = None) -> dict:
    payload = {
        "tool": tool,
        "generated_at": date_utc(),
        "variant_role": VARIANT_ROLE,
        "filter_mode": VARIANT_FILTER_MODE,
        "environment": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
        },
        "input": object_manifest(cli) if cli is not None else None,
        "results": {},
    }
    if extra:
        payload.update(extra)
    return payload

def write_manifest(cli: S3Client, tool: str, payload: dict, local_dir: str) -> str:
    os.makedirs(local_dir, exist_ok=True)
    data = json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    key = f"manifests/{tool}.json"
    cli.put_object(key, data, content_type="application/json; charset=utf-8")
    local_path = os.path.join(local_dir, f"{tool}.json")
    with open(local_path, "wb") as f:
        f.write(data)
    return local_path

def sha256_file(path: str, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            block = f.read(chunk_size)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "selftest":
        assert is_devops_role({"role": "DevOps-инженер"})
        assert not is_devops_role({"role": "Java-разработчик"})
        assert is_devops_name({"name": "Senior DevOps Engineer"})
        assert is_devops_name({"name": "SRE / Platform Engineer"})
        assert not is_devops_name({"name": "Java backend developer"})
        assert matches_variant({"role": "DevOps-инженер"}, "role")
        assert not matches_variant({"role": "Программист", "name": "DevOps engineer"}, "role")
        assert matches_variant({"role": "Программист", "name": "DevOps engineer"}, "role_or_name")
        assert not is_devops({"role": "Java-разработчик", "name": "Senior Java Developer"})
        assert parse_skills("Linux;Docker;Kubernetes") == ["linux", "docker", "kubernetes"]
        assert parse_skills("Linux;Linux;docker") == ["linux", "docker"]
        assert parse_skills("Linux; Docker;Kubernetes") == ["linux", "docker", "kubernetes"]
        assert len(VACANCY_FIELDS) == 23
        assert VACANCY_FIELDS[0] == "source"
        assert VACANCY_FIELDS[11] == "role"
        assert VACANCY_FIELDS[20] == "key_skills"
        assert salary_to_rub({"salary_from": "100", "salary_to": "200", "currency": "RUR"}) == 150
        assert salary_to_rub({"salary_from": "abc", "currency": "RUR"}) is None
        row, reason = parse_vacancy_line("a,b")
        assert row is None and reason == "malformed"
        row, reason = parse_vacancy_line("")
        assert row is None and reason == "blank"
        print("common.py self-test OK")
    else:
        print("common.py — set S3_BACKEND=boto3 (default) to use RustFS, local for tests")
