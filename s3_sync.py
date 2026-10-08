#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""s3_sync.py — односторонняя синхронизация локальных папок с S3-хранилищем.

Скрипт рассчитан на S3-совместимые хранилища, в частности на Спринтбокс
(https://help.sprintbox.ru/main/s3-storage), а также MinIO, Ceph RGW и AWS S3.

Что делает:

* читает список папок из конфигурационного файла (TOML);
* загружает в бакет файлы, которых там нет или которые изменились;
* удаляет из бакета объекты, которых больше нет в локальной папке
  (в пределах указанного префикса — чужие объекты не трогаются);
* умеет показать план действий (--dry-run) и проверить доступ к бакету (--check).

Данные для подключения берутся в таком порядке (первое найденное побеждает):

1. аргументы командной строки (--access-key / --secret-key);
2. переменные окружения: S3_SYNC_ACCESS_KEY / S3_SYNC_SECRET_KEY,
   а также стандартные AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY;
3. секция [storage] конфигурационного файла;
4. профиль из ~/.aws/credentials (ключ profile = "имя профиля").

Подробности — в README.md рядом со скриптом.
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import logging
import os
import posixpath
import re
import stat as stat_module
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

try:  # Python 3.11+
    import tomllib as _toml
except ModuleNotFoundError:  # pragma: no cover - для Python 3.9/3.10
    try:
        import tomli as _toml  # type: ignore[no-redef]
    except ModuleNotFoundError:
        _toml = None  # type: ignore[assignment]

PROG = "s3_sync"
LOG = logging.getLogger(PROG)

DEFAULT_ENDPOINT = "https://s3.spb.sprinthost.ru"
DEFAULT_REGION = "spb"
DEFAULT_EXCLUDES = (".DS_Store", "Thumbs.db", "desktop.ini", "*.tmp", "*.swp", "*~")
ENV_PREFIX = "S3_SYNC_"

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2

STORAGE_KEYS = {
    "endpoint_url",
    "region",
    "bucket",
    "addressing_style",
    "signature_version",
    "profile",
    "credentials_file",
    "access_key",
    "secret_key",
    "session_token",
    "delete",
    "exclude",
    "max_deletes",
    "multipart_threshold",
    "multipart_chunksize",
}
FOLDER_KEYS = {"name", "local", "prefix", "delete", "exclude"}

HINTS = {
    "InvalidAccessKeyId": "Access Key не найден. Проверьте ключ из панели управления Спринтбокс.",
    "SignatureDoesNotMatch": "Неверный Secret Access Key (или он скопирован с лишними символами).",
    "AccessDenied": "Доступ запрещён: у ключа нет прав на этот бакет или на эту операцию.",
    "AllAccessDisabled": "Доступ к бакету отключён администратором хранилища.",
    "NoSuchBucket": "Бакета с таким именем нет. Проверьте поле bucket.",
    "NoSuchKey": "Объект не найден.",
    "AuthorizationHeaderMalformed": "Хранилище ожидает другой регион. Для Спринтбокса это spb.",
    "PermanentRedirect": "Запрос ушёл не в тот регион/endpoint. Проверьте endpoint_url и region.",
    "RequestTimeTooSkewed": "Часы машины расходятся с сервером. Включите синхронизацию времени (NTP).",
    "NotImplemented": "Операция не поддерживается хранилищем. Попробуйте другой addressing_style или signature_version.",
    "InvalidBucketName": "Имя бакета недопустимо (обычно вида s3-123456).",
    "SlowDown": "Хранилище просит снизить темп. Уменьшите --jobs и повторите.",
}

# У HeadBucket ответ приходит без тела, поэтому кода ошибки нет — только статус.
STATUS_HINTS = {
    301: "запрос ушёл не в тот регион: проверьте region (для Спринтбокса это spb)",
    400: "хранилище отклонило запрос: проверьте endpoint_url, bucket и addressing_style",
    403: "доступ запрещён: проверьте Access Key / Secret Access Key и права ключа на бакет",
    404: "бакета с таким именем нет либо у ключа нет к нему доступа — проверьте bucket и права ключа",
}


class ConfigError(Exception):
    """Ошибка в конфигурации или в данных для подключения."""


class SyncError(Exception):
    """Ошибка во время синхронизации."""


# ---------------------------------------------------------------------------
# вспомогательные функции
# ---------------------------------------------------------------------------


def human_size(num: float) -> str:
    """1.5 MiB / 900 B — для читаемых сообщений."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(num) < 1024.0 or unit == "TiB":
            return f"{num:.0f} {unit}" if unit == "B" else f"{num:.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TiB"


_SIZE_RE = re.compile(r"^\s*(\d+(?:[.,]\d+)?)\s*([kmgt]?)(?:i?b)?\s*$", re.IGNORECASE)


def parse_size(value: Any, *, key: str) -> int:
    """'8M', '64 MiB', 1048576 -> число байт."""
    if isinstance(value, bool):
        raise ConfigError(f"{key}: ожидается размер в байтах, получено {value!r}")
    if isinstance(value, int):
        return value
    match = _SIZE_RE.match(str(value))
    if not match:
        raise ConfigError(f"{key}: не удалось разобрать размер {value!r} (примеры: 8388608, '8M', '64 MiB')")
    number = float(match.group(1).replace(",", "."))
    multiplier = {"": 1, "k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}[match.group(2).lower()]
    return int(number * multiplier)


def md5_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def mask_secret(value: str | None) -> str:
    if not value:
        return "не задан"
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}…{value[-3:]}"


def normalize_prefix(prefix: Any) -> str:
    """'backups//site/' -> 'backups/site' (без ведущего и хвостового слэша)."""
    parts = [part for part in str(prefix or "").replace("\\", "/").split("/") if part and part != "."]
    if any(part == ".." for part in parts):
        raise ConfigError(f"префикс {prefix!r} не должен содержать '..'")
    return "/".join(parts)


def is_excluded(relpath: str, patterns: Sequence[str]) -> bool:
    """Проверка относительного пути (posix) по списку шаблонов.

    Шаблон без '/' сравнивается с именем файла на любом уровне ('*.tmp'),
    шаблон с '/' — с полным относительным путём ('cache/**', 'src/tmp/*.log').
    """
    for pattern in patterns:
        pat = str(pattern).strip().replace("\\", "/").lstrip("/")
        if not pat:
            continue
        if pat.endswith("/**"):
            base = pat[:-3].rstrip("/")
            if relpath == base or relpath.startswith(base + "/"):
                return True
            continue
        if "/" in pat:
            if fnmatch.fnmatchcase(relpath, pat):
                return True
        elif fnmatch.fnmatchcase(posixpath.basename(relpath), pat) or fnmatch.fnmatchcase(relpath, pat):
            return True
    return False


def _as_list(value: Any, *, key: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value]
    raise ConfigError(f"{key}: ожидается строка или список строк, получено {type(value).__name__}")


def _as_bool(value: Any, *, key: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on", "да"}:
            return True
        if lowered in {"0", "false", "no", "off", "нет"}:
            return False
    raise ConfigError(f"{key}: ожидается true/false, получено {value!r}")


# ---------------------------------------------------------------------------
# модель настроек
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Auth:
    access_key: str | None = None
    secret_key: str | None = None
    session_token: str | None = None
    profile: str | None = None
    credentials_file: str | None = None
    access_key_source: str = "не задан"
    secret_key_source: str = "не задан"
    profile_source: str = "не задан"

    @property
    def has_explicit_keys(self) -> bool:
        return bool(self.access_key and self.secret_key)


@dataclass(frozen=True)
class Folder:
    name: str
    local: Path
    prefix: str
    delete: bool
    excludes: tuple[str, ...]


@dataclass
class Settings:
    endpoint_url: str
    region: str
    bucket: str
    addressing_style: str
    signature_version: str
    auth: Auth
    folders: tuple[Folder, ...]
    max_deletes: int
    multipart_threshold: int
    multipart_chunksize: int
    config_path: Path


@dataclass(frozen=True)
class LocalFile:
    relpath: str
    path: Path
    size: int
    mtime: float


@dataclass(frozen=True)
class RemoteObject:
    key: str
    size: int
    etag: str
    last_modified: datetime


@dataclass
class PlannedUpload:
    folder: Folder
    local: LocalFile
    key: str
    reason: str


@dataclass
class PlannedDelete:
    folder: Folder
    key: str


@dataclass
class Stats:
    uploaded: int = 0
    deleted: int = 0
    skipped: int = 0
    kept: int = 0
    bytes_uploaded: int = 0
    errors: int = 0


# ---------------------------------------------------------------------------
# чтение конфигурации
# ---------------------------------------------------------------------------


def _pick(cli_value: Any, env_names: Sequence[str], cfg_value: Any, cfg_label: str) -> tuple[Any, str]:
    """Приоритет: аргумент командной строки → переменная окружения → конфиг."""
    if cli_value is not None and cli_value != "":
        return cli_value, "аргумент командной строки"
    for name in env_names:
        value = os.environ.get(name)
        if value:
            return value, f"переменная окружения {name}"
    if cfg_value is not None and cfg_value != "":
        return cfg_value, cfg_label
    return None, "не задан"


def load_settings(args: argparse.Namespace) -> Settings:
    if _toml is None:
        raise ConfigError(
            "не найден модуль разбора TOML: для Python 3.9/3.10 установите tomli (pip install tomli); "
            "в Python 3.11+ tomllib входит в стандартную библиотеку"
        )

    config_path = Path(args.config).expanduser()
    try:
        raw_bytes = config_path.read_bytes()
    except OSError as exc:
        raise ConfigError(f"не удалось прочитать конфигурационный файл {config_path}: {exc}") from exc
    try:
        # utf-8-sig — чтобы конфиг, сохранённый «Блокнотом» с BOM, тоже читался.
        document = _toml.loads(raw_bytes.decode("utf-8-sig"))
    except UnicodeDecodeError as exc:
        raise ConfigError(f"{config_path}: файл должен быть в UTF-8 ({exc})") from exc
    except Exception as exc:  # tomllib.TOMLDecodeError
        raise ConfigError(f"{config_path}: синтаксическая ошибка TOML — {exc}") from exc

    if not isinstance(document, dict):
        raise ConfigError(f"{config_path}: ожидается TOML-документ с секцией [storage]")

    storage = document.get("storage")
    if not isinstance(storage, dict):
        raise ConfigError(f"{config_path}: отсутствует обязательная секция [storage]")

    unknown = sorted(set(storage) - STORAGE_KEYS)
    if unknown:
        LOG.warning("в секции [storage] неизвестные ключи: %s (игнорируются)", ", ".join(unknown))
    top_level = sorted(set(document) - {"storage", "folders", "defaults"})
    if top_level:
        LOG.warning("неизвестные секции конфигурации: %s (игнорируются)", ", ".join(top_level))

    defaults = document.get("defaults") or {}
    if not isinstance(defaults, dict):
        raise ConfigError(f"{config_path}: секция [defaults] должна быть таблицей")
    unknown = sorted(set(defaults) - {"delete", "exclude"})
    if unknown:
        LOG.warning("в секции [defaults] неизвестные ключи: %s (игнорируются)", ", ".join(unknown))

    delete_default = _as_bool(defaults.get("delete", True), key="defaults.delete")
    exclude_default = tuple(_as_list(defaults.get("exclude"), key="defaults.exclude"))

    endpoint_url, _ = _pick(args.endpoint_url, [f"{ENV_PREFIX}ENDPOINT_URL"], storage.get("endpoint_url"), "конфигурационный файл")
    endpoint_url = str(endpoint_url or DEFAULT_ENDPOINT).strip().rstrip("/")
    if not endpoint_url.startswith(("http://", "https://")):
        raise ConfigError(f"endpoint_url должен начинаться с http:// или https:// (получено {endpoint_url!r})")

    region, _ = _pick(args.region, [f"{ENV_PREFIX}REGION", "AWS_DEFAULT_REGION"], storage.get("region"), "конфигурационный файл")
    region = str(region or DEFAULT_REGION)

    bucket, _ = _pick(args.bucket, [f"{ENV_PREFIX}BUCKET"], storage.get("bucket"), "конфигурационный файл")
    if not bucket:
        raise ConfigError('не задан бакет: укажите bucket = "s3-123456" в секции [storage] или --bucket')
    bucket = str(bucket).strip()

    addressing_style = str(storage.get("addressing_style") or "path").strip().lower()
    if addressing_style not in {"path", "virtual", "auto"}:
        raise ConfigError("addressing_style: допустимы только path, virtual или auto")
    signature_version = str(storage.get("signature_version") or "s3v4").strip()

    # --no-env-credentials отключает и стандартные AWS_*, и собственные S3_SYNC_*:
    # иначе ключи из окружения по-прежнему перебивали бы ключи из конфига, и флаг
    # не защищал бы от чужих AWS_* (см. тест test_16).
    def credential_env(*names: str) -> list[str]:
        return [] if args.no_env_credentials else list(names)

    access_key, access_key_source = _pick(
        args.access_key,
        credential_env(f"{ENV_PREFIX}ACCESS_KEY", "AWS_ACCESS_KEY_ID"),
        storage.get("access_key"),
        "конфигурационный файл (не рекомендуется)",
    )
    secret_key, secret_key_source = _pick(
        args.secret_key,
        credential_env(f"{ENV_PREFIX}SECRET_KEY", "AWS_SECRET_ACCESS_KEY"),
        storage.get("secret_key"),
        "конфигурационный файл (не рекомендуется)",
    )
    session_token, _ = _pick(
        args.session_token,
        credential_env(f"{ENV_PREFIX}SESSION_TOKEN", "AWS_SESSION_TOKEN"),
        storage.get("session_token"),
        "конфигурационный файл",
    )
    profile, profile_source = _pick(
        args.profile,
        credential_env(f"{ENV_PREFIX}PROFILE", "AWS_PROFILE"),
        storage.get("profile"),
        "конфигурационный файл",
    )
    credentials_file, _ = _pick(
        args.credentials_file,
        credential_env(f"{ENV_PREFIX}CREDENTIALS_FILE", "AWS_SHARED_CREDENTIALS_FILE"),
        storage.get("credentials_file"),
        "конфигурационный файл",
    )

    auth = Auth(
        access_key=str(access_key) if access_key else None,
        secret_key=str(secret_key) if secret_key else None,
        session_token=str(session_token) if session_token else None,
        profile=str(profile) if profile else None,
        credentials_file=os.path.expanduser(str(credentials_file)) if credentials_file else None,
        access_key_source=access_key_source,
        secret_key_source=secret_key_source,
        profile_source=profile_source,
    )

    if storage.get("access_key") or storage.get("secret_key"):
        LOG.warning(
            "ключи доступа хранятся в конфигурационном файле %s. Это допустимо, но лучше держать их "
            "в переменных окружения или в ~/.aws/credentials с правами 600",
            config_path,
        )
        try:
            if os.name != "nt" and config_path.stat().st_mode & 0o077:
                LOG.warning("файл %s доступен другим пользователям: chmod 600 %s", config_path, config_path)
        except OSError:
            pass

    if auth.credentials_file and not Path(auth.credentials_file).is_file():
        raise ConfigError(f"файл с ключами не найден: {auth.credentials_file}")

    raw_folders = document.get("folders")
    if raw_folders is None:
        raw_folders = []
    if not isinstance(raw_folders, list):
        raise ConfigError(f"{config_path}: folders должен быть массивом таблиц ([[folders]])")

    base_dir = config_path.parent.resolve()
    folders: list[Folder] = []
    for index, item in enumerate(raw_folders, start=1):
        if not isinstance(item, dict):
            raise ConfigError(f"{config_path}: folders[{index}] должен быть таблицей [[folders]]")
        unknown = sorted(set(item) - FOLDER_KEYS)
        if unknown:
            LOG.warning("folders[%d] (%s): неизвестные ключи: %s", index, item.get("local", "?"), ", ".join(unknown))
        local_raw = item.get("local")
        if not local_raw:
            raise ConfigError(f"{config_path}: folders[{index}]: не задан local")
        local_value = os.path.expandvars(os.path.expanduser(str(local_raw)))
        local_path = Path(local_value)
        if not local_path.is_absolute():
            local_path = base_dir / local_path
        local_path = Path(os.path.normpath(str(local_path)))

        prefix = normalize_prefix(item.get("prefix", ""))
        name = str(item.get("name") or prefix or local_path.name or f"folder-{index}")
        delete = _as_bool(item.get("delete", delete_default), key=f"folders[{index}].delete")
        excludes = _as_list(item.get("exclude"), key=f"folders[{index}].exclude")
        merged = tuple(dict.fromkeys((*exclude_default, *excludes))) if excludes else exclude_default
        folders.append(
            Folder(name=name, local=local_path, prefix=prefix, delete=delete, excludes=merged)
        )

    if not folders and not getattr(args, "check", False):
        raise ConfigError(f"{config_path}: не описано ни одной папки (нужна хотя бы одна секция [[folders]])")

    # Дубликаты имён и вложенные префиксы опасны: удаление в «внешнем» префиксе
    # снесло бы объекты «внутреннего».
    seen_names: dict[str, int] = {}
    for folder in folders:
        seen_names[folder.name] = seen_names.get(folder.name, 0) + 1
    duplicates = [name for name, count in seen_names.items() if count > 1]
    if duplicates:
        raise ConfigError(f"имена папок должны быть уникальными, повторы: {', '.join(sorted(duplicates))}")

    ordered = sorted(folders, key=lambda folder: folder.prefix)
    for outer, inner in zip(ordered, ordered[1:]):
        if outer.prefix == inner.prefix:
            raise ConfigError(
                f"папки «{outer.name}» и «{inner.name}» используют один префикс {outer.prefix!r}; "
                "объедините их или задайте разные префиксы"
            )
        if not outer.prefix:
            # Пустой префикс — это весь бакет, поэтому любой непустой префикс лежит
            # «внутри» него: папка без префикса считала бы чужими объектами всё, что
            # не совпало с её локальными файлами, и удаляла бы их.
            raise ConfigError(
                f"папка «{outer.name}» без префикса синхронизирует весь бакет, а префикс {inner.prefix!r} "
                f"(папка «{inner.name}») вложен в него; при синхронизации с удалением они будут мешать "
                "друг другу — задайте первой папке непустой префикс или уберите вторую папку"
            )
        if inner.prefix.startswith(outer.prefix + "/"):
            raise ConfigError(
                f"префикс {inner.prefix!r} (папка «{inner.name}») вложен в префикс {outer.prefix!r} "
                f"(папка «{outer.name}»); при синхронизации с удалением они будут мешать друг другу — "
                "задайте непересекающиеся префиксы"
            )

    max_deletes = storage.get("max_deletes", 0)
    try:
        max_deletes = int(max_deletes)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"max_deletes: ожидается целое число, получено {max_deletes!r}") from exc
    if args.max_delete is not None:
        max_deletes = args.max_delete

    return Settings(
        endpoint_url=endpoint_url,
        region=region,
        bucket=bucket,
        addressing_style=addressing_style,
        signature_version=signature_version,
        auth=auth,
        folders=tuple(folders),
        max_deletes=max_deletes,
        multipart_threshold=parse_size(storage.get("multipart_threshold", "64M"), key="multipart_threshold"),
        multipart_chunksize=parse_size(storage.get("multipart_chunksize", "16M"), key="multipart_chunksize"),
        config_path=config_path,
    )


# ---------------------------------------------------------------------------
# клиент S3
# ---------------------------------------------------------------------------


def build_client(settings: Settings, args: argparse.Namespace) -> Any:
    try:
        import boto3
        from botocore.config import Config as BotoConfig
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise ConfigError(
            "не установлен boto3. Установите зависимости: python -m pip install -r requirements.txt"
        ) from exc

    auth = settings.auth
    if auth.credentials_file:
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = auth.credentials_file
    if auth.profile:
        os.environ.setdefault("AWS_PROFILE", auth.profile)
    if args.no_env_credentials:
        # Чтобы boto3 не подхватил чужие AWS_* из окружения.
        for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
            os.environ.pop(name, None)

    session_kwargs: dict[str, Any] = {"region_name": settings.region}
    if auth.has_explicit_keys:
        session_kwargs["aws_access_key_id"] = auth.access_key
        session_kwargs["aws_secret_access_key"] = auth.secret_key
        if auth.session_token:
            session_kwargs["aws_session_token"] = auth.session_token
    elif auth.profile:
        session_kwargs["profile_name"] = auth.profile

    session = boto3.session.Session(**session_kwargs)

    extra: dict[str, Any] = {}
    defaults = getattr(BotoConfig, "OPTION_DEFAULTS", {})
    # Свежие botocore по умолчанию шлют контрольные суммы CRC32 (aws-chunked),
    # которые поддерживают не все S3-совместимые хранилища.
    if "request_checksum_calculation" in defaults:
        extra["request_checksum_calculation"] = "when_required"
    if "response_checksum_validation" in defaults:
        extra["response_checksum_validation"] = "when_required"

    boto_config = BotoConfig(
        signature_version=settings.signature_version,
        s3={"addressing_style": settings.addressing_style},
        retries={"max_attempts": args.retries, "mode": "standard"},
        connect_timeout=args.connect_timeout,
        read_timeout=args.read_timeout,
        max_pool_connections=max(10, args.jobs * 2),
        **extra,
    )
    return session.client("s3", endpoint_url=settings.endpoint_url, config=boto_config)


def explain_client_error(exc: Exception, settings: Settings) -> str:
    """Превращает ошибку botocore в подсказку на русском."""
    from botocore.exceptions import (  # импорт внутри функции: boto3 нужен не всегда
        ClientError,
        EndpointConnectionError,
        NoCredentialsError,
        PartialCredentialsError,
        SSLError,
    )

    if isinstance(exc, (NoCredentialsError, PartialCredentialsError)):
        return (
            "не найдены ключи доступа. Задайте S3_SYNC_ACCESS_KEY и S3_SYNC_SECRET_KEY, "
            "либо профиль в ~/.aws/credentials, либо access_key/secret_key в конфигурационном файле"
        )
    if isinstance(exc, EndpointConnectionError):
        return f"не удалось подключиться к {settings.endpoint_url}: проверьте адрес и доступ в сеть"
    if isinstance(exc, SSLError):
        return f"ошибка TLS при обращении к {settings.endpoint_url}: {exc}"
    if isinstance(exc, ClientError):
        error = exc.response.get("Error", {})
        code = str(error.get("Code", ""))
        message = str(error.get("Message", "")).strip()
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", "?")
        hint = HINTS.get(code)
        if hint is None and isinstance(status, int) and (not code or code.isdigit()):
            hint = STATUS_HINTS.get(status)
        text = f"HTTP {status}, код {code or '?'}"
        if message:
            text += f": {message}"
        if hint:
            text += f"\n    подсказка: {hint}"
        return text
    return f"{type(exc).__name__}: {exc}"


# ---------------------------------------------------------------------------
# обход локальных папок и хранилища
# ---------------------------------------------------------------------------


def collect_local(
    root: Path,
    excludes: Sequence[str],
    *,
    follow_symlinks: bool = False,
    unreadable: list[str] | None = None,
) -> dict[str, LocalFile]:
    """Обходит локальную папку и возвращает карту «относительный путь → файл».

    `unreadable` — необязательный накопитель относительных путей, которые не
    удалось прочитать (файл или каталог). Вызывающий код обязан считать такой
    обход неполным: нечитаемый файл просто отсутствует в результате и выглядит
    как удалённый локально, поэтому по нему нельзя удалять объект из хранилища.
    """
    files: dict[str, LocalFile] = {}
    if not root.is_dir():
        raise SyncError(f"локальная папка не найдена: {root}")

    def report_unreadable(target: str | None, exc: OSError) -> None:
        rel = ""
        if target:
            try:
                rel = Path(target).relative_to(root).as_posix()
            except ValueError:  # путь вне root — показываем как есть
                rel = str(target)
        if unreadable is not None and rel:
            unreadable.append(rel)
        LOG.warning("не удалось прочитать %s: %s", rel or target, exc)

    def on_walk_error(exc: OSError) -> None:
        # os.walk по умолчанию молча пропускает нечитаемый каталог целиком.
        report_unreadable(getattr(exc, "filename", None), exc)

    for dirpath, dirnames, filenames in os.walk(root, followlinks=follow_symlinks, onerror=on_walk_error):
        current = Path(dirpath)
        rel_dir = "" if current == root else current.relative_to(root).as_posix()

        kept_dirs = []
        for name in sorted(dirnames):
            rel = f"{rel_dir}/{name}" if rel_dir else name
            if is_excluded(rel, excludes):
                LOG.debug("каталог исключён: %s", rel)
                continue
            kept_dirs.append(name)
        dirnames[:] = kept_dirs

        for name in sorted(filenames):
            rel = f"{rel_dir}/{name}" if rel_dir else name
            if is_excluded(rel, excludes):
                LOG.debug("файл исключён: %s", rel)
                continue
            full = current / name
            try:
                st = full.stat() if follow_symlinks else full.lstat()
            except OSError as exc:
                report_unreadable(str(full), exc)
                continue
            if not follow_symlinks and stat_module.S_ISLNK(st.st_mode):
                LOG.debug("символическая ссылка пропущена: %s", rel)
                continue
            if not stat_module.S_ISREG(st.st_mode):
                LOG.debug("не обычный файл, пропущен: %s", rel)
                continue
            files[rel] = LocalFile(relpath=rel, path=full, size=st.st_size, mtime=st.st_mtime)
    return files


def list_remote(client: Any, bucket: str, prefix: str) -> list[RemoteObject]:
    paginator = client.get_paginator("list_objects_v2")
    kwargs: dict[str, Any] = {"Bucket": bucket}
    if prefix:
        kwargs["Prefix"] = prefix + "/"
    objects: list[RemoteObject] = []
    for page in paginator.paginate(**kwargs):
        for item in page.get("Contents") or []:
            key = str(item["Key"])
            if key.endswith("/"):
                continue  # маркер «папки»
            objects.append(
                RemoteObject(
                    key=key,
                    size=int(item.get("Size", 0)),
                    etag=str(item.get("ETag", "")).strip('"'),
                    last_modified=item["LastModified"],
                )
            )
    return objects


def key_for(folder: Folder, relpath: str) -> str:
    return f"{folder.prefix}/{relpath}" if folder.prefix else relpath


# ---------------------------------------------------------------------------
# планирование
# ---------------------------------------------------------------------------


def plan_folder(
    folder: Folder,
    local_files: dict[str, LocalFile],
    remote_objects: Iterable[RemoteObject],
    *,
    force_upload: bool,
    checksum: bool,
    tolerance: float,
) -> tuple[list[PlannedUpload], list[PlannedDelete], int, int]:
    """Возвращает (загрузки, удаления, пропущено, оставлено в хранилище)."""
    remote_by_key = {obj.key: obj for obj in remote_objects}
    uploads: list[PlannedUpload] = []
    skipped = 0

    for rel in sorted(local_files):
        local = local_files[rel]
        key = key_for(folder, rel)
        remote = remote_by_key.get(key)
        reason: str | None = None

        if force_upload:
            reason = "принудительная загрузка"
        elif remote is None:
            reason = "нет в хранилище"
        elif remote.size != local.size:
            reason = f"размер изменился ({human_size(remote.size)} → {human_size(local.size)})"
        else:
            md5_checked = False
            if checksum and remote.etag and "-" not in remote.etag:
                md5_checked = True
                try:
                    if md5_file(local.path) != remote.etag.lower():
                        reason = "md5 отличается"
                except OSError as exc:
                    LOG.warning("не удалось посчитать md5 для %s: %s", rel, exc)
                    md5_checked = False
            if reason is None and not md5_checked and local.mtime > remote.last_modified.timestamp() + tolerance:
                reason = "локальный файл новее"

        if reason:
            uploads.append(PlannedUpload(folder=folder, local=local, key=key, reason=reason))
        else:
            skipped += 1

    local_keys = {key_for(folder, rel) for rel in local_files}
    stale = [obj for key, obj in remote_by_key.items() if key not in local_keys]
    deletes = [PlannedDelete(folder=folder, key=obj.key) for obj in stale] if folder.delete else []
    kept = 0 if folder.delete else len(stale)
    return uploads, deletes, skipped, kept


# ---------------------------------------------------------------------------
# выполнение
# ---------------------------------------------------------------------------


def _run_tasks(tasks: Sequence[Any], worker: Callable[[Any], None], jobs: int, label: str) -> list[tuple[Any, Exception]]:
    errors: list[tuple[Any, Exception]] = []
    if not tasks:
        return errors
    if jobs <= 1:
        for task in tasks:
            try:
                worker(task)
            except Exception as exc:  # noqa: BLE001 - нужен отчёт по каждому файлу
                errors.append((task, exc))
                LOG.error("%s: ошибка — %s", label, exc)
        return errors

    with ThreadPoolExecutor(max_workers=jobs, thread_name_prefix="s3sync") as pool:
        futures = {pool.submit(worker, task): task for task in tasks}
        for future in as_completed(futures):
            task = futures[future]
            try:
                future.result()
            except Exception as exc:  # noqa: BLE001
                errors.append((task, exc))
                LOG.error("%s: ошибка — %s", label, exc)
    return errors


def execute_uploads(client: Any, settings: Settings, uploads: Sequence[PlannedUpload], args: argparse.Namespace) -> Stats:
    stats = Stats()
    if not uploads:
        return stats

    from boto3.s3.transfer import TransferConfig

    transfer_config = TransferConfig(
        multipart_threshold=settings.multipart_threshold,
        multipart_chunksize=settings.multipart_chunksize,
        max_concurrency=max(1, args.jobs),
        use_threads=args.jobs > 1,
    )
    lock = threading.Lock()

    def upload(item: PlannedUpload) -> None:
        client.upload_file(
            Filename=str(item.local.path),
            Bucket=settings.bucket,
            Key=item.key,
            Config=transfer_config,
        )
        with lock:
            stats.uploaded += 1
            stats.bytes_uploaded += item.local.size
        LOG.info(
            "загружен  %s -> s3://%s/%s (%s, %s)",
            item.local.relpath,
            settings.bucket,
            item.key,
            human_size(item.local.size),
            item.reason,
        )

    errors = _run_tasks(uploads, upload, args.jobs, "загрузка")
    stats.errors += len(errors)
    return stats


def execute_deletes(client: Any, settings: Settings, deletes: Sequence[PlannedDelete]) -> Stats:
    stats = Stats()
    if not deletes:
        return stats

    for start in range(0, len(deletes), 1000):
        chunk = list(deletes[start : start + 1000])
        if len(chunk) == 1:
            item = chunk[0]
            try:
                client.delete_object(Bucket=settings.bucket, Key=item.key)
            except Exception as exc:  # noqa: BLE001
                stats.errors += 1
                LOG.error("не удалось удалить s3://%s/%s — %s", settings.bucket, item.key, exc)
                continue
            stats.deleted += 1
            LOG.info("удалён    s3://%s/%s (нет локально)", settings.bucket, item.key)
            continue

        response = client.delete_objects(
            Bucket=settings.bucket,
            Delete={"Objects": [{"Key": item.key} for item in chunk], "Quiet": True},
        )
        errors = response.get("Errors") or []
        failed = {str(error.get("Key")) for error in errors}
        for item in chunk:
            if item.key in failed:
                continue
            stats.deleted += 1
            LOG.info("удалён    s3://%s/%s (нет локально)", settings.bucket, item.key)
        for error in errors:
            stats.errors += 1
            LOG.error("не удалось удалить s3://%s/%s — %s", settings.bucket, error.get("Key"), error.get("Message"))
    return stats


def run_sync(
    client: Any, settings: Settings, args: argparse.Namespace, folders: Sequence[Folder]
) -> tuple[Stats, list[str]]:
    total = Stats()
    problems: list[str] = []
    plans: list[tuple[Folder, list[PlannedUpload], list[PlannedDelete], int, int]] = []

    for folder in folders:
        LOG.info("папка «%s»: %s -> s3://%s/%s", folder.name, folder.local, settings.bucket, folder.prefix)
        unreadable: list[str] = []
        try:
            local_files = collect_local(
                folder.local,
                folder.excludes,
                follow_symlinks=args.follow_symlinks,
                unreadable=unreadable,
            )
        except SyncError as exc:
            problems.append(str(exc))
            LOG.error("%s", exc)
            continue

        remote_objects = list_remote(client, settings.bucket, folder.prefix)
        uploads, deletes, skipped, kept = plan_folder(
            folder,
            local_files,
            remote_objects,
            force_upload=args.force_upload,
            checksum=args.checksum,
            tolerance=args.tolerance,
        )

        if unreadable:
            preview = ", ".join(sorted(unreadable)[:3])
            suffix = f" и ещё {len(unreadable) - 3}" if len(unreadable) > 3 else ""
            where = f"папка «{folder.name}»: не удалось прочитать {len(unreadable)} запись(ей) при обходе ({preview}{suffix})"
            if folder.delete and not args.force:
                # Нечитаемый файл выглядит как удалённый локально: без этой проверки
                # его объект сносился бы из хранилища по неполному обходу.
                message = (
                    f"{where}. Удаление отменено — по неполному обходу удалять нельзя. "
                    "Разберитесь с правами доступа или запустите с --force"
                )
                problems.append(message)
                LOG.error("%s", message)
                kept += len(deletes)
                deletes = []
            elif folder.delete:
                LOG.warning("%s; обход неполный, но --force отключает предохранитель — удаление выполнено", where)
            else:
                LOG.warning("%s; обход неполный, часть файлов не попала в план загрузки", where)
        elif not local_files and remote_objects and folder.delete and not args.force:
            message = (
                f"папка «{folder.name}» ({folder.local}) пуста, а в хранилище {len(remote_objects)} объектов. "
                "Удаление отменено — проверьте пути; чтобы удалить всё равно, запустите с --force"
            )
            problems.append(message)
            LOG.error("%s", message)
            deletes = []
            kept = len(remote_objects)

        if not folder.delete and deletes:
            kept += len(deletes)
            deletes = []

        LOG.info(
            "папка «%s»: локально %d файлов, в хранилище %d объектов, к загрузке %d, к удалению %d, без изменений %d",
            folder.name,
            len(local_files),
            len(remote_objects),
            len(uploads),
            len(deletes),
            skipped,
        )
        total.skipped += skipped
        total.kept += kept
        plans.append((folder, uploads, deletes, skipped, kept))

    planned_deletes = sum(len(deletes) for _, _, deletes, _, _ in plans)
    if settings.max_deletes and planned_deletes > settings.max_deletes and not args.force:
        message = (
            f"к удалению {planned_deletes} объектов, это больше max_deletes = {settings.max_deletes}. "
            "Ничего не удалено: проверьте пути и при необходимости увеличьте max_deletes или запустите с --force"
        )
        problems.append(message)
        LOG.error("%s", message)
        plans = [(folder, uploads, [], skipped, kept + len(deletes)) for folder, uploads, deletes, skipped, kept in plans]
        total.kept += planned_deletes
        planned_deletes = 0

    all_uploads = [item for _, uploads, _, _, _ in plans for item in uploads]
    all_deletes = [item for _, _, deletes, _, _ in plans for item in deletes]

    if args.dry_run:
        for item in all_uploads:
            LOG.info("[dry-run] загрузил бы  %s -> s3://%s/%s (%s)", item.local.relpath, settings.bucket, item.key, item.reason)
        for item in all_deletes:
            LOG.info("[dry-run] удалил бы    s3://%s/%s", settings.bucket, item.key)
        LOG.info(
            "режим --dry-run: изменения не вносились (к загрузке %d, к удалению %d, без изменений %d)",
            len(all_uploads),
            len(all_deletes),
            total.skipped,
        )
        return total, problems

    upload_stats = execute_uploads(client, settings, all_uploads, args)
    total.uploaded += upload_stats.uploaded
    total.bytes_uploaded += upload_stats.bytes_uploaded
    total.errors += upload_stats.errors

    delete_stats = execute_deletes(client, settings, all_deletes)
    total.deleted += delete_stats.deleted
    total.errors += delete_stats.errors

    return total, problems


# ---------------------------------------------------------------------------
# режим проверки подключения
# ---------------------------------------------------------------------------


def run_check(client: Any, settings: Settings, args: argparse.Namespace) -> int:
    from botocore.exceptions import ClientError

    LOG.info("Конфигурация:      %s", settings.config_path)
    LOG.info("Endpoint:          %s", settings.endpoint_url)
    LOG.info("Регион:            %s", settings.region)
    LOG.info("Бакет:             %s", settings.bucket)
    LOG.info("Стиль адресации:   %s", settings.addressing_style)
    LOG.info("Версия подписи:    %s", settings.signature_version)
    LOG.info("Access Key:        %s (%s)", mask_secret(settings.auth.access_key), settings.auth.access_key_source)
    LOG.info(
        "Secret Key:        %s (%s)",
        "***" if settings.auth.secret_key else "не задан",
        settings.auth.secret_key_source,
    )
    if settings.auth.profile:
        LOG.info("Профиль:           %s (%s)", settings.auth.profile, settings.auth.profile_source)
    if settings.auth.credentials_file:
        LOG.info("Файл с ключами:    %s", settings.auth.credentials_file)

    ok = True
    try:
        client.head_bucket(Bucket=settings.bucket)
        LOG.info("HEAD бакета:       OK — бакет существует и доступен")
    except ClientError as exc:
        ok = False
        LOG.error("HEAD бакета:       ОШИБКА — %s", explain_client_error(exc, settings))
    except Exception as exc:  # noqa: BLE001
        ok = False
        LOG.error("HEAD бакета:       ОШИБКА — %s", explain_client_error(exc, settings))

    if ok:
        try:
            response = client.list_objects_v2(Bucket=settings.bucket, MaxKeys=5)
            more = " (список не полный)" if response.get("IsTruncated") else ""
            LOG.info("Список объектов:   OK — видно объектов: %d%s", response.get("KeyCount", 0), more)
        except Exception as exc:  # noqa: BLE001
            ok = False
            LOG.error("Список объектов:   ОШИБКА — %s", explain_client_error(exc, settings))

    if args.check_write:
        probe_key = f"{ENV_PREFIX.lower()}probe-{int(time.time())}-{os.getpid()}.txt"
        try:
            client.put_object(Bucket=settings.bucket, Key=probe_key, Body=b"s3_sync write test")
            LOG.info("Запись объекта:    OK — создан %s", probe_key)
            client.delete_object(Bucket=settings.bucket, Key=probe_key)
            LOG.info("Удаление объекта:  OK — %s удалён", probe_key)
        except Exception as exc:  # noqa: BLE001
            ok = False
            LOG.error("Запись в бакет:    ОШИБКА — %s", explain_client_error(exc, settings))

    LOG.info("Итог проверки:     %s", "подключение работает" if ok else "есть проблемы (см. выше)")
    return EXIT_OK if ok else EXIT_ERROR


# ---------------------------------------------------------------------------
# командная строка
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROG,
        description="Синхронизация папок из конфигурационного файла с S3-хранилищем (с удалением лишнего).",
        epilog=(
            "Примеры:\n"
            f"  {PROG} --config s3-sync.conf --check --check-write\n"
            f"  {PROG} --config s3-sync.conf --dry-run --verbose\n"
            f"  {PROG} --config s3-sync.conf\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-c", "--config", required=True, help="путь к конфигурационному файлу (TOML)")
    parser.add_argument("--check", action="store_true", help="только проверить подключение и доступ к бакету")
    parser.add_argument("--check-write", action="store_true", help="в режиме --check дополнительно проверить запись и удаление")
    parser.add_argument("--dry-run", action="store_true", help="показать план действий и ничего не менять")
    parser.add_argument("--no-delete", action="store_true", help="не удалять объекты, которых нет локально")
    parser.add_argument("--force", action="store_true", help="разрешить опасные удаления (пустая папка, превышение max_deletes)")
    parser.add_argument("--force-upload", action="store_true", help="загрузить все файлы, даже если они не изменились")
    parser.add_argument("--checksum", action="store_true", help="сравнивать md5 (медленнее, зато ловит правки с тем же размером)")
    parser.add_argument("--only", action="append", metavar="ИМЯ", help="синхронизировать только указанные папки (можно несколько раз)")
    parser.add_argument("--skip-missing", action="store_true", help="пропускать отсутствующие локальные папки вместо ошибки")
    parser.add_argument("--follow-symlinks", action="store_true", help="идти по символическим ссылкам на каталоги")
    parser.add_argument("--jobs", type=int, default=4, help="число параллельных загрузок (по умолчанию 4)")
    parser.add_argument("--tolerance", type=float, default=2.0, help="допуск сравнения времени, секунды (по умолчанию 2)")
    parser.add_argument("--max-delete", type=int, default=None, help="максимум удалений за запуск (0 — без ограничения)")
    parser.add_argument("--retries", type=int, default=5, help="число повторов при сетевых ошибках (по умолчанию 5)")
    parser.add_argument("--connect-timeout", type=float, default=15.0, help="таймаут подключения, секунды")
    parser.add_argument("--read-timeout", type=float, default=120.0, help="таймаут чтения, секунды")

    overrides = parser.add_argument_group("переопределение параметров подключения")
    overrides.add_argument("--endpoint-url", help=f"адрес хранилища (по умолчанию {DEFAULT_ENDPOINT})")
    overrides.add_argument("--region", help=f"регион (по умолчанию {DEFAULT_REGION})")
    overrides.add_argument("--bucket", help="имя бакета")
    overrides.add_argument("--profile", help="профиль в ~/.aws/credentials")
    overrides.add_argument("--credentials-file", help="путь к файлу с ключами в формате ~/.aws/credentials")
    overrides.add_argument("--access-key", help="Access Key (попадёт в историю команд — лучше через окружение или профиль)")
    overrides.add_argument("--secret-key", help="Secret Access Key (см. предупреждение выше)")
    overrides.add_argument("--session-token", help="временный токен сессии, если он есть")
    overrides.add_argument("--no-env-credentials", action="store_true", help="игнорировать AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY")

    logging_group = parser.add_argument_group("вывод")
    logging_group.add_argument("-v", "--verbose", action="store_true", help="подробный вывод")
    logging_group.add_argument("-q", "--quiet", action="store_true", help="только ошибки и итог")
    logging_group.add_argument("--log-file", help="дополнительно писать журнал в файл")
    return parser


def setup_logging(args: argparse.Namespace) -> None:
    level = logging.INFO
    if args.verbose:
        level = logging.DEBUG
    if args.quiet:
        level = logging.WARNING
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if args.log_file:
        handlers.append(logging.FileHandler(args.log_file, encoding="utf-8"))
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
        force=True,
    )


def select_folders(settings: Settings, args: argparse.Namespace) -> list[Folder]:
    folders = list(settings.folders)
    if args.only:
        wanted = {name.strip() for name in args.only if name.strip()}
        known = {folder.name for folder in folders}
        missing = sorted(wanted - known)
        if missing:
            raise ConfigError(
                f"--only: неизвестные имена папок: {', '.join(missing)}. Известные: {', '.join(sorted(known))}"
            )
        folders = [folder for folder in folders if folder.name in wanted]

    if args.no_delete:
        folders = [
            Folder(name=f.name, local=f.local, prefix=f.prefix, delete=False, excludes=f.excludes) for f in folders
        ]

    absent = [folder for folder in folders if not folder.local.is_dir()]
    if absent and not args.skip_missing:
        names = ", ".join(f"«{folder.name}» ({folder.local})" for folder in absent)
        raise ConfigError(f"локальные папки не найдены: {names}. Проверьте пути или запустите с --skip-missing")
    folders = [folder for folder in folders if folder.local.is_dir()]
    if not folders:
        raise ConfigError("после отбора не осталось ни одной папки для синхронизации")
    return folders


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    setup_logging(args)

    if args.jobs < 1:
        LOG.error("--jobs должен быть не меньше 1")
        return EXIT_USAGE
    if args.check_write:
        args.check = True

    try:
        settings = load_settings(args)
        client = build_client(settings, args)
    except ConfigError as exc:
        LOG.error("%s", exc)
        return EXIT_USAGE
    except Exception as exc:  # noqa: BLE001
        LOG.error("не удалось подготовить подключение: %s", exc)
        return EXIT_ERROR

    if args.check:
        try:
            return run_check(client, settings, args)
        except Exception as exc:  # noqa: BLE001
            LOG.error("проверка не удалась: %s", explain_client_error(exc, settings))
            return EXIT_ERROR

    try:
        folders = select_folders(settings, args)
    except ConfigError as exc:
        LOG.error("%s", exc)
        return EXIT_USAGE

    started = time.monotonic()
    try:
        stats, problems = run_sync(client, settings, args, folders)
    except Exception as exc:  # noqa: BLE001
        LOG.error("синхронизация прервана: %s", explain_client_error(exc, settings))
        return EXIT_ERROR

    elapsed = time.monotonic() - started
    LOG.info(
        "Итог: загружено %d (%s), удалено %d, без изменений %d, оставлено в хранилище %d, ошибок %d, время %.1f с",
        stats.uploaded,
        human_size(stats.bytes_uploaded),
        stats.deleted,
        stats.skipped,
        stats.kept,
        stats.errors,
        elapsed,
    )
    for problem in problems:
        LOG.error("проблема: %s", problem)
    if problems or stats.errors:
        return EXIT_ERROR
    return EXIT_OK


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        LOG.error("прервано пользователем")
        sys.exit(EXIT_ERROR)
