# -*- coding: utf-8 -*-
"""Сквозные тесты s3_sync.py на встроенном мини-S3-сервере (tests/fake_s3.py).

Запуск из каталога s3-folder-sync:

    python -m pip install boto3
    python -m unittest discover -s tests -v

Тесты запускают настоящий скрипт отдельным процессом и настоящий boto3,
поэтому проверяется вся цепочка: конфиг → клиент → план → загрузка/удаление.
"""

from __future__ import annotations

import contextlib
import io
import itertools
import os
import shutil
import subprocess
import sys
import time
import unittest
from collections import namedtuple
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from fake_s3 import FakeS3Server  # noqa: E402

PROJECT = HERE.parent
SCRIPT = PROJECT / "s3_sync.py"

# Временные каталоги создаём внутри проекта: в некоторых окружениях системный
# %TEMP% может быть недоступен для записи.
TMP_ROOT = PROJECT / ".test-tmp"
TMP_ROOT.mkdir(exist_ok=True)
_CASE_NUMBERS = itertools.count(1)

BUCKET = "s3-sync-test"
ACCESS_KEY = "test-access-key"
SECRET_KEY = "test-secret-key"
REGION = "spb"
PREFIX = "backup/data"

# Результат запуска скрипта в текущем процессе: повторяет поля CompletedProcess
# там, где дочерний процесс не подходит (см. run_sync_inprocess).
InProcessResult = namedtuple("InProcessResult", "returncode stderr")


def load_s3_sync():
    """Загружает s3_sync.py как модуль — нужен тестам, подменяющим файловые вызовы."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("s3_sync_under_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Без регистрации в sys.modules падает @dataclass: он ищет модуль по имени.
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(spec.name, None)
        raise
    return module


class SyncEndToEndTest(unittest.TestCase):
    server: FakeS3Server
    client = None

    @classmethod
    def setUpClass(cls):
        try:
            import boto3  # noqa: F401
        except ModuleNotFoundError:
            raise unittest.SkipTest("boto3 не установлен: python -m pip install boto3")
        cls.server = FakeS3Server().start()
        cls.endpoint = cls.server.endpoint

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    # -- вспомогательное ----------------------------------------------------

    def setUp(self):
        import boto3

        # Каталог создаём через Path.mkdir, а не tempfile.mkdtemp: в некоторых
        # песочницах каталоги с правами 700 недоступны на запись.
        self._tmp = TMP_ROOT / f"case-{os.getpid()}-{next(_CASE_NUMBERS)}"
        self._tmp.mkdir(parents=True, exist_ok=True)
        self.addCleanup(shutil.rmtree, self._tmp, ignore_errors=True)
        self.root = Path(self._tmp)
        self.local = self.root / "data"
        self.local.mkdir()
        self.config = self.root / "s3-sync.conf"
        self.server.store.buckets.clear()
        self.server.store.uploads.clear()
        self.server.store.create_bucket(BUCKET)
        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            region_name=REGION,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
        )

    def write_config(self, body: str | None = None, storage_extra: str = "") -> None:
        if body is None:
            body = f"""
[storage]
endpoint_url = "{self.endpoint}"
region = "{REGION}"
bucket = "{BUCKET}"
addressing_style = "path"
max_deletes = 0
{storage_extra}

[defaults]
delete = true
exclude = ["*.tmp", ".git/**"]

[[folders]]
name = "data"
local = "{self.local.as_posix()}"
prefix = "{PREFIX}"
"""
        self.config.write_text(body, encoding="utf-8")

    def run_sync(self, *extra: str, env_overrides: dict[str, str | None] | None = None) -> subprocess.CompletedProcess:
        env = dict(os.environ)
        for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE"):
            env.pop(name, None)
        env["S3_SYNC_ACCESS_KEY"] = ACCESS_KEY
        env["S3_SYNC_SECRET_KEY"] = SECRET_KEY
        env["PYTHONIOENCODING"] = "utf-8"
        # Если зависимости лежат в отдельной папке (--target), дочернему процессу
        # тоже нужен PYTHONPATH; при установке в venv этого не требуется.
        extra_paths = [str(path) for path in (PROJECT / ".deps",) if path.is_dir()]
        if extra_paths:
            env["PYTHONPATH"] = os.pathsep.join(extra_paths + [env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
        if env_overrides:
            # Значение None убирает переменную: нужно там, где проверяется
            # приоритет источников (S3_SYNC_* перебивает AWS_*).
            for name, value in env_overrides.items():
                if value is None:
                    env.pop(name, None)
                else:
                    env[name] = value
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--config", str(self.config), *extra],
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=env,
            timeout=180,
        )

    def run_sync_inprocess(self, *extra: str, fail_read_for: str | None = None) -> InProcessResult:
        """Запускает s3_sync.main() в текущем процессе и перехватывает его stderr.

        Нужен там, где сбой файловой системы иначе не воспроизвести: вызовы
        os.stat/os.lstat для указанного файла начинают падать с OSError — так
        выглядит ошибка прав доступа или сбой сетевой ФС.
        """
        module = load_s3_sync()
        env_backup = dict(os.environ)
        real_stat, real_lstat = os.stat, os.lstat

        def broken(original):
            def wrapper(path, *args, **kwargs):
                if str(path).replace("\\", "/").endswith(fail_read_for):
                    raise OSError(13, "Permission denied")
                return original(path, *args, **kwargs)

            return wrapper

        for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE"):
            os.environ.pop(name, None)
        os.environ["S3_SYNC_ACCESS_KEY"] = ACCESS_KEY
        os.environ["S3_SYNC_SECRET_KEY"] = SECRET_KEY
        if fail_read_for:
            os.stat = broken(real_stat)
            os.lstat = broken(real_lstat)

        buffer = io.StringIO()
        try:
            with contextlib.redirect_stderr(buffer):
                code = module.main(["--config", str(self.config), *extra])
        finally:
            os.stat, os.lstat = real_stat, real_lstat
            os.environ.clear()
            os.environ.update(env_backup)
        return InProcessResult(returncode=code, stderr=buffer.getvalue())

    def make_file(self, relpath: str, content: str | bytes, mtime: float | None = None) -> Path:
        path = self.local / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            path.write_bytes(content)
        else:
            path.write_text(content, encoding="utf-8")
        if mtime is not None:
            os.utime(path, (mtime, mtime))
        return path

    def remote(self) -> dict[str, str]:
        result = {}
        for key, item in self.server.store.list(BUCKET, ""):
            result[key] = item["body"].decode("utf-8", "replace")
        return result

    # -- тесты --------------------------------------------------------------

    def test_01_initial_upload_and_exclusions(self):
        self.make_file("a.txt", "aaa")
        self.make_file("sub/b.txt", "bbb")
        self.make_file("cache/skip.tmp", "tmp")
        self.make_file(".git/config", "git")
        self.write_config()

        completed = self.run_sync()
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(self.remote(), {f"{PREFIX}/a.txt": "aaa", f"{PREFIX}/sub/b.txt": "bbb"})
        self.assertIn("загружено 2", completed.stderr)

    def test_02_second_run_uploads_nothing(self):
        self.make_file("a.txt", "aaa")
        self.write_config()
        self.assertEqual(self.run_sync().returncode, 0)

        completed = self.run_sync()
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("загружено 0", completed.stderr)
        self.assertIn("без изменений 1", completed.stderr)

    def test_03_changes_and_deletions(self):
        self.make_file("a.txt", "aaa")
        self.make_file("sub/b.txt", "bbb")
        self.write_config()
        self.assertEqual(self.run_sync().returncode, 0)

        (self.local / "a.txt").unlink()
        self.make_file("sub/b.txt", "bbb-changed-longer")
        self.make_file("c.txt", "ccc")

        completed = self.run_sync()
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(
            self.remote(),
            {f"{PREFIX}/sub/b.txt": "bbb-changed-longer", f"{PREFIX}/c.txt": "ccc"},
        )
        self.assertIn("удалено 1", completed.stderr)

    def test_04_dry_run_changes_nothing(self):
        self.make_file("a.txt", "aaa")
        self.write_config()
        self.assertEqual(self.run_sync().returncode, 0)

        (self.local / "a.txt").unlink()
        self.make_file("d.txt", "ddd")

        completed = self.run_sync("--dry-run")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("dry-run", completed.stderr)
        self.assertEqual(self.remote(), {f"{PREFIX}/a.txt": "aaa"})

    def test_05_checksum_catches_same_size_change(self):
        old_time = time.time() - 3600
        self.make_file("f.txt", "AAAA", mtime=old_time)
        self.write_config()
        self.assertEqual(self.run_sync().returncode, 0)

        self.make_file("f.txt", "BBBB", mtime=old_time)

        completed = self.run_sync()
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(self.remote(), {f"{PREFIX}/f.txt": "AAAA"})

        completed = self.run_sync("--checksum")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(self.remote(), {f"{PREFIX}/f.txt": "BBBB"})

    def test_06_empty_folder_guard(self):
        self.make_file("a.txt", "aaa")
        self.write_config()
        self.assertEqual(self.run_sync().returncode, 0)

        (self.local / "a.txt").unlink()

        completed = self.run_sync()
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("пуста", completed.stderr)
        self.assertEqual(self.remote(), {f"{PREFIX}/a.txt": "aaa"})

        completed = self.run_sync("--force")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(self.remote(), {})

    def test_07_check_mode(self):
        self.write_config()
        completed = self.run_sync("--check", "--check-write")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("подключение работает", completed.stderr)
        self.assertIn("HEAD бакета:       OK", completed.stderr)

    def test_08_check_reports_wrong_bucket(self):
        self.write_config(
            body=f"""
[storage]
endpoint_url = "{self.endpoint}"
region = "{REGION}"
bucket = "no-such-bucket"
"""
        )
        completed = self.run_sync("--check")
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("ОШИБКА", completed.stderr)
        # HeadBucket отвечает без тела, поэтому подсказка строится по HTTP-статусу.
        self.assertIn("бакета с таким именем нет", completed.stderr)

    def test_09_check_reports_unreachable_endpoint(self):
        self.write_config(
            body="""
[storage]
endpoint_url = "http://127.0.0.1:9"
region = "spb"
bucket = "x"
"""
        )
        completed = self.run_sync("--check")
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("не удалось подключиться", completed.stderr)

    def test_10_nested_prefixes_rejected(self):
        self.write_config(
            body=f"""
[storage]
endpoint_url = "{self.endpoint}"
region = "{REGION}"
bucket = "{BUCKET}"

[[folders]]
name = "outer"
local = "{self.local.as_posix()}"
prefix = "backups"

[[folders]]
name = "inner"
local = "{self.local.as_posix()}"
prefix = "backups/site"
"""
        )
        completed = self.run_sync()
        self.assertEqual(completed.returncode, 2)
        self.assertIn("вложен", completed.stderr)

    def test_11_no_delete_then_delete_again(self):
        self.make_file("a.txt", "aaa")
        self.make_file("b.txt", "bbb")
        self.write_config()
        self.assertEqual(self.run_sync().returncode, 0)

        (self.local / "b.txt").unlink()

        completed = self.run_sync("--no-delete")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn(f"{PREFIX}/b.txt", self.remote())

        completed = self.run_sync("--only", "data")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertNotIn(f"{PREFIX}/b.txt", self.remote())

    def test_12_max_deletes_guard(self):
        for index in range(3):
            self.make_file(f"file{index}.bin", f"data-{index}")
        self.write_config()
        self.assertEqual(self.run_sync().returncode, 0)

        # Удаляем два файла из трёх: папка не пуста, значит сработает
        # предохранитель max_deletes, а не защита от пустой папки.
        for index in (0, 1):
            (self.local / f"file{index}.bin").unlink()

        completed = self.run_sync("--max-delete", "1")
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("max_deletes", completed.stderr)
        self.assertEqual(len(self.remote()), 3)

        completed = self.run_sync("--max-delete", "1", "--force")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(list(self.remote()), [f"{PREFIX}/file2.bin"])

    def test_13_large_file_uses_multipart_upload(self):
        payload = bytes(range(256)) * 800  # ~200 KiB
        self.make_file("big.bin", payload)
        self.write_config(storage_extra='multipart_threshold = "1K"\nmultipart_chunksize = "64K"')

        completed = self.run_sync()
        self.assertEqual(completed.returncode, 0, completed.stderr)

        items = dict(self.server.store.list(BUCKET, ""))
        stored = items[f"{PREFIX}/big.bin"]
        self.assertEqual(stored["body"], payload)
        self.assertIn("-", stored["etag"], "ожидался составной ETag multipart-загрузки")

    def test_14_missing_local_folder(self):
        self.make_file("a.txt", "aaa")
        self.write_config()
        (self.local / ".." / "data").rename(self.root / "data-moved")

        completed = self.run_sync()
        self.assertEqual(completed.returncode, 2)
        self.assertIn("не найдены", completed.stderr)

        completed = self.run_sync("--skip-missing")
        self.assertEqual(completed.returncode, 2)
        self.assertIn("не осталось ни одной папки", completed.stderr)

    # -- регрессии: предохранители против потери данных ----------------------

    def test_15_folder_without_prefix_conflicts_with_prefixed_folder(self):
        """Папка без префикса покрывает весь бакет, поэтому рядом с чужим префиксом — отказ."""
        self.make_file("a.txt", "aaa")
        self.client.put_object(Bucket=BUCKET, Key="backups/site/keep.txt", Body=b"keep")
        self.write_config(
            body=f"""
[storage]
endpoint_url = "{self.endpoint}"
region = "{REGION}"
bucket = "{BUCKET}"

[[folders]]
name = "whole-bucket"
local = "{self.local.as_posix()}"

[[folders]]
name = "site"
local = "{self.local.as_posix()}"
prefix = "backups/site"
"""
        )
        completed = self.run_sync()
        self.assertEqual(completed.returncode, 2, completed.stderr)
        self.assertIn("без префикса", completed.stderr)
        self.assertIn(
            "backups/site/keep.txt",
            self.remote(),
            "конфигурация должна отклоняться до любых удалений в бакете",
        )

    def test_16_no_env_credentials_ignores_aws_env(self):
        """--no-env-credentials отключает и стандартные AWS_*, а не только S3_SYNC_*."""
        self.write_config(
            body=f"""
[storage]
endpoint_url = "{self.endpoint}"
region = "{REGION}"
bucket = "{BUCKET}"
access_key = "CONFIGKEY123"
secret_key = "CONFIGSECRET123"
"""
        )
        # S3_SYNC_* проверяются раньше AWS_*, поэтому здесь их убираем: иначе
        # до стандартных AWS_* дело просто не дойдёт.
        env_overrides = {
            "S3_SYNC_ACCESS_KEY": None,
            "S3_SYNC_SECRET_KEY": None,
            "AWS_ACCESS_KEY_ID": "ENVKEY456",
            "AWS_SECRET_ACCESS_KEY": "ENVSECRET456",
        }

        # Без флага приоритет у окружения — это штатное поведение.
        without_flag = self.run_sync("--check", env_overrides=env_overrides)
        self.assertEqual(without_flag.returncode, 0, without_flag.stderr)
        self.assertIn("ENVK…456", without_flag.stderr)

        # С флагом ключи берутся из конфига, ключи окружения игнорируются.
        with_flag = self.run_sync("--check", "--no-env-credentials", env_overrides=env_overrides)
        self.assertEqual(with_flag.returncode, 0, with_flag.stderr)
        self.assertIn("CONF…123", with_flag.stderr)
        self.assertNotIn("ENVK…456", with_flag.stderr)

    def test_17_unreadable_local_file_blocks_deletion(self):
        """Ошибка чтения при обходе не должна приводить к удалению объекта из хранилища."""
        self.make_file("a.txt", "aaa")
        self.make_file("b.txt", "bbb")
        self.write_config()
        self.assertEqual(self.run_sync().returncode, 0)

        result = self.run_sync_inprocess(fail_read_for="b.txt")
        self.assertNotEqual(result.returncode, 0, "неполный обход обязан сообщаться как ошибка")
        self.assertIn("не удалось прочитать", result.stderr)
        self.assertEqual(sorted(self.remote()), [f"{PREFIX}/a.txt", f"{PREFIX}/b.txt"])

    def test_18_unreadable_local_file_with_force_deletes_and_warns(self):
        """--force снимает предохранитель: удаление выполняется, но предупреждение остаётся."""
        self.make_file("a.txt", "aaa")
        self.make_file("b.txt", "bbb")
        self.write_config()
        self.assertEqual(self.run_sync().returncode, 0)

        result = self.run_sync_inprocess("--force", fail_read_for="b.txt")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--force", result.stderr)
        self.assertEqual(sorted(self.remote()), [f"{PREFIX}/a.txt"])

    def test_19_unreadable_directory_is_reported(self):
        """Нечитаемый каталог не должен молча выпадать из обхода."""
        module = load_s3_sync()
        locked = self.local / "locked"
        locked.mkdir()
        real_walk = os.walk

        def fake_walk(root, *, followlinks=False, onerror=None):
            if onerror is not None:
                onerror(OSError(13, "Permission denied", str(locked)))
            return iter(())

        unreadable: list[str] = []
        os.walk = fake_walk
        try:
            files = module.collect_local(self.local, (), unreadable=unreadable)
        finally:
            os.walk = real_walk
        self.assertEqual(files, {})
        self.assertEqual(unreadable, ["locked"])

    def test_20_unreadable_file_with_no_delete_only_warns(self):
        """Без удаления неполный обход ничем не грозит: предупреждение есть, код возврата нулевой."""
        self.make_file("a.txt", "aaa")
        self.make_file("b.txt", "bbb")
        self.write_config()
        self.assertEqual(self.run_sync().returncode, 0)

        result = self.run_sync_inprocess("--no-delete", fail_read_for="b.txt")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("обход неполный", result.stderr)
        self.assertEqual(sorted(self.remote()), [f"{PREFIX}/a.txt", f"{PREFIX}/b.txt"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
