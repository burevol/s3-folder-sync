# s3-folder-sync — синхронизация папок с S3 (Спринтбокс)

[![Тесты](https://github.com/burevol/s3-folder-sync/actions/workflows/tests.yml/badge.svg)](https://github.com/burevol/s3-folder-sync/actions/workflows/tests.yml)
[![Лицензия: MIT](https://img.shields.io/badge/%D0%BB%D0%B8%D1%86%D0%B5%D0%BD%D0%B7%D0%B8%D1%8F-MIT-blue.svg)](LICENSE)

`s3_sync.py` — один Python-скрипт, который приводит содержимое S3-бакета в
соответствие с набором локальных папок, перечисленных в конфигурационном файле:

* загружает новые и изменившиеся файлы;
* **удаляет из хранилища объекты, которых больше нет локально** — только внутри
  указанного префикса, чужие данные не трогает;
* умеет показать план (`--dry-run`) и проверить доступ к бакету (`--check`).

Написан под хранилище [Спринтбокс](https://help.sprintbox.ru/main/s3-storage)
(`https://s3.spb.sprinthost.ru`, регион `spb`), но работает с любым
S3-совместимым сервисом (MinIO, Ceph RGW, AWS S3): endpoint и регион задаются
в конфигурации.

## Содержимое

| Файл | Назначение |
| --- | --- |
| `s3_sync.py` | сам скрипт, зависимость только `boto3` |
| `s3-sync.conf.example` | пример конфигурации, копируется в `s3-sync.conf` |
| `requirements.txt` | зависимости (`boto3`, `tomli` для Python < 3.11) |
| `tests/test_sync.py` | сквозные тесты (запускают скрипт отдельным процессом) |
| `tests/fake_s3.py` | мини-S3 на стандартной библиотеке для тестов |
| `LICENSE` | лицензия MIT |
| `.github/workflows/tests.yml` | CI: тесты на Python 3.9, 3.11 и 3.13 |

## Установка

Нужен Python 3.9+ (проверено на 3.13). В системный Python ничего ставить не
нужно — разворачивайте отдельное окружение:

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt          # Linux/macOS
.venv\Scripts\pip install -r requirements.txt      # Windows
```

Дальше скрипт запускается интерпретатором окружения:
`.venv/bin/python s3_sync.py --config s3-sync.conf` (в Windows —
`.venv\Scripts\python.exe s3_sync.py --config s3-sync.conf`).

> Работа в Windows-песочнице DSH: создание venv и работа pip там требуют обхода
> из-за прав на временные каталоги — рецепт в
> [`../pip-sandbox-fix/README.md`](../pip-sandbox-fix/README.md).

## Быстрый старт

```bash
cp s3-sync.conf.example s3-sync.conf
chmod 600 s3-sync.conf                 # если ключи решите хранить в файле
# правим endpoint/bucket/папки ...
export S3_SYNC_ACCESS_KEY="Access Key из панели Спринтбокс"
export S3_SYNC_SECRET_KEY="Secret Access Key из панели Спринтбокс"

.venv/bin/python s3_sync.py --config s3-sync.conf --check --check-write   # проверить доступ
.venv/bin/python s3_sync.py --config s3-sync.conf --dry-run -v            # посмотреть план
.venv/bin/python s3_sync.py --config s3-sync.conf                         # выполнить
```

---

# Данные для подключения к тестовому бакету

## Что вообще нужно

В панели управления Спринтбокс: **S3 → создать бакет → «Данные для подключения»**
(они же дублируются письмом). Оттуда нужны четыре значения, плюс одно
необязательное:

| Что в панели | Куда в конфиге | Пример / значение для Спринтбокса |
| --- | --- | --- |
| **URL** | `[storage] endpoint_url` | `https://s3.spb.sprinthost.ru` |
| **Region** | `[storage] region` | `spb` |
| **Name** (имя бакета) | `[storage] bucket` | `s3-123456` |
| **Access Key** | ключ доступа (см. ниже) | `20 символов`, публичный идентификатор |
| **Secret Access Key** | секретный ключ (см. ниже) | показывается один раз при создании |

То есть «предоставить данные для подключения» = передать скрипту
`endpoint_url`, `region`, `bucket` и пару ключей. Endpoint и регион для
Спринтбокса уже стоят в примере конфига значениями по умолчанию, так что
фактически остаются **имя бакета и два ключа**.

Важно: **Secret Access Key показывается только один раз** — при создании
бакета (и дублируется в письме). Если он потерян, в панели управления нужно
перевыпустить ключи для бакета.

## Четыре способа передать ключи

Выбирайте один; приоритет: аргументы командной строки → переменные окружения →
конфигурационный файл → профиль в `~/.aws/credentials`.

### 1. Переменные окружения (рекомендуется для тестового запуска и CI)

```bash
export S3_SYNC_ACCESS_KEY="XXXXACCESSKEYXXXX"
export S3_SYNC_SECRET_KEY="SECRETKEY"
python s3_sync.py --config s3-sync.conf --check --check-write
```

Понимаются и стандартные переменные AWS — `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_PROFILE`,
`AWS_SHARED_CREDENTIALS_FILE`. Если в окружении лежат «чужие» AWS-ключи,
запускайте с `--no-env-credentials`.

### 2. Профиль в `~/.aws/credentials` (рекомендуется для постоянной работы)

`~/.aws/credentials`:

```ini
[sprintbox-test]
aws_access_key_id = XXXXACCESSKEYXXXX
aws_secret_access_key = SECRETKEY
```

В конфиге:

```toml
[storage]
profile = "sprintbox-test"
```

Для отдельного файла ключей с другим путём есть ключ `credentials_file` или
аргумент `--credentials-file`:

```toml
[storage]
credentials_file = "~/.config/s3-sync/credentials"   # chmod 600
```

### 3. Ключи прямо в конфигурационном файле

```toml
[storage]
access_key = "XXXXACCESSKEYXXXX"
secret_key = "SECRETKEY"
```

Работает, но менее безопасно: ключи попадают в бэкапы, в git и в `ps`.
Скрипт предупредит об этом при запуске. Обязательно `chmod 600 s3-sync.conf`.

### 4. Аргументы командной строки — только для разовой отладки

```bash
python s3_sync.py --config s3-sync.conf --access-key ... --secret-key ... --check
```

Ключи останутся в истории shell. Лучше так не делать.

## Проверка, что данные корректны

```bash
python s3_sync.py --config s3-sync.conf --check --check-write
```

Скрипт печатает endpoint, регион, имя бакета, стиль адресации, замаскированный
Access Key и **источник**, откуда он взялся, затем выполняет:

* `HEAD` бакета — существует ли бакет и пускает ли ключ;
* `LIST` — чтение списка объектов;
* (`--check-write`) запись и удаление пробного объекта.

Успех выглядит так:

```
Endpoint:          https://s3.spb.sprinthost.ru
Регион:            spb
Бакет:             s3-123456
Стиль адресации:   path
Access Key:        XXXX…KEY (переменная окружения S3_SYNC_ACCESS_KEY)
Secret Key:        *** (переменная окружения S3_SYNC_SECRET_KEY)
HEAD бакета:       OK — бакет существует и доступен
Список объектов:   OK — видно объектов: 0
Запись объекта:    OK — создан s3_sync_probe-…txt
Удаление объекта:  OK — s3_sync_probe-…txt удалён
Итог проверки:     подключение работает
```

При ошибке скрипт печатает код ответа хранилища и подсказку:

| Код S3 | Что значит |
| --- | --- |
| `InvalidAccessKeyId` | неверный Access Key |
| `SignatureDoesNotMatch` | неверный Secret Access Key |
| `NoSuchBucket` | нет такого бакета — проверьте `bucket` |
| `AccessDenied` | ключ есть, но прав на бакет/операцию нет |
| `RequestTimeTooSkewed` | сбиты часы на машине (нужен NTP) |
| `NotImplemented` | хранилище не поняло запрос — попробуйте `addressing_style = "virtual"` |

У `HEAD`-запроса к бакету ответ приходит без тела, поэтому вместо кода скрипт
показывает подсказку по HTTP-статусу (404 — нет бакета, 403 — нет доступа).
Спринтбокс на неизвестный Access Key тоже отвечает `AccessDenied`, так что при
403 проверяйте сначала ключ, потом права.

## Технические детали подключения к Спринтбоксу

* Хранилище — Ceph RGW, ключи работают по подписи **AWS Signature v4**
  (`signature_version = "s3v4"`), регион `spb`.
* Endpoint — `https://s3.spb.sprinthost.ru`. Слэш в конце не обязателен.
* Работают оба стиля адресации (проверено на `s3.spb.sprinthost.ru`):
  * `path` — `https://s3.spb.sprinthost.ru/бакет/ключ` — значение по умолчанию,
    самый совместимый вариант;
  * `virtual` — `https://бакет.s3.spb.sprinthost.ru/ключ`; DNS-запись
    `*.s3.sprinthost.ru` есть, сертификат на неё выписан.
* Скрипт отключает новые «контрольные суммы в теле запроса» botocore
  (`request_checksum_calculation = when_required`), чтобы не получить
  несовместимость с хранилищем.

---

# Конфигурационный файл

Формат — TOML. Полный пример с комментариями — в `s3-sync.conf.example`.

## `[storage]`

| Ключ | По умолчанию | Описание |
| --- | --- | --- |
| `endpoint_url` | `https://s3.spb.sprinthost.ru` | адрес хранилища |
| `region` | `spb` | регион |
| `bucket` | — (обязательно) | имя бакета |
| `addressing_style` | `path` | `path`, `virtual` или `auto` |
| `signature_version` | `s3v4` | версия подписи |
| `profile` | — | профиль в `~/.aws/credentials` |
| `credentials_file` | — | свой файл с ключами |
| `access_key`, `secret_key`, `session_token` | — | ключи прямо в конфиге |
| `max_deletes` | `0` | предохранитель: больше этого числа удалений за запуск — стоп без `--force` (0 — без ограничения) |
| `multipart_threshold` | `64M` | с какого размера файл грузится частями |
| `multipart_chunksize` | `16M` | размер части |

## `[defaults]`

Значения по умолчанию для всех папок: `delete` (по умолчанию `true`) и
`exclude` — список шаблонов. Шаблон без `/` сравнивается с именем файла на любом
уровне (`*.tmp`), шаблон с `/` — с путём внутри папки (`cache/**`,
`src/tmp/*.log`). Каталог, попавший под шаблон, целиком пропускается.

## `[[folders]]`

| Ключ | Обязателен | Описание |
| --- | --- | --- |
| `name` | нет | имя для логов и ключа `--only`; по умолчанию — префикс |
| `local` | да | локальная папка; относительный путь считается от каталога конфига |
| `prefix` | нет | «папка» в бакете; ключ = `prefix/путь-внутри-папки`; пусто — корень бакета |
| `delete` | нет | удалять ли в хранилище то, чего нет локально |
| `exclude` | нет | дополнительные шаблоны исключений |

Префиксы разных папок не должны совпадать или вкладываться друг в друга —
скрипт откажется работать, потому что удаление в «внешнем» префиксе снесло бы
данные «внутреннего».

# Как принимаются решения о загрузке и удалении

Для каждого файла по порядку:

1. `--force-upload` — грузим всё;
2. объекта с таким ключом нет — грузим («нет в хранилище»);
3. размер отличается от локального — грузим;
4. `--checksum` и у объекта обычный ETag (не multipart): считаем md5 локально,
   при расхождении грузим;
5. иначе, если локальный файл новее времени изменения объекта (допуск
   `--tolerance`, по умолчанию 2 с) — грузим;
6. иначе файл считается актуальным и не загружается.

Объекты, для которых нет локального файла, удаляются (если `delete = true`).
Символические ссылки не загружаются (для каталогов есть `--follow-symlinks`).

Практические следствия:

* содержимое сравнивается по размеру и времени, а не по хешу — так быстро;
  правка с тем же размером и сохранённым временем изменения подхватится
  только с `--checksum`;
* после загрузки время объекта в хранилище становится больше локального, поэтому
  повторный запуск ничего не грузит.

# Использование

```
python s3_sync.py --config FILE [опции]

  --check                 проверить подключение и доступ к бакету
  --check-write           в режиме --check ещё и записать/удалить пробный объект
  --dry-run               показать план и ничего не менять
  --no-delete             не удалять в хранилище
  --force                 разрешить опасные удаления (пустая папка, > max_deletes)
  --force-upload          загрузить всё заново
  --checksum              сравнивать md5
  --only ИМЯ              только указанные папки (можно повторять)
  --skip-missing          не падать, если локальной папки нет
  --follow-symlinks       идти по симлинкам на каталоги
  --jobs N                параллельных загрузок (по умолчанию 4)
  --tolerance СЕК         допуск сравнения времени (по умолчанию 2)
  --max-delete N          лимит удалений за запуск (0 — без лимита)
  --retries N             повторов при сетевых ошибках (по умолчанию 5)
  --log-file FILE         писать журнал в файл
  -v / -q                 подробный / тихий вывод
  --endpoint-url, --region, --bucket, --profile, --credentials-file,
  --access-key, --secret-key, --session-token, --no-env-credentials
```

Коды возврата: `0` — успех, `1` — ошибки при выполнении, `2` — ошибка
конфигурации или параметров. Это удобно для cron и systemd.

## Автоматический запуск

cron (ключи — в файле окружения с правами 600):

```cron
# /etc/cron.d/s3-folder-sync
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin
*/30 * * * * root set -a; . /etc/s3-sync.env; set +a; /opt/s3-folder-sync/.venv/bin/python /opt/s3-folder-sync/s3_sync.py --config /etc/s3-sync.conf --quiet >> /var/log/s3-folder-sync.log 2>&1
```

```bash
# /etc/s3-sync.env  (chmod 600, владелец root)
S3_SYNC_ACCESS_KEY=XXXXACCESSKEYXXXX
S3_SYNC_SECRET_KEY=SECRETKEY
```

systemd (таймер + сервис, ключи в `EnvironmentFile`):

```ini
# /etc/systemd/system/s3-folder-sync.service
[Unit]
Description=Sync folders to S3
[Service]
Type=oneshot
EnvironmentFile=/etc/s3-sync.env
ExecStart=/opt/s3-folder-sync/.venv/bin/python /opt/s3-folder-sync/s3_sync.py --config /etc/s3-sync.conf --quiet
```

```ini
# /etc/systemd/system/s3-folder-sync.timer
[Unit]
Description=Run s3-folder-sync every 30 minutes
[Timer]
OnBootSec=5min
OnUnitActiveSec=30min
[Install]
WantedBy=timers.target
```

## Предохранители

* пустая (или внезапно опустевшая) локальная папка при непустом хранилище —
  удаление отменяется, нужен `--force`;
* `max_deletes` ограничивает число удалений за запуск;
* отсутствующая локальная папка — ошибка, а не «удалить всё» (обходится
  `--skip-missing`);
* `--dry-run` и `--only` помогают проверить новый конфиг;
* без `delete = true` скрипт вообще ничего не удаляет (есть и `--no-delete`).

## Тесты

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt                          # Linux/macOS
.venv/bin/python -m unittest discover -s tests -v
```

В Windows: `.venv\Scripts\pip install -r requirements.txt` и
`.venv\Scripts\python.exe -m unittest discover -s tests -v`.

Тесты поднимают мини-S3 (`tests/fake_s3.py`, только стандартная библиотека) и
запускают настоящий скрипт отдельным процессом. Проверяются: первичная загрузка
и исключения, идемпотентность повторного запуска, удаление исчезнувших файлов,
`--dry-run`, `--checksum`, предохранители (пустая папка, `max_deletes`,
отсутствующая папка), `--check` при неверном бакете и недоступном endpoint,
запрет вложенных префиксов и multipart-загрузка больших файлов.

Тот же набор тестов прогоняется в GitHub Actions
([`.github/workflows/tests.yml`](.github/workflows/tests.yml)) на Python 3.9, 3.11
и 3.13 при каждом push и pull request — вместе с проверкой, что скрипт
запускается и что `s3-sync.conf.example` остаётся корректным TOML. Никаких
секретов для CI не нужно: используется локальный мини-S3.

Дополнительно всё проверено на реальном бакете Спринтбокса: загрузка (в том числе
multipart для файла 12 МБ), повторный запуск без изменений, обновление файла по
размеру, удаление исчезнувшего объекта, `--checksum` для правки того же размера,
сработавший предохранитель на пустую папку и удаление по `--force`. Подключение
к реальному endpoint проверено и с фиктивными ключами: хранилище отвечает
`403 AccessDenied` (то есть запрос разобран корректно) в обоих стилях адресации —
`path` и `virtual`.

## Лицензия

MIT — см. [LICENSE](LICENSE).
