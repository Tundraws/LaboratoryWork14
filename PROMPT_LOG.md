# PROMPT_LOG

## Использованные AI-инструменты

- Codex / GPT-5 в роли senior software engineer, code reviewer и архитектора учебного production-like проекта.

## Исходный промпт

Пользователь попросил выполнить лабораторную работу №14 «Разработка конвейеров обработки данных на Python и Go» для студентки Мельниковой Анастасии, группа 220032-11, вариант 13, повышенная сложность: анализ пассажиропотока по датчикам метро. В требованиях были указаны обязательные README, PROMPT_LOG, Docker Compose, исходный код в `src/`, тесты в `tests/`, частые conventional commits, чистая архитектура, Go collector, Python analytics, Apache Arrow, etcd, Rust validation, Kubernetes/HPA, NATS/Kafka, dashboard и тесты.

## Что сгенерировано

1. Go-сборщик в `src/go-collector`:
   - симуляция датчиков метро;
   - распределение датчиков по shard;
   - etcd coordinator с fallback на локальное распределение;
   - tumbling window aggregation;
   - публикация агрегатов в NATS;
   - HTTP endpoint `/arrow` с Apache Arrow IPC stream;
   - `/healthz` и `/metrics`;
   - graceful shutdown и централизованная конфигурация.

2. Python-сервис в `src/python-analytics`:
   - FastAPI app factory `create_app()`;
   - Pydantic-схемы;
   - Arrow client;
   - Polars cleanup и Parquet persistence;
   - DuckDB SQL summaries;
   - Plotly visualization service;
   - NATS consumer;
   - realtime dashboard endpoint.

3. Rust/PyO3 validator в `src/rust-validator`:
   - проверка неотрицательных счётчиков;
   - проверка `net_flow = entries - exits`;
   - unit tests.

4. Инфраструктура:
   - `.gitignore`;
   - `.env.example`;
   - `docker-compose.yml`;
   - Dockerfile для Go и Python сервисов;
   - Kubernetes manifests;
   - GitHub Actions CI;
   - Makefile.

5. Тесты:
   - Go unit tests;
   - Python unit/API tests;
   - Rust unit tests.

## Runtime issues и исправления

### 1. Python-зависимости отсутствовали в окружении

Что сгенерировал ИИ: Python-сервис с DuckDB/Polars/PyArrow.  
Проблема: `pytest` упал с `ModuleNotFoundError: No module named 'duckdb'`.  
Как исправлено: зависимости установлены через `python -m pip install -r src\python-analytics\requirements.txt`.

### 2. Локальная установка `--target .pydeps` создала файл с ошибкой доступа

Что сгенерировал ИИ: попытка изолированно установить зависимости в `.pydeps`.  
Проблема: `PermissionError` на `.pydeps\py.py` и `.pydeps\typing_extensions.py`.  
Как исправлено: `.pydeps` добавлен в `.gitignore`, повреждённый файл удалён, зависимости установлены в текущий Python.

### 3. Polars изменил поведение timezone parsing

Что сгенерировал ИИ: `str.to_datetime(strict=False)` для ISO timestamps.  
Проблема: Polars потребовал явно указать timezone при парсинге строк с timezone.  
Как исправлено: добавлено `time_zone="UTC"` для `window_start` и `window_end`.

### 4. PyO3 unit tests требовали инициализации Python

Что сгенерировал ИИ: Rust-тесты вызывали `Python::with_gil` напрямую.  
Проблема: тесты падали с сообщением, что Python interpreter не инициализирован.  
Как исправлено: добавлен `pyo3::prepare_freethreaded_python()` перед `Python::with_gil`.

### 5. GitHub Actions: Go и Rust jobs падали после push

Что сгенерировал ИИ: CI использовал Go 1.22, хотя локальная проверка выполнялась на Go 1.24; Rust crate включал PyO3 feature `extension-module`, который подходит для сборки расширения, но может ломать обычный `cargo test` на Linux runner.  
Проблема: GitHub Actions показал падение `ci / go` и `ci / rust`, при этом Python job прошёл.  
Как исправлено: CI, `go.mod` и Go Dockerfile синхронизированы на Go 1.24; PyO3 переведён на test-friendly feature `auto-initialize`; flaky Go-тест оконной агрегации переписан на детерминированное закрытие входного канала.

### 6. Realtime-поток был реализован, но не подключён к lifecycle API

Что сгенерировал ИИ: `NATSWindowConsumer` и WebSocket endpoint были добавлены отдельными компонентами.  
Проблема: FastAPI lifespan не запускал NATS consumer автоматически, а HTML dashboard использовал polling вместо WebSocket.  
Как исправлено: consumer подключён в `lifespan`, ошибки NATS логируются без падения API, dashboard отправляет запрос состояния через WebSocket и откатывается на HTTP polling при недоступном сокете.

## Проверки

Выполнены локально:

```powershell
cd src/go-collector
go test ./...

$env:PYTHONPATH='src/python-analytics/src'
python -m pytest tests/python

cd src/rust-validator
cargo test
```

Результат: все тесты прошли.

## Улучшения после генерации

- Добавлены атомарные conventional commits вместо одного большого коммита.
- Исправлен Prometheus-like `/metrics` endpoint в Go.
- Добавлена fallback-валидация Python, чтобы сервис запускался без обязательной сборки Rust extension.
- Добавлен dashboard HTML endpoint, а не только JSON API.
- Добавлены CI и Kubernetes/HPA assets для проверки повышенной сложности.
- После падения CI исправлены версии toolchain, PyO3 test mode и хрупкий Go-тест.
- Realtime dashboard теперь использует WebSocket, а Python API запускает NATS consumer при старте.
