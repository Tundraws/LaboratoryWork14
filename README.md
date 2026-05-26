# Лабораторная работа №14

**Студент:** Мельникова Анастасия  
**Группа:** 220032-11  
**Вариант:** 13  
**Тип варианта:** повышенная сложность  
**Тема:** Анализ пассажиропотока  
**Источник данных:** эмуляция датчиков метро

## Описание программы

Проект реализует production-like конвейер обработки пассажиропотока метро:

```text
Go-сборщики датчиков -> tumbling window aggregation -> NATS / Apache Arrow
-> Python FastAPI analytics -> Polars cleanup -> Parquet -> DuckDB SQL
-> Plotly-визуализации и realtime dashboard
```

Go-сервис запускается в нескольких экземплярах, получает shard через etcd, эмулирует свою часть датчиков, валидирует события, агрегирует пассажиропоток по окнам и публикует агрегаты в NATS. Также сервис отдаёт текущий snapshot в Apache Arrow IPC stream через HTTP endpoint `/arrow`.

Python-сервис получает агрегаты из Arrow или NATS, валидирует данные через Pydantic и опциональный Rust/PyO3-модуль, сохраняет очищенные данные в Parquet, выполняет SQL-аналитику через DuckDB и показывает веб-дашборд.

## Технологии

- Go 1.24: конкурентный сборщик, `context.Context`, graceful shutdown, `slog`, etcd, NATS, Apache Arrow.
- Python 3.11+: FastAPI, Pydantic v2, Polars, DuckDB, PyArrow, Plotly, aiohttp.
- Rust 2021 + PyO3: библиотека валидации агрегатов.
- Docker Compose: etcd, NATS, Go collector, Python analytics.
- Kubernetes: Deployment, ConfigMap, HPA для автоскалирования Go-сборщика.
- CI: GitHub Actions для Go, Python и Rust тестов.

## Структура

```text
src/
  go-collector/        # распределённый Go-сборщик и Arrow/NATS transport
  python-analytics/    # FastAPI аналитика, Polars, DuckDB, dashboard
  python-benchmark/    # asyncio/aiohttp сборщик для сравнения Go vs Python
  rust-validator/      # PyO3-валидатор агрегатов
tests/
  python/              # unit/API тесты Python-сервиса
k8s/                   # Kubernetes Deployment, ConfigMap, HPA
```

## Сборка

Скопируйте пример окружения:

```powershell
Copy-Item .env.example .env
```

Сборка и запуск всех сервисов:

```powershell
docker compose up --build
```

Локальная проверка без Docker:

```powershell
cd src/go-collector
go test ./...

cd ..\..\src\rust-validator
cargo test

cd ..\..
python -m pip install -r src\python-analytics\requirements.txt
$env:PYTHONPATH="src/python-analytics/src"
python -m pytest tests/python
```

## Запуск

Go-сборщик локально:

```powershell
cd src/go-collector
$env:COLLECTOR_INSTANCE_ID="collector-1"
$env:COLLECTOR_TOTAL_SHARDS="3"
$env:COLLECTOR_ARROW_ADDR=":8080"
go run ./cmd/collector
```

Python API локально:

```powershell
$env:PYTHONPATH="src/python-analytics/src"
uvicorn metro_analytics.api.app:create_app --factory --host 0.0.0.0 --port 8000
```

Одноразовый импорт Arrow snapshot в Parquet:

```powershell
$env:PYTHONPATH="src/python-analytics/src"
python -m metro_analytics.cli --arrow-url http://localhost:8080/arrow --parquet-path data/passenger_flow.parquet
```

Сравнение производительности Python asyncio-сборщика:

```powershell
python src/python-benchmark/collector_benchmark.py --events 10000 --workers 8
```

## Примеры API

Health check Go-сборщика:

```powershell
Invoke-RestMethod http://localhost:8080/healthz
```

Получить агрегаты из Arrow и сохранить их через Python:

```powershell
Invoke-RestMethod -Method Post http://localhost:8000/aggregates/from-arrow
```

Получить SQL-сводку DuckDB:

```powershell
Invoke-RestMethod http://localhost:8000/summary
```

Открыть realtime dashboard:

```text
http://localhost:8000/
```

## Повышенная сложность

В проекте реализованы пункты повышенного уровня:

- распределённый Go-сборщик с координацией shard assignment через etcd и graceful fallback;
- оконная агрегация tumbling window на стороне Go;
- передача агрегатов через Apache Arrow IPC stream;
- Rust/PyO3-валидатор, подключаемый Python-сервисом;
- Dockerfile для каждого сервиса и docker-compose с healthchecks;
- Kubernetes Deployment и HPA;
- Python asyncio/aiohttp collector benchmark для сравнения с Go;
- потоковая передача агрегатов через NATS;
- веб-дашборд FastAPI с обновлением состояния.

## Тесты

```powershell
make test
```

Покрыты ключевые свойства:

- валидация событий и агрегатов;
- корректность `net_flow = entries - exits`;
- шардирование датчиков;
- оконная агрегация;
- API health и импорт из Arrow-клиента через fake dependency;
- Rust validation success/error cases.
