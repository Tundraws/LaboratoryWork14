# Rust validator

PyO3-расширение `metro_validator` выполняет быстрые проверки агрегатов пассажиропотока:

- счётчики неотрицательные;
- `average_per_tick` неотрицательный;
- `net_flow = entries - exits`.

Python-сервис пытается подключить этот модуль через `build_validator()`. Если расширение не собрано, используется Pydantic fallback, поэтому локальные тесты не зависят от Rust toolchain.

