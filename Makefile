.PHONY: test test-go test-python lint-go lint-python compose-up compose-down

test: test-go test-python

test-go:
	cd src/go-collector && go test ./...

test-python:
	cd src/python-analytics && python -m pytest ../../tests/python

lint-go:
	cd src/go-collector && gofmt -w . && go test ./...

lint-python:
	cd src/python-analytics && python -m ruff check .

compose-up:
	docker compose up --build

compose-down:
	docker compose down

