install:
	uv sync

pylock:
	uv export -o pylock.toml

test:
	uv run pytest

run:
	uv run raw-transactions-handler

format:
	uv run ruff format

lint:
	uv run ruff check .

clean:
	find . -type d -name '__pycache__' -exec rm -r {} +

help:
	uv run raw-transactions-handler --help
