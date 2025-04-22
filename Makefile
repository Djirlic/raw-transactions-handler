install:
	uv sync

pylock:
	uv export -o pylock.toml

test:
	uv run pytest

run:
	uv run raw-transactions-handler

format:
	uv run black .

lint:
	uv run flake8 .

clean:
	find . -type d -name '__pycache__' -exec rm -r {} +

help:
	uv run raw-transactions-handler --help