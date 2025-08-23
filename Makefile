check:
	uv run pytest -s --cov=lupyne.engine tests/test_engine.py
	uv run pytest -s --cov-append --cov=lupyne.services tests/test_rest.py tests/test_graphql.py

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy -p lupyne.engine

html:
	uv run --with lupyne mkdocs build
