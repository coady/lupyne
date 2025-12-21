check:
	uv run pytest -s --cov=lupyne.engine tests/test_engine.py
	uv run pytest -s --cov-append --cov=lupyne.services tests/test_rest.py tests/test_graphql.py

lint:
	uvx ruff check
	uvx ruff format --check
	uvx ty check lupyne/engine

html:
	uv run --group docs -w . mkdocs build
