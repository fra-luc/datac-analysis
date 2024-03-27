check: format-check lint type-check

format:
	poetry run black src
	poetry run isort src

format-check:
	poetry run black --check src
	poetry run isort --check src

lint:
	poetry run pylint src

release:
	poetry run pyinstaller src/main.py -n datac-consumer

type-check:
	poetry run mypy src