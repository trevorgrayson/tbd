export PYTHON?=python3
export VENV?=.venv

test: .venv
	./.venv/bin/coverage run --source=tbd -m pytest
	./.venv/bin/coverage report

.venv: requirements.txt
	$(PYTHON) -m pip install -r requirements.txt -t .venv
	touch .venv

install:
	python3 setup.py install --user

.PHONY: test install