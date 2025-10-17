# Makefile for GCP ETL Pipeline
# Quick commands for testing, linting, and development

.PHONY: help install test test-api test-pg test-bq coverage clean lint format

# Default target
help:
	@echo "GCP ETL Pipeline - Available Commands"
	@echo "======================================"
	@echo ""
	@echo "Setup:"
	@echo "  make install          Install all dependencies"
	@echo "  make install-test     Install test dependencies only"
	@echo ""
	@echo "Testing:"
	@echo "  make test             Run all unit tests"
	@echo "  make test-api         Run API extraction tests"
	@echo "  make test-pg          Run PostgreSQL tests"
	@echo "  make test-bq          Run BigQuery tests"
	@echo "  make coverage         Run tests with coverage report"
	@echo "  make coverage-html    Generate HTML coverage report"
	@echo ""
	@echo "Code Quality:"
	@echo "  make lint             Run code linting"
	@echo "  make format           Format code with black"
	@echo "  make clean            Remove test artifacts"
	@echo ""

# Installation
install:
	pip install -r requirements.txt
	pip install -r requirements-test.txt

install-test:
	pip install -r requirements-test.txt

# Testing
test:
	pytest -v

test-api:
	pytest tests/test_api_extraction.py -v

test-pg:
	pytest tests/test_postgresql_loader.py -v

test-bq:
	pytest tests/test_bigquery_writer.py -v

coverage:
	pytest --cov=. --cov-report=term-missing

coverage-html:
	pytest --cov=. --cov-report=html
	@echo "Coverage report generated: open htmlcov/index.html"

# Code Quality
lint:
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

format:
	black *.py tests/*.py

# Cleanup
clean:
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf *.pyc
	rm -rf __pycache__/
	rm -rf tests/__pycache__/
	rm -rf validation_issues_log.csv
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
