test:
	uv run pytest -v

dev:
	uv run uvicorn van311.main:app --reload

seed-db:
	uv run python -m van311.db.seeding

schedule-jobs:
	uv run python -m van311.jobs.scheduler
