test:
	uv run pytest

dev:
	uv run uvicorn van311.main:app --reload

seed:
	uv run python -m van311.db.seeding

schedule-jobs:
	uv run python -m van311.jobs.scheduler
