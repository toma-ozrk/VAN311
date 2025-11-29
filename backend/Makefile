test:
	uv run pytest -v

dev:
	uv run uvicorn van311.main:app --reload

seed-db:
	docker compose --profile seed -f docker/docker-compose.yml run seed

build:
	docker compose -f docker/docker-compose.yml build

run:
	docker compose -f docker/docker-compose.yml up
