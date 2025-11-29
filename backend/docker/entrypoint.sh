#!/usr/bin/env sh
set -e

case "$1" in
    api)
        shift
        exec uv run uvicorn van311.main:app --host 0.0.0.0 --port 8000 "$@"
        ;;
    scheduler)
        shift
        exec uv run python -u -m van311.jobs.scheduler "$@"
        ;;
    seed-db)
        shift
        exec uv run python -m van311.db.db_init "$@"
        ;;
    *)
        exec "$@"
        ;;
esac
