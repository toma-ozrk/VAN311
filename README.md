# 🇨🇦 Vancouver 3-1-1 Service Analytics Dashboard

**Project Status:** Functional API and Data Pipeline Complete

This project is a back-end web service designed to ingest, analyze, and serve public civic data from the City of Vancouver's 3-1-1 Service Request portal. It functions as a resilient ETL (Extract, Transform, Load) pipeline, transforming raw government data into performance metrics for easy visualization and comparison.

## Architecture

This project was built to showcase data engineering practices and the ability to manage resource allocation and ensure data integrity in a web service environment.

### 1\. Data Pipeline (ETL)

The core logic is implemented in dedicated, isolated Python scripts to prevent the web service from being blocked by heavy I/O tasks.

- **Ingestion Worker (scripts/updates.py):** Will run continuously in the background to poll the external 3-1-1 API every 50 minutes.

- **Materialized Views:** Data is optimized for reporting by maintaining two tables:
  1.  service_requests: Stores the raw 56,000+ records and calculated raw metrics (e.g., time_to_resolve_days).
  2.  metric_aggregates: Stores pre-calculated, aggregated metrics (Averages, Counts, Backlog totals) across various dimensions for instant retrieval by the API.

- **Resource Safety:** All I/O heavy scripts are structured with try/finally blocks to guarantee the database connection is closed and unlocked after every run, ensuring resource integrity.

### 2\. Data Integrity

The system implements techniques to handle known deficiencies in the public API:

- **Guaranteed Unique Key:** Since the source API lacks a unique ID, a **Deterministic Hash Key**(hashlib.sha256) is synthesized from critical fields (department, type, timestamp). This ensures that the system can reliably **Upsert** (Update or Insert) records, preventing duplicates and tracking status changes (Open → Closed) accurately.

- **Defensive Mapping:** The ServiceRequest.dict_to_service_request method ensures all fields are defensively mapped using .get(key, default="") to prevent script crashes when the external API omits or reorders fields.

- **Statistical Accuracy:** The system correctly distinguishes between the **Total Workload** (Volume including NULL locations) and **Analyzable Performance** (Metrics calculated only on confirmed location data), ensuring all reported averages are statistically sound.

### 3\. Stability and Automation

- **Continuous Integration (CI):** Implemented using **GitHub Actions** to automatically run the test suite upon every commit or pull request. This ensures code merges only happen if all data contracts and business logic pass verification.

- **Comprehensive Testing:** The test suite focuses on the **Testing Pyramid** approach, prioritizing:
  - **Unit Tests of Flow:** Utilizing mocking to test orchestrator loops (while True) and resource cleanup contracts.
  - **Integration Tests:** WIP

## 📈 Implemented Metrics

| Metric        | Calculation                                  | Aggregation Levels                                                         |
| ------------- | -------------------------------------------- | -------------------------------------------------------------------------- |
| avg_ttr       | Average time to close a request              | City-Wide Filtered, By neighbourhood, By issue, Hyper-Local                |
| avg_ttu       | Average time to update a request             | City-Wide Filtered, By neighbourhood, By issue, Hyper-Local                |
| volume        | Count of all requests                        | City-Wide Filtered, By neighbourhood, By issue, Hyper-Local, Null location |
| open_requests | Count of all requests where status is 'Open' | City-Wide Filtered, By neighbourhood, By issue, Hyper-Local                |

## 🛠️ Technology Stack

- **Backend Framework:** Python (FastAPI)

- **CI/CD:** Github Actions, Pytest

- **Data Analysis:** Python

- **Database:** SQLite (SQLAlchemy ORM for future scaling planned)

- **Job Scheduling:** APScheduler

- **Portability:** Docker, Docker Compose

## Getting Started Locally

### Prerequisites

1.  Clone this repository.
2.  Make sure docker is installed.
3.  (Optional) Make sure make command is installed on your machine.

## Running the Project

### 1\. `cd ./backend`

### 2\. Build Docker Image (Run Once)

```bash
make build
```

or

```bash
docker compose -f docker/docker-compose.yml build
```

### 3\. Seed Database (Run Once)

```bash
make seed-db
```

or

```bash
docker compose --profile seed -f docker/docker-compose.yml run seed
```

### 4\. Running the Background Worker (For Continuous Updates)

```bash
make scheduler
```

or

```bash
docker compose -f docker/docker-compose.yml up
```

### 5\. Starting Frontend

- WIP
