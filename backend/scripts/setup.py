from van311.db.metrics import calculate_metrics
from van311.db.seeding import seed_database

if __name__ == "__main__":
    seed_database()
    calculate_metrics()
