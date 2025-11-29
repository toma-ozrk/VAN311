from .aggregation import calculate_metrics
from .seeding import seed_database


def main():
    seed_database()
    calculate_metrics()


if __name__ == "__main__":
    main()
