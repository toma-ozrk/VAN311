from tqdm import tqdm

from van311.database import _seed_month, create_db_table, get_db_connection
from van311.utils import subtract_months_from_today


def seed_database():
    try:
        with (
            get_db_connection() as con,
            tqdm(total=59000, desc="😋 Seeding Database") as pbar,
        ):
            create_db_table(con)
            for i in reversed(range(0, 3)):
                month, year = subtract_months_from_today(i)
                _seed_month(con, month, year, pbar)

            con.commit()
        print("--- Finished database seeding ---")

    except Exception as e:
        print(f"CRITICAL ERROR during seeding: {e}")


if __name__ == "__main__":
    seed_database()
