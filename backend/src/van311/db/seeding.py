from time import sleep

from tqdm import tqdm

from ..utils.dates import subtract_months_from_today
from .core import _seed_helper, create_db_table, get_db_connection

SLEEP_INTERVAL = 1.8


def seed_database():
    try:
        with (
            get_db_connection() as con,
            tqdm(
                total=70000, desc="😋 Seeding Database (estimated time / items)"
            ) as pbar,
        ):
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            create_db_table(con)

            month, year = subtract_months_from_today(2)

            ts = f"{year}-{month}-01T00:00:00+00:00"
            while True:
                ts = _seed_helper(con, ts, pbar)

                if not ts:
                    break

                con.commit()
                sleep(SLEEP_INTERVAL)

            # con.commit()
        print("--- Finished database seeding ---")

    except Exception as e:
        print(f"CRITICAL ERROR during seeding: {e}")


if __name__ == "__main__":
    seed_database()
