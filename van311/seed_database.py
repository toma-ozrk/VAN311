from time import sleep

from tqdm import tqdm

from van311.api import fetch_requests
from van311.database import get_db_connection, upsert_page_data
from van311.utils import subtract_months_from_today


def create_db_table(con):
    cur = con.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS service_requests(department TEXT, issue_type TEXT,
        status TEXT, closure_reason TEXT, open_ts TEXT, close_ts TEXT, modified_ts TEXT,
        address TEXT, local_area TEXT, channel TEXT, lat TEXT, long TEXT, id TEXT PRIMARY KEY)"""
    )


def _seed_month(con, month, year, pbar):
    j = 0
    while True:
        offset = j * 100
        data = fetch_requests(offs=offset, month=month, year=year, seeding=True)

        upsert_page_data(con, data)

        sleep(1.8)
        pbar.update(100)
        if len(data) < 100:
            break

        j += 1

    con.commit()


def seed_database(con):
    pbar = tqdm(total=59000, desc="😋 Seeding Database")

    for i in reversed(range(0, 3)):
        month, year = subtract_months_from_today(i)
        _seed_month(con, month, year, pbar)

    pbar.close()
    print("Task completed!")
    con.close()


con = get_db_connection()
create_db_table(con)
seed_database(con)
