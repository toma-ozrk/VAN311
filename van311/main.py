from database import upsert_service_requests
from seed_database import seed_database

# UPSERT SERVICE REQUESTS
# con = get_db_connection()
# data = fetch_requests()
# cur = con.cursor()
# cur.execute(
#     """CREATE TABLE IF NOT EXISTS service_requests(department TEXT, issue_type TEXT,
#     status TEXT, closure_reason TEXT, open_ts TEXT, close_ts TEXT, modified_ts TEXT,
#     address TEXT, local_area TEXT, channel TEXT, lat TEXT, long TEXT, id TEXT PRIMARY KEY)"""
# )
# upsert_service_requests(con, data)

# SEED DATABASE
# con = get_db_connection()
# create_db_table(con)
# seed_database(con)
