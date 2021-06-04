import pandas as pd

from db.database import get_connection, close_connection

conn, cursor = get_connection()

sql_company_info = """select * from tbl_company where Code='FPT' limit 1"""
info = pd.DataFrame(pd.read_sql_query(sql_company_info, conn))

print(info.iloc[0]['Code'])

close_connection(conn)
