import psycopg2
import os

database_old = "test_02"
database_new = "turbid_1"
user = "ramesh"
password = "ramesh"
host = "127.0.0.1"
port = "5432"

conn_old = psycopg2.connect(database=database_old, user=user, password=password, host=host, port=port)
conn_new = psycopg2.connect(database=database_new, user=user, password=password, host=host, port=port)

print "Opened database successfully"

cur_old = conn_old.cursor()
cur_new = conn_new.cursor()


cur_old.execute(''' select * from res_partner where id not in (1,3) limit 10''')
rows = cur_old.fetchall()
for row in rows:
    pass
    # print row


# Get column names
col_name_list = []

for col_name in cur_old.description:
    col_name_list.append(col_name[0])
    print col_name



conn_old.close()
conn_new.close()
