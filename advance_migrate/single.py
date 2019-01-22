import sys
from migrate import DataMigration
# from t_name import tables

database_old = "version_7_db"
database_new = "version_11_db"
user = "ubuntu"
password = "vvti_ajax"

# database_old = "live_db"
# database_new = "volk"
# user = "ramesh"
# password = "ramesh"

host = "127.0.0.1"
port = "5432"

table_name = sys.argv[1]
ext_from = sys.argv[2]
ext_till = sys.argv[3]

live = DataMigration(database_old, user, password, host, port)
new = DataMigration(database_new, user, password, host, port)
live.authentication()
new.authentication()

col_list = new.get_col_names(table_name)

for col in col_list:
    col_status = live.check_col_in_db(col, table_name)
    if col_status:
        rows = live.get_col_vals(col, table_name, ext_from, ext_till)
        new.advance_update_query(table_name, col, rows)

new.close()


