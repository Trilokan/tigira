import sys
from migrate import DataMigration

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

live = DataMigration(database_old, user, password, host, port)
new = DataMigration(database_new, user, password, host, port)
live.authentication()
new.authentication()

try:
    col_list = new.get_col_names(table_name)
    rows = live.get_row_vals(col_list, table_name)

    new.update_query(table_name, col_list, rows)

except:
    print table_name

new.close()


