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
ext_from = int(sys.argv[2])
ext_till = int(sys.argv[3])

live = DataMigration(database_old, user, password, host, port)
new = DataMigration(database_new, user, password, host, port)
live.authentication()
new.authentication()

# Remove NOT NULL Constraints
new.remove_not_null(table_name)

# GET id list in db
actual_ids = live.get_updated_ids_list(table_name, ext_from, ext_till)
updated_ids = new.get_updated_ids_list(table_name, ext_from, ext_till)

set_ids = list(set(actual_ids) - set(updated_ids))

new_ids = new.list_to_text(set_ids)

# Update all id in new db
new.data_copy(database_old, database_new, table_name, new_ids)

new.close()

