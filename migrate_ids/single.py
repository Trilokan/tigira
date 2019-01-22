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
    # Remove NOT NULL Constraints
    new.remove_not_null(table_name)

    # GET id list in new db
    ids = new.get_updated_ids_list(table_name)

    # Update all id in new db
    new.data_copy(database_old, database_new, table_name, ids)

except:
    new.table_transfer(database_old, database_new, table_name)

new.close()


