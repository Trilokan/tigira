from table_list import DataMigration
from t_name import tables


database_old = "version_7_db"
database_new = "version_11_db"
user = "ubuntu"
password = "vvti_ajax"
host = "127.0.0.1"
port = "5432"

table_name = "account_account"


for table_name in tables:
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


