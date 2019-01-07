from table_list import DataMigration
from t_name import tables


database_old = "live_db"
database_new = "volk"
user = "ramesh"
password = "ramesh"
host = "127.0.0.1"
port = "5432"

table_name = "account_account"


live = DataMigration(database_old, user, password, host, port)
new = DataMigration(database_new, user, password, host, port)
live.authentication()
new.authentication()

live.update_query(table_name)

new.close()


