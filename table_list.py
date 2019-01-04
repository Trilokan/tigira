import os
import psycopg2


ALTER_TABLE = "ALTER TABLE {0}"
ALTER_COLUMN = "ALTER COLUMN {0} drop NOT NULL"
CMD = """PGPASSWORD=ramesh psql -U ramesh {0} -c 'COPY {1} TO stdout' | PGPASSWORD=ramesh psql -U ramesh {2} -c 'COPY {3}(id) FROM stdin'"""
DB_TRANSFER = """PGPASSWORD=ramesh psql -U ramesh {0} -c 'COPY {1} TO stdout' | PGPASSWORD=ramesh psql -U ramesh {2} -c 'COPY {3} FROM stdin'"""


class DataMigration:

    def __init__(self, db, usr, passwd, host, port):
        self.db = db
        self.usr = usr
        self.passwd = passwd
        self.host = host
        self.port = port
        self.cr = None

    def authentication(self):
        self.cr = psycopg2.connect(database=self.db,
                                   user=self.usr,
                                   password=self.passwd,
                                   host=self.host,
                                   port=self.port)

    def query_read(self, query):
        cr = self.cr.cursor()
        cr.execute(query)
        return cr

    def query_write(self, query):
        cr = self.cr.cursor()
        cr.execute(query)
        self.cr.commit()
        return True

    def get_col_names(self, table_name):
        model = self.query_read("select * from {0} limit 1".format(table_name))

        data_list = []

        for data_types in model.description:
            data_list.append(data_types[0])

        data_list.remove('id')

        return data_list

    def list_to_text(self, recs):
        data = ""
        for rec in recs:
            if recs[0] != rec:
                data = "{0}, {1}".format(data, rec)
            else:
                data = "{0}".format(rec)

        return data

    def remove_not_null(self, table_name):
        tables = self.get_col_names(table_name)

        query = ""
        for table in tables:
            if tables[0] != table:
                query = "{0}, \n {1}".format(query, ALTER_COLUMN.format(table))
            else:
                query = "{0}".format(ALTER_COLUMN.format(table))

        query = "{0} \n {1}".format(ALTER_TABLE.format(table_name), query)

        self.query_write(query)
        return True

    def data_copy(self, source_db, dest_db, table_name, not_in_id):
        "psql source_database -c 'COPY table TO stdout' | psql target_database -c 'COPY table FROM stdin'"

        if not_in_id:
            query = "(SELECT id from {0} where id not in ({1}))".format(table_name, not_in_id)
        else:
            query = "(SELECT id from {0})".format(table_name)

        cmd = CMD.format(source_db, query, dest_db, table_name)
        os.system(cmd)

    def table_transfer(self, source_db, dest_db, table_name):
        cmd = DB_TRANSFER.format(source_db, table_name, dest_db, table_name)
        os.system(cmd)

    def get_updated_ids_list(self, table_name):
        query = "SELECT id from {0}".format(table_name)
        h = self.query_read(query)

        recs = h.fetchall()

        data_list = []
        for rec in recs:
            data_list.append(rec[0])

        ids = self.list_to_text(data_list)

        return ids

    def close(self):
        self.cr.close()
        self.cr = None




