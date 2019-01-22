import os
import psycopg2


ALTER_TABLE = "ALTER TABLE {0}"
ALTER_COLUMN = "ALTER COLUMN {0} drop NOT NULL"
# CMD = """PGPASSWORD=ramesh psql -U ramesh {0} -c 'COPY {1} TO stdout' | PGPASSWORD=ramesh psql -U ramesh {2} -c 'COPY {3}(id) FROM stdin'"""
# DB_TRANSFER = """PGPASSWORD=ramesh psql -U ramesh {0} -c 'COPY {1} TO stdout' | PGPASSWORD=ramesh psql -U ramesh {2} -c 'COPY {3} FROM stdin'"""
CMD = """PGPASSWORD=vvti_ajax psql -U ubuntu {0} -c 'COPY {1} TO stdout' | PGPASSWORD=vvti_ajax psql -U ubuntu {2} -c 'COPY {3}(id) FROM stdin'"""
DB_TRANSFER = """PGPASSWORD=vvti_ajax psql -U ubuntu {0} -c 'COPY {1} TO stdout' | PGPASSWORD=vvti_ajax psql -U ubuntu {2} -c 'COPY {3} FROM stdin'"""
UPDATE_SQL = """ UPDATE {0} SET {1} WHERE {2};"""


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

    def get_col_string(self, data_list):
        cols_string = False
        for datum in data_list:
            if data_list[0] != datum:
                cols_string = "{0}, \n {1}".format(cols_string, datum)
            else:
                cols_string = "{0}".format(datum)

        return cols_string

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

    def data_copy(self, source_db, dest_db, table_name, in_list):
        "psql source_database -c 'COPY table TO stdout' | psql target_database -c 'COPY table FROM stdin'"

        if in_list:
            query = "(SELECT id from {0} where id in ({1}))".format(table_name, in_list)
        else:
            query = "(SELECT id from {0})".format(table_name)

        cmd = CMD.format(source_db, query, dest_db, table_name)
        os.system(cmd)

    def table_transfer(self, source_db, dest_db, table_name):
        cmd = DB_TRANSFER.format(source_db, table_name, dest_db, table_name)
        os.system(cmd)

    def get_updated_ids_list(self, table_name, ext_from, ext_till):
        query = "SELECT id from {0} where id > {1} and id < {2}".format(table_name, ext_from, ext_till)
        h = self.query_read(query)

        recs = h.fetchall()

        data_list = []
        for rec in recs:
            data_list.append(rec[0])

        ids = self.list_to_text(data_list)

        return ids

    def get_row_vals(self, col_list, table_name):
        col_string = self.get_col_string(col_list)

        if col_string:
            query = "SELECT id, {0} from {1} limit 10;".format(col_string, table_name)
            data = self.query_read(query)
            data_list = data.fetchall()

            return data_list

    def update_query(self, table_name, col_list, rows):
        if col_list:
            for row in rows:
                query = False
                for col in range(0, len(col_list)):
                    if col == 0:
                        query = "{0}={1}".format(col_list[col], self.convert_type(row[col + 1]))
                    else:
                        query = "{0}, {1}={2}".format(query, col_list[col], self.convert_type(row[col + 1]))

                update_sql = UPDATE_SQL.format(table_name, query, "id={0}".format(row[0]))
                # print update_sql
                self.query_write(update_sql)

    def convert_type(self, data):
        if data:
            if isinstance(data, int):
                return data
            else:
                return "'{0}'".format(str(data))
        else:
            return "NULL"

    def close(self):
        self.cr.close()
        self.cr = None

    def split(self, arr, size):
        arrs = []
        while len(arr) > size:
            pice = arr[:size]
            arrs.append(pice)
            arr = arr[size:]
        arrs.append(arr)
        return arrs




