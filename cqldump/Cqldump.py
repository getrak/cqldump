# cqldump.py
from argparse import ArgumentParser
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.metadata import KeyspaceMetadata, TableMetadata
from cassandra.query import SimpleStatement
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime
from ssl import CERT_REQUIRED, PROTOCOL_TLSv1


class Cqldump():

    cluster = ''
    session = ''

    def main(self):
        """
            Main method responsible for receiving the
            parameters and invoke the other methods
        """
        inicio = datetime.now()

        parser = ArgumentParser(description='Extract dump for Cassandra DB')

        # HOST
        parser.add_argument('host', metavar='HOST',
                            help='Hosting connection with Cassandra', type=str)
        # KEYSPACE
        parser.add_argument('k', metavar='KEYSPACE',
                            help='Keyspace name', type=str)
        # TABLE
        parser.add_argument('t', metavar='TABLE',
                            help='Table name', type=str)
        # USER
        parser.add_argument('--u', '--user', metavar='User',
                            type=str)
        # PASSWORD
        parser.add_argument('--p', '--pass', metavar='Password',
                            type=str)
        # WHERE
        parser.add_argument('--w', '--where', metavar='Where Clause',
                            type=str)
        # SSL
        parser.add_argument('--ssl', help='Path to SSL key')

        args = parser.parse_args()  # GET PARAMS

        # CONNECT WITH CASSANDRA
        self.connect(args.host, args.u, args.p, args.ssl, args.k)
        # BUILD QUERY
        query = self.read(args.t, args.w)
        # WRITE IN .CQL FILE
        self.stdout(query, args.k, args.t)

        fim = datetime.now()

        # LOGS
        print(f'\n/** Start: {inicio} **/')
        print(f'/** End: {fim} **/')
        print(f'/** Duration: {fim - inicio} **/')
        print("/** Dump successfully exported! **/")

    def connect(self, host, user, password, ssl, keyspace):
        """
            Function that makes the connection to
            Apache Cassandra by DataStax Driver
        """

        self.session

        auth = ''
        if user != '' and password != '':
            auth = PlainTextAuthProvider(username=user, password=password)
            self.cluster = Cluster([host], auth_provider=auth)
        elif ssl != '':
            ssl_opts = {
                'ca_certs': ssl,
                'ssl_version': PROTOCOL_TLSv1,
                'cert_reqs': CERT_REQUIRED
            }
            self.cluster = Cluster([host], ssl_context=ssl_opts)
        elif user != '' and password != '' and ssl != '':
            ssl_opts = {
                'ca_certs': ssl,
                'ssl_version': PROTOCOL_TLSv1,
                'cert_reqs': CERT_REQUIRED
            }
            self.cluster = Cluster([host], auth_provider=auth,
                                   ssl_context=ssl_opts)
        else:
            self.cluster = Cluster([host])

        self.session = self.cluster.connect(keyspace)
        self.session.row_factory = dict_factory

    def read(self, table, where):
        """
            Function that builds the query responsible for
            extract data from Apacha Cassandra
        """
        query = f"SELECT * FROM {table}"
        if where:
            query += f" WHERE {where} ALLOW FILTERING"
        return query

    def stdout(self, query, keyspace, table):
        """
            Function that prints the Dump result to standard output (stdout)
        """

        keyspace_metadata = self.cluster.metadata.keyspaces[keyspace]

        query_columns = (f"select column_name, type from system_schema.columns \
                         where keyspace_name = '{keyspace}'")
        columns = self.session.execute(query_columns)
        array_col = []
        str_columns = ""
        for col in columns:
            array_col.append(col)
            str_columns += col['column_name']+", "
        str_columns = str_columns[:(len(str_columns) - 2)]

        create_keyspace_sql = keyspace_metadata.as_cql_query(). \
            replace("CREATE KEYSPACE", "CREATE KEYSPACE IF NOT EXISTS")
        create_table_sql = keyspace_metadata.tables[table].export_as_string() \
            .replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS") \
            .replace("}'", "}").replace("caching = '{", "caching = {") \
            .replace('"', "'")

        print(create_keyspace_sql)
        print(f"; \n\nUSE {keyspace};\n\n")
        print(create_table_sql)

        statement = SimpleStatement(query, fetch_size=80000)
        for row in self.session.execute(statement):
            values = ""
            for col in array_col:
                if(col['type'] != 'int'):
                    values += (f"'{row[col['column_name']]}', ")
                else:
                    values += (f"{row[col['column_name']]}, ")

            values = values[:(len(values) - 2)]
            print(f"\nINSERT INTO {table} ({str_columns}) VALUES ({values});")
