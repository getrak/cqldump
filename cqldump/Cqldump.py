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
            Método Main responsável por receber os
            parâmetros e invocar os outros métodos
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
        parser.add_argument('--w', '--where', metavar='Where',
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
        print(f'\n/** Início: {inicio} **/')
        print(f'/** Fim: {fim} **/')
        print(f'/** mpo total: {fim - inicio} **/')
        print("/** Dump successfully exported! **/")

    def connect(self, host, user, password, ssl, keyspace):
        """
            Função que faz a conexão via DataStax Driver ao Apache Cassandra
        """
        global cluster
        global session

        auth = ''
        if user != '' and password != '':
            auth = PlainTextAuthProvider(username=user, password=password)
            cluster = Cluster([host], auth_provider=auth)
        elif ssl != '':
            ssl_opts = {
                'ca_certs': ssl,
                'ssl_version': PROTOCOL_TLSv1,
                'cert_reqs': CERT_REQUIRED
            }
            cluster = Cluster([host], ssl_context=ssl_opts)
        else:
            cluster = Cluster([host])

        session = cluster.connect(keyspace)
        session.row_factory = dict_factory

    def read(self, table, where):
        """
            Função que constrói a query responsável por
            extrair os dados do Apacha Cassandra
        """
        query = f"SELECT * FROM {table}"
        if where:
            query += f" WHERE {where} limit 10 ALLOW FILTERING"
        return query

    def stdout(self, query, keyspace, table):
        """
            Função que imprimi o resultado do Dump para a saída padrão (stdout)
        """
        global cluster
        global session
        keyspace_metadata = cluster.metadata.keyspaces[keyspace]

        columns = keyspace_metadata.tables[table].columns
        str_columns = ""
        for col in columns:
            str_columns += col+", "
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
        for row in session.execute(statement):

            values = ""
            for col in columns:
                if (isinstance(row[col], int)):
                    values += (f"{row[col]}, ")
                else:
                    values += (f"'{row[col]}', ")
            values = values[:(len(values) - 2)]

            print(f"\nINSERT INTO {table} ({str_columns}) VALUES ({values});")
