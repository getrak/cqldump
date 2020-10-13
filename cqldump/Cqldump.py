# cqldump.py
#! /usr/bin/env python
from argparse import ArgumentParser
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.metadata import KeyspaceMetadata, TableMetadata
from cassandra.query import SimpleStatement
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime

class Cqldump():

    cluster = ''
    session = ''

    def main(self):
        inicio = datetime.now()

        parser = ArgumentParser(description='Extract dump for Cassandra DB')
        parser.add_argument('host', metavar='HOST', help='Hosting connection with Cassandra',type=str) #HOST
        parser.add_argument('--u','--user',help='User',type=str) #USER
        parser.add_argument('--p','--pass',help='Password',type=str) #PASS
        parser.add_argument('--w','--where',help='Where Clause',type=str) #WHERE
        parser.add_argument('--k','--keyspace',help='Keyspace',type=str) #KEYSPACE
        parser.add_argument('--t','--table',help='Table',type=str) #TABLE  

        args = parser.parse_args() # GET PARAMS
        
        self.connect(args.host, args.u, args.p, args.k) # CONNECT WITH CASSANDRA
        query = self.read(args.t, args.w) # BUILD QUERY
        self.stdout(query, args.k, args.t) # WRITE IN .CQL FILE

        fim = datetime.now()

        # LOGS
        print(f'# Início: {inicio}')
        print(f'# Fim: {fim}')
        print(f'# mpo total: {fim - inicio}')
        print("# Dump successfully exported!")

    def connect(self, host, user, password, keyspace):
        global cluster 
        global session
        
        auth_provider = ''
        if user != '' and password != '':
            auth_provider = PlainTextAuthProvider(username=user, password=password)
            cluster = Cluster([host],auth_provider=auth_provider)
        else:
            cluster = Cluster([host])

        session = cluster.connect(keyspace)
        session.row_factory = dict_factory

    
    def read(self,table, where):
        query = f"SELECT * FROM {table}"
        if where:
            query += f" WHERE {where} limit 10 ALLOW FILTERING"
        return query

    def write(self, query, keyspace, table):
        global cluster
        global session
        keyspace_metadata = cluster.metadata.keyspaces[keyspace]

        columns =  keyspace_metadata.tables[table].columns
        str_columns = ""
        for col in columns:
            str_columns += col+", "
        str_columns = str_columns[:(len(str_columns) - 2)]
            
        name_file = f'cqldump_{datetime.now().strftime("%d%m%Y%H%M%S")}.cql'
        
        with open(name_file, 'w') as f:
            create_keyspace_sql = keyspace_metadata.as_cql_query().replace("CREATE KEYSPACE","CREATE KEYSPACE IF NOT EXISTS")
            create_table_sql = keyspace_metadata.tables[table].export_as_string().replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS").replace("}'","}").replace("caching = '{","caching = {").replace('"',"'")

            f.write(create_keyspace_sql)
            f.write(f"; \n\nUSE {keyspace};\n\n")
            f.write(create_table_sql)

            statement = SimpleStatement(query, fetch_size=80000)
            for row in session.execute(statement):

                values = ""
                for col in columns:
                    values += (f"{row[col]}, ") if isinstance(row[col], int) else (f"'{row[col]}', ")
                values = values[:(len(values) - 2)]
                

                f.write(f"\nINSERT INTO {table} ({str_columns}) VALUES ({values});")

        f.close
    
    def stdout(self, query, keyspace, table):
        global cluster
        global session
        keyspace_metadata = cluster.metadata.keyspaces[keyspace]

        columns =  keyspace_metadata.tables[table].columns
        str_columns = ""
        for col in columns:
            str_columns += col+", "
        str_columns = str_columns[:(len(str_columns) - 2)]
            
        create_keyspace_sql = keyspace_metadata.as_cql_query().replace("CREATE KEYSPACE","CREATE KEYSPACE IF NOT EXISTS")
        create_table_sql = keyspace_metadata.tables[table].export_as_string().replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS").replace("}'","}").replace("caching = '{","caching = {").replace('"',"'")

        print(create_keyspace_sql)
        print(f"; \n\nUSE {keyspace};\n\n")
        print(create_table_sql)

        statement = SimpleStatement(query, fetch_size=80000)
        for row in session.execute(statement):

            values = ""
            for col in columns:
                values += (f"{row[col]}, ") if isinstance(row[col], int) else (f"'{row[col]}', ")
            values = values[:(len(values) - 2)]
            

            print(f"\nINSERT INTO {table} ({str_columns}) VALUES ({values});")
