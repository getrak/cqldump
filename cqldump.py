# cqldump.py
#! /usr/bin/env python
from argparse import ArgumentParser
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.metadata import KeyspaceMetadata, TableMetadata
from datetime import datetime

parser = ArgumentParser()
parser.add_argument('--h',type=str) #HOST
parser.add_argument('--w',type=str) #WHERE
parser.add_argument('--k',type=str) #KEYSPACE
parser.add_argument('--t',type=str) #TABLE

args = parser.parse_args()

cluster = Cluster([args.h])
session = cluster.connect(args.k)
session.row_factory = dict_factory

columns =  cluster.metadata.keyspaces[args.k].tables[args.t].columns

# QUERY
query = "select * from " + args.t
if args.w:
    query += " where "+args.w
query += " limit 10;"
rows = session.execute(query)

str_columns = ""
for col in columns:
        str_columns += col+", "

# WRITE IN FILE .CQL
data_atual = datetime.now().strftime('%d%m%Y%H%M%S')
with open(f'cqldump_{data_atual}.cql', 'w') as f:

    create_keyspace_sql = cluster.metadata.keyspaces[args.k].as_cql_query().replace("CREATE KEYSPACE","CREATE KEYSPACE IF NOT EXISTS")
    create_table_sql = cluster.metadata.keyspaces[args.k].tables[args.t].export_as_string().replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS").replace("}'","}").replace("caching = '{","caching = {").replace('"',"'")

    f.write(create_keyspace_sql)
    f.write(f"; \n\nUSE {args.k};\n\n")
    f.write(create_table_sql)
    
    for row in rows:
        vals = ""
        f.write(f"\n\nINSERT INTO {args.t} ({str_columns[:(len(str_columns) - 2)]}) VALUES (")
        for col in columns:
            vals += (f"{row[col]}, ") if isinstance(row[col], int) else (f"'{row[col]}', ")
        f.write(f"{vals[:(len(vals) - 2)]});")

f.close

print("Dump successfully exported!")