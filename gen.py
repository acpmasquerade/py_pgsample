#!/usr/bin/env python
from creds import USER, PASSWORD, HOST, PORT, DB, SAMPLE_DB, START_TABLE, LIMIT

user=USER
password=PASSWORD
host=HOST
port=PORT
db=DB
sampledb=SAMPLE_DB

start_table=START_TABLE
limit=LIMIT

# ------ code ---------
import psycopg2
import math
import json

conn=psycopg2.connect(f"dbname={db} user={user} password={password} port={port} host={host}")
cur = conn.cursor()
cur.execute("set statement_timeout=0;")
tables = []
fkeys = {}
refs_db = {}

def get_tables():
    cur.execute(f"SELECT   * FROM   pg_catalog.pg_tables WHERE   schemaname != '{db}' AND schemaname != 'information_schema'; ")
    for a in cur.fetchall():
        tables.append(a)
        #print("# TABLE --", a[1])
    print(f"-- Found {len(tables)} tables --")

def describe(tablename):
    cur.execute(f"SELECT * FROM information_schema.COLUMNS WHERE TABLE_NAME = '{tablename}';")
    return cur.fetchall()

def columns(tablename):
    cur.execute(f"select column_name, ordinal_position from information_schema.COLUMNS where TABLE_NAME = '{tablename}'")
    return cur.fetchall()

def get_fkeys(tablename=None):
    global fkeys
    if not fkeys:
        cur.execute("""SELECT conrelid::regclass AS "FK_Table"
      ,CASE WHEN pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %' THEN substring(pg_get_constraintdef(c.oid), 14, position(')' in pg_get_constraintdef(c.oid))-14) END AS "FK_Column"
      ,CASE WHEN pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %' THEN substring(pg_get_constraintdef(c.oid), position(' REFERENCES ' in pg_get_constraintdef(c.oid))+12, position('(' in substring(pg_get_constraintdef(c.oid), 14))-position(' REFERENCES ' in pg_get_constraintdef(c.oid))+1) END AS "PK_Table"
      ,CASE WHEN pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %' THEN substring(pg_get_constraintdef(c.oid), position('(' in substring(pg_get_constraintdef(c.oid), 14))+14, position(')' in substring(pg_get_constraintdef(c.oid), position('(' in substring(pg_get_constraintdef(c.oid), 14))+14))-1) END AS "PK_Column"
FROM   pg_constraint c
JOIN   pg_namespace n ON n.oid = c.connamespace
WHERE  contype IN ('f', 'p ')
AND pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %'
ORDER  BY pg_get_constraintdef(c.oid), conrelid::regclass::text, contype DESC;
""")
        for kk in cur.fetchall():
            pk_table = kk[0].replace('"', "")
            if pk_table not in fkeys:
                fkeys[pk_table] = []
            fkeys[pk_table].append((kk[1], (kk[2].replace('"', ""), kk[3])))
    if tablename:
        return fkeys.get(tablename, [])

def approximate(tablename):
    cur.execute(f"SELECT reltuples AS approximate_row_count FROM pg_class WHERE relname = '{tablename}'; ")
    return int(cur.fetchone()[0])

def extract(tablename, save=True):
    approx = approximate(tablename)
    print(f"# Table {tablename} ~ apx. {approx} rows ") 
    if approx > limit:
        sample_percentage =  limit / approx * 100
        sample_query = f"select * from {tablename} TABLESAMPLE SYSTEM({sample_percentage}) REPEATABLE (60);"
        cur.execute(sample_query)
    else:
        cur.execute(f"select * from {tablename} ")

    sampleset = cur.fetchall()

    refs = fkeys.get(tablename)
    cols = columns(tablename)

    if save is True:
        with open(f"dataset/{tablename}.json", "w+") as fobj:
            for single in sampleset:
                fobj.write(json.dumps([str(col) for col in single]))

    return sampleset 
    

get_tables()
get_fkeys()

# build refs set
for tbl in tables:
    tablename = tbl[1]
    refs_db[tablename] = {}
    for key in fkeys.get(tablename, []):
        fieldname = key[0]
        refs_db[tablename][fieldname] = []

# Begin
print(f"Starting with {start_table}") 
extract(start_table)

for tbl in tables:
    tbl_name = tbl[1]

