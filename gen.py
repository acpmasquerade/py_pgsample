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
fkeys_db = {}

def get_tables():
    cur.execute(f"SELECT   * FROM   pg_catalog.pg_tables WHERE  schemaname != '{db}' AND schemaname != 'information_schema' AND schemaname != 'pg_catalog' ")
    for a in cur.fetchall():
        tables.append(a)
    #print(f"-- Found {len(tables)} tables --")

def describe(tablename):
    cur.execute(f"SELECT * FROM information_schema.COLUMNS WHERE TABLE_NAME = '{tablename}';")
    return cur.fetchall()

def columns(tablename):
    cur.execute(f"select column_name from information_schema.COLUMNS where TABLE_NAME = '{tablename}'")

    cols = {}
    for key, val in enumerate(cur.fetchall()):
        cols[val[0]] = key

    return cols

def get_fkeys():
    global fkeys_db
    if not fkeys_db:
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
            if pk_table not in fkeys_db:
                fkeys_db[pk_table] = {}
            fieldname = kk[1]
            to_table = kk[2].replace('"', "")
            to_field = kk[3]
            fkeys_db[pk_table][fieldname] = (to_table, to_field, [])

def approximate(tablename):
    cur.execute(f"SELECT reltuples AS approximate_row_count FROM pg_class WHERE relname = '{tablename}'; ")
    return int(cur.fetchone()[0])

def column_position(cols, colname):
    for cc in cols:
        if cc[0] == colname:
            return cc[1]

def extract(tablename, save=True):
    approx = approximate(tablename)
    print(f"# Table {tablename} ~ apx. {approx} rows ") 
    if approx > limit:
        sample_percentage =  limit / approx * 100
        sample_query = f"select * from \"{tablename}\" TABLESAMPLE SYSTEM({sample_percentage}) REPEATABLE (60);"
        cur.execute(sample_query)
    else:
        cur.execute(f"select * from \"{tablename}\"; ")

    sampleset = cur.fetchall()

    if save is True:
        with open(f"dataset/{tablename}.json", "w+") as fobj:
            for single in sampleset:
                fobj.write(json.dumps([str(col) for col in single]))

    cols = columns(tablename)

    for row in sampleset:
        for fkey in fkeys_db.get(tablename, {}):
            print("fk", tablename, fkey)
            col_pos = cols.get(fkey) 
            fkeys_db[tablename][fkey][2].append(row[col_pos])

    return sampleset 
    
get_tables()
get_fkeys()

# Begin
print(f"Starting with {start_table}") 
extract(start_table)
for tbl in tables:
    tablename = tbl[1]
    if tablename==start_table:
        continue
    if tablename.startswith("c"):
        break
    extract(tablename)

tables_extradata = {}
# Now Get the references and append to the files
for tablename, table_refs in fkeys_db.items():
    for fieldname, refinfo in table_refs.items():
        to_table, to_field, refs = refinfo
        print("--", tablename, fieldname,  to_table, to_field)
        if not refs:
            continue
        #_refs = [ str(s) if s is not None else None for s in set(refs) ]
        #_refs = #tuple([ str(s) if s is not None else None for s in set(refs)])
        _refs = tuple(set(refs))
        query=f"select * from {to_table} where {to_field} IN %s "
        cur.execute(query, (_refs,) )

        if to_table not in tables_extradata:
            tables_extradata[to_table] = []
        tables_extradata[to_table].append(cur.fetchall())

# Lets finalize everything
for tbl in tables_extradata:
    if not tables_extradata.get(tbl):
        continue
    with open(f"dataset/{tbl}.json", "w+") as f:
        sampleset = json.loads(f.read().strip() or "[]")
        for row in tables_extradata.get(tbl):
            print(row)
            sampleset.append([str(col) for col in row])
        f.write(json.dumps(sampleset))
