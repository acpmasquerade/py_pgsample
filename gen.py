#!/usr/bin/env python
from .creds import USER, PASSWORD, HOST, PORT, DB, SAMPLE_DB, START_TABLE, LIMIT

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
conn=psycopg2.connect(f"dbname={db} user={user} password={password} port={port} host={host}")
cur = conn.cursor()
tables = []
fkeys = {}

def get_tables():
    cur.execute(f"SELECT   * FROM   pg_catalog.pg_tables WHERE   schemaname != '{db}' AND schemaname != 'information_schema'; ")
    for a in cur.fetchall():
        tables.append(a)
        print("# TABLE --", a[1])

def describe(table):
    cur.execute(f"SELECT * FROM information_schema.COLUMNS WHERE TABLE_NAME = '{table}';")
    desc = cur.fetchall()
    print("# TABLE -- ", table)
    for d in desc:
        print(f"# - field - {d[2]} || {d[3]} || {d[4]} || {d[7]}")
    return desc


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


get_tables()
get_fkeys()

start_table_sample_rows = cur.execute(f"select * from {start_table} TABLESAMPLE SYSTEM_ROWS({limit});").fetchall()
print(first_table_sample_rows)

for tbl in tables:
    tbl_name = tbl[1]

