import sqlite3

conn = sqlite3.connect("zimstats.sqlite")
cur = conn.cursor()

cur.execute("PRAGMA table_info('LONG CPI WEIGHTED ANUUAL SUMMARY SHEET1')")
cols = cur.fetchall()

for c in cols:
    print(c)
