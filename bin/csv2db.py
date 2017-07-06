#!/usr/bin/python

from datetime import date, datetime, timedelta
import ConfigParser, sys, io, psycopg2

# Full patch to cfg file
cfg_file = "../loggist.conf"

config = ConfigParser.RawConfigParser(allow_no_value=True)
config.read(cfg_file)

conn = psycopg2.connect(database="loggist", user="", password="", host="", port="5432")
print("Database Connected....")
print(sys.argv[1])

# Create tmp table
def _create_table():
   cur = conn.cursor()
   cur.execute("DROP TABLE IF EXISTS mal.csv;")
   cur.execute("CREATE TABLE mal.csv(DatePacket timestamp, duration integer);")

   sqlstr = "COPY mal.csv FROM STDIN DELIMITER ',' CSV"
   with open(sys.argv[1], 'r') as fi:
        cur.copy_expert(sqlstr, fi)
   print("Copy ok")
   print("Table mal.csv Created.")
   conn.commit()

_create_table()
conn.close()
