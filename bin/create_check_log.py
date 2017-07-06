#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys,  pprint, re, os 
from datetime import date, datetime, timedelta
import ConfigParser
import io
import psycopg2

cfg_file = "/root/scripts/mera/conf/mera.conf"
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.read(cfg_file)

conn = psycopg2.connect( 
database=config.get("pg", "db"),
user=config.get("pg", "user"),
password=config.get("pg", "passwd"),
host=config.get("pg", "host"),
port=config.get("pg", "port")
)

print("Database Connected....")

cur = conn.cursor()
 
### drop and create db
def _db_f():

   query  = "DROP TABLE IF EXISTS mera.check_log"
   cur.execute(query)
   conn.commit()

   query  = "CREATE TABLE mera.check_log as SELECT log_name, count(*) FROM mera.all_logs group by 1"
   cur.execute(query)
   conn.commit()

_db_f();

