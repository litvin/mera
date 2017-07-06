#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys,  pprint, re, os 
from datetime import date, datetime, timedelta
import ConfigParser
import io
import psycopg2


if ('-c' in sys.argv):
    print("corp")
    cfg_file = "/root/scripts/mera/conf/corp.conf"
elif ('-n' in sys.argv):
    print("neoba")
    cfg_file = "/root/scripts/mera/conf/neoba.conf"
elif ('-h' in sys.argv):
    print("hecate")
    cfg_file = "/root/scripts/mera/conf/hecate.conf"
elif ('-a' in sys.argv):
    print("transite1")
    cfg_file = "/root/scripts/mera/conf/transite1.conf"
elif ('-b' in sys.argv):
    print("transite2")
    cfg_file = "/root/scripts/mera/conf/transite2.conf"
else:
    cfg_file = "/root/scripts/mera/conf/mera.conf"
    os.system("/root/scripts/mera/bin/create_check_log.py 1")
    print("Exit")
    quit()

### Full patch to cfg file
#cfg_file = "/root/scripts/mera/conf/mera.conf"
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

ddd = {}
cur = conn.cursor()

### clear variables
def _clear_var():
  ln = ""
  ddd[config.get("mera-field", "ntype201")]=""
  ddd[config.get("mera-field", "ntype202")]=""
  ddd[config.get("mera-field", "ntype203")]=""
  for k in range(201, 223):
    ddd[config.get("mera-field", "type"+str(k))]=""
  return()

### Parse file ewsd.log
def _file_parse(file_name):
   list_dir_file_count = {}
   open(config.get("file", "work-dir")+config.get("file", "tmp-file"), 'w').close()
   f = open(config.get("file", "data-dir")+file_name, 'r')
   list_dir_file_count[file_name] = len(f.readlines())
   f.close()
   return(list_dir_file_count)   
 
### list file inserted db
def _list_db_f():
   query  = "select * from mera.check_log"
   cur.execute(query)
   data =  cur.fetchall()
   list_add_file=[]
   list_add_file_count={}
 
   for rec in data:
     list_add_file.append(str(rec[0]).split()[0])
     list_add_file_count[(str(rec[0]).split()[0])] = str(rec[1]).split()[0]

   return(list_add_file, list_add_file_count)

### list files find in data dir
def _list_l(directory):
   list_file=[]
   list=os.listdir(directory)
   for file in list:
    if file.startswith(config.get("server", "prefix")):
      list_file.append(file)
   return(list_file)

### test file in db or data dir
def _data_test():
   err = 0
   err_f = ""
   lf_db, c_db = _list_db_f()
   for filename in _list_l( config.get("file", "data-dir") ):
       if not filename in lf_db :
          print("New file: "+filename)
       else:
          print("File saved in db: "+filename)
          if c_db[filename] not in str(_file_parse(filename)[filename]):
              err = err + 1
              err_f = err_f + "'" + filename + "'" + ", "
              print("+++++++++ERROR FILE++++++++++")
              print(c_db[filename]) 
              print(_file_parse(filename)[filename])
              print(filename)  
              print("+++++++++++++++++++++++++++++")
   err_f = str(err) + "Sql: " + "DELETE FROM mera.all_logs WHERE log_name IN ("+ err_f + "'end')"
   return(err_f)

err_f = _data_test()

print("END error files: "+str(err_f))

