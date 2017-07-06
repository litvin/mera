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
elif ('-f' in sys.argv):
    print("chimera")
    cfg_file = "/root/scripts/mera/conf/chimera.conf"
elif ('-a' in sys.argv):
    print("transite1")
    cfg_file = "/root/scripts/mera/conf/transite1.conf"
elif ('-b' in sys.argv):
    print("transite2")
    cfg_file = "/root/scripts/mera/conf/transite2.conf"
elif ('-p' in sys.argv):
    print("Create parthinons.")
    cfg_file = "/root/scripts/mera/conf/mera.conf"
else:
    cfg_file = "/root/scripts/mera/conf/mera.conf"
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

   open(config.get("file", "work-dir")+config.get("file", "tmp-file"), 'w').close()
   tf = open(config.get("file", "work-dir")+config.get("file", "tmp-file"), 'a')
   f = open(config.get("file", "data-dir")+file_name, 'r')

   for line in f:
     _clear_var()
     for n in line.split(", "):
       for k in range(201, 223):
           if (config.get("mera-field", "type"+str(k))) in n:
            ddd[config.get("mera-field", "type"+str(k))]=n.split("=")[1].rstrip('\n')
                    
       if (config.get("mera-field", "ntype201")) in n:
            ddd[config.get("mera-field","ntype201")]=datetime.strptime(n.split("=")[1][:8]+" "+n.split("=")[1][19:], '%H:%M:%S %a %b %d %Y')
             
       elif config.get("mera-field", "ntype202") in n:
            ddd[config.get("mera-field","ntype202")]=datetime.strptime(n.split("=")[1][:8]+" "+n.split("=")[1][19:], '%H:%M:%S %a %b %d %Y')
       elif config.get("mera-field", "ntype203") in n:
            ddd[config.get("mera-field","ntype203")]=n.split("=")[1].split()[0]
       else: ddd[config.get("mera-field","ntype204")]=file_name

     ln = ddd['DIALPEER-NAME']+","+ddd['DISCONNECT-CODE-LOCAL']+","+ddd['DISCONNECT-CODE-Q931']+","+ddd['DST-CODEC']+","+ddd['DST-IP']+","+ddd['DST-NAME']+","+ddd['DST-NUMBER-BILL']+","+ddd['DST-NUMBER-IN']+","+ddd['DST-NUMBER-OUT']+","+ddd['ELAPSED-TIME']+","+ddd['HOST']+","+ddd['LAST-CHECKED-DIALPEER']+","+ddd['PDD-TIME']+","+ddd['QOS']+","+ddd['ROUTE-RETRIES']+","+ddd['SCD-TIME']+","+ddd['SRC-CODEC']+","+ddd['SRC-IP']+","+ddd['SRC-NAME']+","+ddd['SRC-NUMBER-BILL']+","+ddd['SRC-NUMBER-IN']+","+ddd['SRC-NUMBER-OUT']+","+str(ddd['DISCONNECT-TIME'])+","+str(ddd['SETUP-TIME'])+","+ddd['PDD-REASON']+","+ddd['LOG-NAME']+"\n"
 
     tf.write(ln)

   f.close()
   tf.close()

### insert data to db from csv 
def _data_insert():
   sqlstr = "COPY mera.all_logs FROM STDIN DELIMITER ',' CSV"
   with open(config.get("file", "work-dir")+config.get("file", "tmp-file"), 'r') as fi:
        cur.copy_expert(sqlstr, fi)
        print("Execute sql")
   print("Copy ok")
   fi.close()
   conn.commit()
   open(config.get("file", "work-dir")+config.get("file", "tmp-file"), 'w').close()
    
### list file inserted db
def _list_db_f():
###   query  = " SELECT log_name FROM mera.all_logs where setup_time < now() - interval '72 hour' group by log_name " 
   query  = "SELECT log_name FROM mera.all_logs where log_name like '"+ config.get("server", "prefix") +"%' group by log_name; "
   print(query)
   cur.execute(query)
   data =  cur.fetchall()
   list_add_file=[]
 
   for rec in data:
     list_add_file.append(str(rec[0]).split()[0])
   return(list_add_file)

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
   lf_db = _list_db_f()
   for filename in _list_l( config.get("file", "data-dir") ):
       if not filename in lf_db :
          print("New file: "+filename)
          _file_parse(filename)
          _data_insert()
          print("File "+filename+" inserted OK." )
       else:
          print("file in db: "+filename)
          

_data_test()

print("END")
