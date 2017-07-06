#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys,  pprint, re, os, time 
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
    print("Exit")
    quit()


### Full patch to cfg file
#cfg_file = "/root/scripts/mera/conf/mera.conf"
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.read(cfg_file)

### list files find in data dir
def _list_l(directory):
   list_file=[]
   list=os.listdir(directory)
   for file in list:

    if file.startswith(config.get("server", "prefix")):
      list_file.append(file)
      a = os.stat(os.path.join(directory,file))
 
      print(a.st_ctime)     
      print(file + " " + time.ctime(a.st_ctime))

   return(list_file)


_list_l( config.get("file", "data-dir"))


print(datetime.today())
print(date.today())
print(timedelta(days=7))
print(date.today()-timedelta(days=7))
print(datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p'))


print("END")
