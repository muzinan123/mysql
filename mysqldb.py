#!/usr/bin/env python
import time, MySQLdb    
      
conn=MySQLdb.connect(host="127.0.0.1",user="root",passwd="",port=3315)  
cursor = conn.cursor()    
      
#sql = "show global variables like 'max%';"   
sql = "show slave status;"   
cursor.execute(sql)    
s = cursor.fetchall()
print s
   
