#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import datetime
import time
import ConfigParser
import pyodbc
import MySQLdb

script_dir=os.path.split(os.path.realpath(__file__))[0]+'/'

config_file=script_dir+'/config.ini'
cp=ConfigParser.ConfigParser()
cp.read(config_file)



try:
    source_host=cp.get('mssql','host')
    source_port=int(cp.get('mssql','port'))
    source_user=cp.get('mssql','user')
    source_passwd=cp.get('mssql','passwd')
    source_db=cp.get('mssql','db')
    source_table=cp.get('mssql','table')
    source_pk=cp.get('mssql','pk')
    source_columns=cp.get('mssql','columns')
    source_pk_values=cp.get('mssql','pk_values')
    #try:
    #    source_start=int(cp.get('mssql','pk_min'))
    #except:
    #    source_start=0
    #try:
    #    source_end=int(cp.get('mssql','pk_max'))
    #except:
    #    source_end=0

    target_host=cp.get('mysql','host')
    target_port=int(cp.get('mysql','port'))
    target_user=cp.get('mysql','user')
    target_passwd=cp.get('mysql','passwd')
    target_db=cp.get('mysql','db')
    target_table=cp.get('mysql','table')
    target_charset=cp.get('mysql','charset')

    debug=int(cp.get('main','debug'))
    batch_count=int(cp.get('main','batch_count'))
    batch_sleep=int(cp.get('main','sleep'))
except :
    #raise ConfigParser.NoOptionError(e)
    print "config.ini ERROR.  You can copy it from config.ini.sample "
    exit()



conn=pyodbc.connect('DRIVER={SQL Server};SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s' 
    %(source_host,source_port,source_db,source_user,source_passwd))
source_cursor=conn.cursor()

link=MySQLdb.connect(target_host,target_user,target_passwd,target_db,charset=target_charset)
target_cursor=link.cursor()

skiped_line_count=0

#--- function start ---------------------------------------
def do_batch(source_cursor,target_table,column_names,batch_count):
    sql="insert into `%s` (`%s`) values(%s)" %(
            target_table, '`, `'.join(column_names),  ', '.join([r'%s']*len(column_names)))
    i=0
    skiped=0
    while True:
        i+=1;
        if i >= batch_count:
            break
        try:
            row=source_cursor.fetchone()
            if not row:
                print 'row null, break this batch (done)'
                break
            vs=[]
            vs=tuple([row.__getattribute__(column) for column in column_names])
            #print vs
            target_cursor.execute(sql, vs)
        except:
            #source_cursor.skip(1)
            skiped+=1
            print "[Notice] line error , skiped"
        ## debug for error, use the code below, for the exception name
        #except UnicodeDecodeError, e:
        #    source_cursor.skip(1)
        #    print "UnicodeDecodeError, skip"
        #except Exception, e:
        #    print 'str(Exception):\t', str(Exception)
        #    print 'str(e):\t\t', str(e)
        #    print 'repr(e):\t', repr(e)
        #    print 'e.message:\t', e.message
    return skiped
#--- function start ---------------------------------------



if source_pk_values:
    pkv=source_pk_values.split(',')
    pkv_size=len(pkv)
    print "source lines %s (defined by pk list)\n" %(pkv_size)
    for i in range(pkv_size//batch_count):
        sql="select %s from %s where %s in ( %s )" %(
                source_columns,source_table,source_pk, ','.join(pkv[i*batch_count:i*batch_count+batch_count]))
        source_cursor.execute(sql)
        column_names=[item[0] for item in source_cursor.description]
        skiped_line_count += do_batch(source_cursor,target_table,column_names,batch_count)
        if batch_sleep > 0:
            print "sleep %ss" %batch_sleep
            time.sleep(batch_sleep)
    #print column_names
else:
    source_cursor.execute("select min(%s) as min_pk,max(%s) as max_pk,count(*) as cnt from %s" %(
        source_pk,source_pk,source_table))
    source_min_pk,source_max_pk,source_cnt=source_cursor.fetchone()
    #source_start=max(source_start,source_min_pk)
    #source_end=min(source_end,source_max_pk)
    #source_max_pk=2000
    batch_start=source_min_pk // batch_count * batch_count
    print "source lines %s, id range [%s,%s].\ntask range [%s,%s] batch size %s \n" %(
        source_cnt,source_min_pk,source_max_pk,batch_start,source_max_pk,batch_count)

    while batch_start <= source_max_pk+1:
        print "[%s,%s)" %(batch_start,batch_start+batch_count)
        sql="select %s from %s where %s >= %s and %s < %s" %(
                source_columns,source_table,source_pk, batch_start,source_pk, batch_start+batch_count )
        source_cursor.execute(sql)

        column_names=[item[0] for item in source_cursor.description]
        skiped_line_count += do_batch(source_cursor,target_table,column_names,batch_count)
        #print column_names
        batch_start+=batch_count
        if batch_sleep > 0:
            print "sleep %ss" %batch_sleep
            time.sleep(batch_sleep)

print "\n\ndone"
if skiped_line_count :
    print "[Notice] %s lines skiped for error" %skiped_line_count

source_cursor.close()
target_cursor.close()
exit()




