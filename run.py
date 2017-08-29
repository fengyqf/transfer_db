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
except :
    #raise ConfigParser.NoOptionError(e)
    print "config.ini ERROR.  You can copy it from config.ini.sample "
    exit()



conn=pyodbc.connect('DRIVER={SQL Server};SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s' 
    %(source_host,source_port,source_db,source_user,source_passwd))
source_cursor=conn.cursor()

link=MySQLdb.connect(target_host,target_user,target_passwd,target_db,charset=target_charset)
target_cursor=link.cursor()

#--- function start ---------------------------------------
def do_batch(source_cursor,target_table,column_names,batch_count):
    sql="insert into `%s` (`%s`) values(%s)" %(
            target_table, '`, `'.join(column_names),  ', '.join([r'%s']*len(column_names)))
    i=0
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
            print "[Notice] something error with this line, skip for next line"
        ## debug for error, use the code below, for the exception name
        #except UnicodeDecodeError, e:
        #    source_cursor.skip(1)
        #    print "UnicodeDecodeError, skip"
        #except Exception, e:
        #    print 'str(Exception):\t', str(Exception)
        #    print 'str(e):\t\t', str(e)
        #    print 'repr(e):\t', repr(e)
        #    print 'e.message:\t', e.message

#--- function start ---------------------------------------



#TODO source_max_pk
source_cursor.execute("select min(%s) as min_pk,max(%s) as max_pk,count(*) as cnt from %s" %(source_pk,source_pk,source_table))
source_min_pk,source_max_pk,source_cnt=source_cursor.fetchone()
#source_start=max(source_start,source_min_pk)
#source_end=min(source_end,source_max_pk)
source_max_pk=2000
batch_start=source_min_pk // batch_count * batch_count
print "source lines %s, id range [%s,%s].\ntask range [%s,%s] batch size %s \n" %(
    source_cnt,source_min_pk,source_max_pk,batch_start,source_max_pk,batch_count)

if source_pk_values:
    print "source by pk list, %s" %(len(source_pk_values.split(',')))
    sql="select %s from %s where %s in ( %s )" %(
            source_columns,source_table,source_pk, source_pk_values)
    source_cursor.execute(sql)
    column_names=[]
    for item in source_cursor.description:
        column_names.append(item[0])
    #print column_names
    do_batch(source_cursor,target_table,column_names,batch_count)
else:
    while batch_start <= source_max_pk+1:
        print "[%s,%s)" %(batch_start,batch_start+batch_count)
        sql="select %s from %s where %s >= %s and %s < %s" %(
                source_columns,source_table,source_pk, batch_start,source_pk, batch_start+batch_count+1 )
        source_cursor.execute(sql)

        column_names=[]
        for item in source_cursor.description:
            column_names.append(item[0])

        do_batch(source_cursor,target_table,column_names,batch_count)
        #print column_names
        batch_start+=batch_count



source_cursor.close()
target_cursor.close()
exit()




















time_start=time.time()

stop_words_u=[it.decode('utf-8') for it in stop_words]

for file in os.listdir(script_dir):
    if not (file[0:2] in ['a_','a.'] and file[-4:]=='.txt') :
        continue
    r=open(script_dir+file)
    counts={}
    print "File: %s" %file
    for word_width in range(word_width_min,word_width_max+1):
        r.seek(0)
        print "  %d char-width words" %word_width
        for line in r.readlines():
            if debug:
                print line
            line_u=line.decode('utf-8')
            line_u_len=len(line_u)
            i=0;
            accepted_count=0;
            while(i < line_u_len-word_width):
                i+=1;
                word = line_u[i:(i+word_width)]
                flag_stop=0
                for sw in stop_words_u:
                    if word.find(sw) >= 0:
                        #print '  stoped for %s' %sw
                        flag_stop+=1
                        continue
                if flag_stop==0:
                    counts[word] = counts.get(word, 0) + 1
                    #buff.append(word)
                    accepted_count+=1
                #print '    %s acceped' %(accepted_count)

    r.close()

    print 'finished cutting, %d words.' %len(counts)


    sorted_counts = list(counts.items())
    sorted_counts.sort(lambda a,b: -cmp((a[1], a[0]), (b[1], b[0])))

    output='times\tword\n'
    for item in sorted_counts:
        if item[1] < output_words_min_count:
            break
        output+= '%d\t%s\n' %(item[1],item[0].encode('utf-8'))

    output_file_path=script_dir+'output_'+file[:-4]+'.txt'
    if os.path.exists(output_file_path):
        timestamp=datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        os.rename(output_file_path,'%soutput_%s_bak%s.txt'%(script_dir,file[:-4],timestamp))

    w=open(output_file_path,'w+')
    w.write(output)
    w.flush()
    w.close()

print 'written to '+output_file_path

time_end=time.time()
print '\nfrom %f to %f,   %f seconds taken' %(time_start,time_end,time_end-time_start)
