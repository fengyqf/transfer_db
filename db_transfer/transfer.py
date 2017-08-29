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

#TODO source_max_pk
source_max_pk=13873572

batch_start=0
while batch_start <= source_max_pk:
    print "select %s from %s where %s > %s and %s <= %s" %(
            source_columns,source_table,source_pk, batch_start,source_pk, batch_start+batch_count )
    source_cursor.execute("select %s from %s where %s > %s and %s < %s" %(
            source_columns,source_table,source_pk, batch_start,source_pk, batch_start+batch_count ))

    column_names=[]
    for item in source_cursor.description:
        column_names.append(item[0])

    #print column_names
    sql="insert into `%s` (`%s`) values(%s)" %(
            target_table, '`, `'.join(column_names),  ', '.join([r'%s']*len(column_names)))
    #print sql

    try:
        for row in source_cursor.fetchall():
            vs=[]
            #print row.UT,row.__getattribute__('UT')
            #for column in column_names:
            #    print column,': ',row.__getattribute__(column)
            vs=tuple([row.__getattribute__(column) for column in column_names])

            #print vs

            target_cursor.execute(sql, vs)
        else:
            pass
    except:
        pass

    batch_start+=batch_count
    print "done (%s,%s]" %(batch_start,batch_start+batch_count)
    #break

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
