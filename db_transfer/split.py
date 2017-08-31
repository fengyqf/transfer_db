#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import datetime
import time
import ConfigParser
import MySQLdb

script_dir=os.path.split(os.path.realpath(__file__))[0]+'/'

config_file=script_dir+'/config.ini'
cp=ConfigParser.ConfigParser()
cp.read(config_file)



try:

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




def is_shortname(name):
    pos=name.find(' ')
    if pos > 0:
        tmp=name[pos:].strip()
    else:
        tmp=name
    if tmp==tmp.upper():
        return True
    else:
        return False

""" test is_shortname()
s="Abbotto  Alessandro; Manfredi  Norberto; Marinzi  Chiara; De Angelis  Filippo; Mosconi  Edoardo; Yum  Jun-Ho; Zhang Xianxi; Nazeeruddin  Mohammad K.; Graetzel  Michael; Ji  DB; Zhu  HB; Ye  J; Li  CL; Lv  Lu-Ping; Xie  Jian-Wu; Yu  Wen-Bo; Li  Wei-Wei; Hu  Xian-Chao; Lv  LP; Xie  JW; Yu  WB; Li  WW; Hu  XC; "

for n in s.split('; '):
    print n,'     ',is_shortname(n)
"""
def get_shortname(name,super_short=0):
    name=name.strip()
    firstname_done=0
    last_char_is_blank=0
    second_upper_done=0
    buff=''
    for i in range(0,len(name)):
        if firstname_done:
            #首词已处理过。
            #  遇大写，判断第二个大写字母（首空格后第一个大写字母）是否未处理过
            #    若未处理过，或者未指定了super_short，则追加当前字符
            #    其他情况下不追加
            if name[i].isupper():
                if second_upper_done==0 or super_short==0:
                    buff+=name[i]
                    second_upper_done=1
        elif name[i].isspace():
            #遇空格的处理，标记首词已处理；另防止重复追加空格(词间单空格分隔)
            firstname_done=1
            #上个字母是逗号，则移除之
            if buff[-1:]==',':
                buff=buff[:-1]
            #上个字母非时，追加个空格
            if last_char_is_blank==0:
                buff+=' '
                last_char_is_blank=1
        else:
            #其他情况就是首词未处理，直接追加即可(除了指定的几个字符)
            buff+=name[i]
    return buff

""" A Bad Plan    
    pieces=[it.strip() for it in name.split(' ') if len(it)>0]
    buff=''
    if super_short:
        if pieces[0][-1:]==',':
            buff=pieces[0][:-1]+' '
        buff=
    else:
        for i in range(0,len(pieces)):
        if i==0:
            if pieces[i][-1:]==',':
                pieces[i]=pieces[i][:-1]
                buff=pieces[i][:-1]+' '
            else:
                buff=pieces[i]+' '
        else:
            if pieces[i][0].isupper():
                buff+=pieces[i][0]
"""
""" test get_shortname()
s='Abbasi  Bilal Haider; Liu  Rui; Saxena  Praveen K.; Liu  Chun-Zhao;Xie  Hai-Bing; Irwin  David M.; Zhang  Ya-Ping;Abbasi  R. U.; Abu-Zayyad  T.; Al-Seady  M.; Allen  M.; Amann  J. F.; Archbold  G.; Belov  K.; Belz  J. W.; '
s='Abbasi  Bilal Haider; Liu  Rui; Saxena  Praveen K.; Liu,  Chun-Zhao;Xie  Hai-Bing; Irwin.  David M.; Zhang  Ya-Ping;'
for it in s.split(';'):
    print it, '  ->  ', get_shortname(it),'                     ',get_shortname(it,True)
exit()
"""

# nothing to use, prepare clean
def find_address(name,buff):
    #name
    #name可能为全名或科名(空格，带或不带逗号)为，空格后只有大写字母
    #hay为tuple组的列表

    rtn=None
    #当name为简名
    for it in buff:
        if name==it[0]:
            rtn=it[1]
            break
    if not rtn:
        for it in buff:
            if name==it[0]:
                rtn=it[1]
                break
    return rtn

    """
    ptn_str=name.replace(' ',r'[^a-zA-Z0-9]*')
    pattern = re.compile(ptn_str)
    pos_0=row['address'].find(au)
    if pos_0 > 0:
        pos_s=row['address'].find(']',pos_0+len(au))
        pos_e=row['address'].find('[',pos_s)
        if pos_e > 0:
            rcd[au]['address']=row['address'][pos_s:pos_e]
        else:
            rcd[au]['address']=row['address'][pos_s:]
        print "Found: ",rcd[au]['address']
    else:
        print "Address Not Found for ",au
        rcd[au]['address']='NotFound'
    """
# parse address-string to a list, with each item a tuple contains name and address
#  looks like:  [(name1,address1), (name2,address2), ...]
def parse_address(hay):
    rtn=[]
    for part in [it.strip() for it in hay.split('[')]:
        print part
        pieces=part.split(']')
        if len(pieces) >=2:
            for piece in pieces:
                names=pieces[0].split(';')
                for name in names:
                    rtn.append((name.strip(),pieces[1]))
    return rtn
# Testing
#hay='[Abbasi, Bilal Haider; Liu, Rui; Liu, Chun-Zhao] Chinese Acad Sci, Inst Proc Engn, Natl Key Lab Biochem Engn, Beijing 100190, Peoples R China.   [Saxena, Praveen K.] Univ Guelph, Dept Plant Agr, Guelph, ON N1G 2W1, Canada.   [Abbasi, Bilal Haider] Quai'
#print parse_address(hay)








link=MySQLdb.connect(target_host,target_user,target_passwd,target_db,charset=target_charset)
cursor=link.cursor()

cursor.execute("select min(id) as min_pk,max(id) as max_pk,count(*) as cnt from `%s`" %(target_table))
min_id,max_id,rs_cnt=cursor.fetchone()

batch_start=min_id // batch_count * batch_count
#print "source lines %s, id range [%s,%s].\ntask range [%s,%s] batch size %s \n" %(
#    rs_cnt,min_id,max_id,batch_start,max_id,batch_count)




max_id=10000
x=[]
y=[]
cursor=link.cursor(cursorclass=MySQLdb.cursors.DictCursor)
while batch_start <= max_id+1:
    print "[%s,%s)" %(batch_start,batch_start+batch_count)
    cursor.execute("select `id`,`title`,`De`,`journalid`,`userid`,`Abstract`,`address`\
        ,`response`,`pyear`,`email`,`id1`,`Authors`,`Author_full` from %s \
        where  `id` >= %s and `id` < %s" %(
        target_table, batch_start, batch_start+batch_count))

    for row in cursor.fetchall():
        #print row['id'],' ',row['title'][:20],'...'
        #print "\n\n\n%5s\n%s\n%s\n%s" %(row['id'],row['Authors'],row['Author_full'],row['address'])

        authors=[it.strip() for it in row['Authors'].split(';')]
        author_full=[it.strip() for it in row['Author_full'].split(';')]
        #address=row['address'].split('; ')

        buff_addresses=parse_address(row['address'])

        rcd={}
        for name in author_full:
            rcd[name]={'full_name':name}
            addr=''
            """
            print "\n\n"
            print 'name: ',name
            print 'buff_addresses: ',[it[0] for it in buff_addresses]
            print 'get_shortname(name): ',get_shortname(name)
            print 'get_shortname(buff_addresses[0]): ',[get_shortname(it[0]) for it in buff_addresses]
            print 'get_shortname(name,True): ',get_shortname(name,True)
            print 'get_shortname(buff_addresses[0],True): ',[get_shortname(it[0],True) for it in buff_addresses]
            """
            if addr=='':
                for it in buff_addresses:
                    if it[0] == name:
                        addr=it[1]
            if addr=='':
                for it in buff_addresses:
                    if it[0] == get_shortname(name):
                        addr=it[1]
            if addr=='':
                for it in buff_addresses:
                    if get_shortname(it[0]) == name:
                        addr=it[1]
            if addr=='':
                for it in buff_addresses:
                    if get_shortname(it[0]) == get_shortname(name):
                        addr=it[1]
            if addr=='':
                for it in buff_addresses:
                    if get_shortname(it[0]) == get_shortname(name,True):
                        addr=it[1]
            if addr=='':
                for it in buff_addresses:
                    if get_shortname(it[0],True) == get_shortname(name):
                        addr=it[1]
            if addr=='':
                for it in buff_addresses:
                    if get_shortname(it[0],True) == get_shortname(name,True):
                        addr=it[1]
            rcd[name]['address']=addr
            """
            print "****addr: ",addr
            print "\n\n"
            """

            #find address
#            pos_0=row['address'].find(name)
#            if pos_0 > 0:
#                pos_s=row['address'].find(']',pos_0+len(name))
#                pos_e=row['address'].find('[',pos_s)
#                if pos_e > 0:
#                    rcd[name]['address']=row['address'][pos_s:pos_e]
#                else:
#                    rcd[name]['address']=row['address'][pos_s:]
#                print "Found: ",rcd[name]['address']
#            else:
#                print "Address Not Found for ",name
#                rcd[name]['address']='NotFound'
            #pos=


        print rcd

    break
    


    batch_start+=batch_count
    if batch_sleep > 0:
        print "sleep %ss" %batch_sleep
        time.sleep(batch_sleep)

cursor.close()

print '\n\n'
print x
print y


exit("AAAAAAAAAAAAAAAAAAAAAAAA")


