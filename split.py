#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import datetime
import time
import ConfigParser
import MySQLdb
import difflib

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
    try:
        target_start=int(cp.get('main','start'))
        target_end=int(cp.get('main','end'))
    except:
        target_start=0
        target_end=0
except :
    print "config.ini ERROR.  You can copy it from config.ini.sample "
    exit()




def get_shortname(name,super_short=0):
    try:
        name=name.strip()
    except:     #NoneType
        name=''
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

# parse address-string to a list, with each item a tuple contains name and address
#  looks like:  [(name1,address1), (name2,address2), ...]
def parse_address(hay):
    rtn=[]
    if not hay:
        return rtn
    for part in [it.strip() for it in hay.split('[')]:
        #print part
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

def parse_email(hay):
    rtn=[]
    if not hay:
        return rtn
    buff=''
    for i in range(0,len(hay)):
        if hay[i].isspace():
            if buff!='':
                rtn.append(buff)
                buff=''
        else:
            buff+=hay[i]
    if buff:
        rtn.append(buff)
    return rtn
"""
hay='  zhuzy@fudan.edu.cn   wingfung@hku.hk   x-he@uiuc.edu  '
hay='abruzzese@uniroma2.it   miccoli@ing.uniroma2.it   yjl@yzu.edu.cn'
print parse_email(hay)
exit()
"""

#提取汉语拼音音序字母
#   硬按规则提取，如果作用于英文名上，可能提取出长度为0,1等过短的字符串，使用需注意
def retrive_yinxu(hay):
    buff=''
    hay=hay.lower()
    for i in range(0,len(hay)):
        if i==0:
            if hay[i] in 'aeiou':
                buff+=hay[0]
        elif hay[i] in "aeiouv":
            if hay[i-1] in 'aeiou':
                continue
            if hay[i-1]==' ':
                buff+=hay[i]
                continue
            if i >=2 and hay[i-1]=='h' and hay[i-2] in 'zcs':
                buff+=hay[i-2:i]
            else:
                buff+=hay[i-1]
    return buff
""" test for retrive_yinxu
hay='Yao Ai-Hua; Wang, Yong; Fan. HR; Hu FangFang; Lan TingGuang;Xu  Xiaoqing; Liu  Xianglin; Han  Xiuxun; Yuan  Hairong; Wang  Jun; Guo  Yan; Song  Huaping; Zheng  Gaolin; Wei  Hongyuan; Yang  Shaoyan; Zhu  Qinsheng; Wang  Zhanguo;Jiang  Wei Xiang; Cui  Tie Jun; Yang  Xin Mi; Cheng  Qiang; Liu  Ruopeng; Smith  David R.'
fullnames=[it.strip() for it in hay.split(';') if it.strip()!='']
for it in fullnames:
    print "%20s ~ %s" %(it,retrive_yinxu(it))
exit()
"""

#difflib for match full-name and email, both parameters are list
#  return a dict contain tuple: each fullnames's email, ratio .
def match_email(emails,fullnames,shortnames=[]):
    rates={}
    mapping={}
    for it in fullnames:
        mapping[it]=('',0)
    rates_dbg={}
    for i in range(0,len(emails)):
        pos=emails[i].find('@')
        if pos <= 0:
            continue
        #user 似乎有必要事先移除数字等特殊符号
        user=emails[i][:pos]
        #matchers=[difflib.SequenceMatcher(None,user,fn) for fn in fullnames]
        matchers=[difflib.SequenceMatcher(lambda x: x in " -_",user,fn) for fn in fullnames]
        rates[user]=[mch.ratio() for mch in matchers]
        rates_dbg[user]=["%.3f"%mch.ratio() for mch in matchers]
        rt=max(rates[user])
        idx=rates[user].index(rt)
        mapping[fullnames[idx]]=(emails[i],rt)
        #print mapping
        #ratio to small, use yinxu-name again; test for 0.2
        if rt < 0.2:
            yx=[]
            #print "fullnames:  ",fullnames
            for j in range(0,len(fullnames)):
                tmp=retrive_yinxu(fullnames[j])
                if len(tmp)<2:
                    tmp=''
                yx.append(tmp)
            matchers=[difflib.SequenceMatcher(lambda x: x in " -_",user,fn) for fn in yx]
            rates[user]=[mch.ratio() for mch in matchers]
            #print "rates[user]    ",rates[user]
            rates_dbg[user]=["%.3f"%mch.ratio() for mch in matchers]
            rt_0=max(rates[user])
            #print "rt_0    ",rt_0
            if rt_0 > rt :
                # 清理上一步计算结果，并填充新结果
                mapping[fullnames[idx]]=('',0)
                idx=rates[user].index(rt_0)
                #print "j ~~ ",j
                mapping[fullnames[idx]]=(emails[i],rt_0)
    return mapping
    """
    print ''
    for it in rates_dbg:
        print "%10s ~ %s" %(it,rates_dbg[it])
    """

"""
emails_string="eeeeeeeeeeee"
names_string="nnnnnnnnnnnnnnnnnnnnnnn"
emails_string='guo_mj@ecust.com.cn   siliangz@ecust.com.cn'
names_string='Xiong  Zhi-Qiang; Guo  Mei-Jin; Guo  Yuan-Xin; Chu  Ju; Zhuang  Ying-Ping; Zhang  Si-Liang'
emails_string="abruzzese@uniroma2.it   miccoli@ing.uniroma2.it   yjl@yzu.edu.cn"  #failed pair - still failed
names_string="Abruzzese  Donato; Miccoli  Lorenzo; Yuan  Jianli"  #failed pair - still failed

emails=[it.strip() for it in emails_string.split(' ') if it.strip()!='']
fullnames=[it.strip() for it in names_string.split(';') if it.strip()!='']
mp=match_email(emails,fullnames)
print "\n------------"
print emails
print fullnames
print ""
for it in mp:
    print "%25s ~ %s" %(it,mp[it])
exit()
"""






link=MySQLdb.connect(target_host,target_user,target_passwd,target_db,charset=target_charset)
cursor=link.cursor()

cursor.execute("select min(id) as min_pk,max(id) as max_pk,count(*) as cnt from `%s`" %(target_table))
min_id,max_id,rs_cnt=cursor.fetchone()
# start,end limit in config file
if target_start > 0:
    min_id=target_start
if target_end > 0:
    max_id=target_end
batch_start=min_id // batch_count * batch_count


"""
TODO:
    1.  对多 response 分别匹配
    2.  使用difflib匹配 Author_full 与 address 字段中的名称，命名与简名，及计算出的简名
    3.  在address中无匹配的author，使用response给出一个地址？
    4.  按多个简化方式分别计算相似度，从中挑选最佳方式，以之计算匹配结果
    5.  上条中多方式的“最佳”评判标准：显著超过其他，或者简单的最高
    6.  尝试计算出author 的单位名，从address中
"""

cursor=link.cursor(cursorclass=MySQLdb.cursors.DictCursor)
while batch_start <= max_id+1:
    print "[%s,%s)" %(batch_start,batch_start+batch_count)
    cursor.execute("select `id`,`title`,`De`,`journalid`,`userid`,`Abstract`,`address`\
        ,`response`,`pyear`,`email`,`id1`,`Authors`,`Author_full` from %s \
        where  `id` >= %s and `id` < %s" %(
        target_table, batch_start, batch_start+batch_count))

    for row in cursor.fetchall():
        for it in row:
            if row[it]==None:
                row[it]=''
        if debug:
            print '\n\n',row['id'],' ',row['title'][:20],'...'
            print "\n\n\n%5s: Authors, Author_full, address\n%s\n%s\n%s\n%s" %(row['id'],row['Authors'],row['Author_full'],row['address'],row['email'])

        #记录行分析结果简讯: 主键id号，拆成行数，匹配到地址行数，原邮箱个数，匹配到邮箱行数，是否匹配到response
        parse_report={'pk_id':row['id'], 'lines':0, 'lines_matched_address':0,
            'email_count':0,'lines_matched_email':0,
            'response_matched':0}

        authors=[it.strip() for it in row['Authors'].split(';')]
        author_full=[it.strip() for it in row['Author_full'].split(';')]
        row_emails=parse_email(row['email'])
        s_shortname_from_response=get_shortname(row['response'],True)
        buff_addresses=parse_address(row['address'])
        buff_emails=match_email(row_emails,author_full)

        rcd={}
        to_clean_un999_email=0
        for name in author_full:
            rcd[name]={'full_name':name}
            rcd[name]['pp_id']=row['id']
            name_short=get_shortname(name)
            name_super_short=get_shortname(name,True)
            addr=''
            if addr=='':
                for it in buff_addresses:
                    if it[0] == name:
                        addr=it[1]
            if addr=='':
                for it in buff_addresses:
                    if it[0] == name_short:
                        addr=it[1]
            if addr=='':
                for it in buff_addresses:
                    if get_shortname(it[0]) == name:
                        addr=it[1]
            if addr=='':
                for it in buff_addresses:
                    if get_shortname(it[0]) == name_short:
                        addr=it[1]
            if addr=='':
                for it in buff_addresses:
                    if get_shortname(it[0]) == name_super_short:
                        addr=it[1]
            if addr=='':
                for it in buff_addresses:
                    if get_shortname(it[0],True) == name_short:
                        addr=it[1]
            if addr=='':
                for it in buff_addresses:
                    if get_shortname(it[0],True) == name_super_short:
                        addr=it[1]
            rcd[name]['address']=addr
            if addr!='':
                parse_report['lines_matched_address']+=1

            if name in [it for it in buff_emails if buff_emails[it][1] > 0]:
                rcd[name]['email']=buff_emails[name][0]
                rcd[name]['email_match_ratio']=buff_emails[name][1]
                parse_report['lines_matched_email']+=1
            else:
                rcd[name]['email']=''
                rcd[name]['email_match_ratio']=0

            if s_shortname_from_response==name_super_short:
                rcd[name]['response']=row['response']
                #单邮箱、并且与response匹配，则认定该邮箱为response的邮箱，ratio标记为9999
                #  TODO 或许，这里可能会有问题
                if len(row_emails)==1:
                    rcd[name]['email']=row_emails[0]
                    rcd[name]['email_match_ratio']=9999
                    #标记：需要清除按前面匹配得到的email地址归属
                    to_clean_un999_email=1
                parse_report['response_matched']+=1
            else:
                rcd[name]['response']=''

            if debug:
                print "\n---- ",name,'-----'
                for it in rcd[name]:
                    print '    ',it,' ~ ',rcd[name][it]

        if to_clean_un999_email==1:
            parse_report['lines_matched_email']=1
            for it in rcd:
                if rcd[it]['email_match_ratio']!=9999:
                    rcd[it]['email_match_ratio']=0
                    rcd[it]['email']=''

        values=[]
        for it in rcd:
            values.append((rcd[it]['pp_id'],rcd[it]['address'],rcd[it]['response'],rcd[it]['email']
                ,rcd[it]['email_match_ratio'],rcd[it]['full_name']))
        cursor.executemany("insert into `paper_author` \
            (`pp_id`, `address`, `response`, `email`, `email_match_ratio`, `full_name`)\
             values(%s,%s,%s,%s,%s,%s)",values)

        parse_report['lines']=len(author_full)
        parse_report['email_count']=len(row_emails)

        values=(parse_report['pk_id'],parse_report['lines'],parse_report['lines_matched_address'],parse_report['email_count'],parse_report['lines_matched_email'],parse_report['response_matched'])

        cursor.execute("insert into paper_split_report \
            (`pp_id`, `lines`, `lines_matched_address`, `email_count`, `lines_matched_email`, `response_matched`)\
             values(%s,%s,%s,%s,%s,%s)",values)


    batch_start+=batch_count
    if batch_sleep > 0:
        print "sleep %ss" %batch_sleep
        time.sleep(batch_sleep)

cursor.close()
link.close()


exit("All Done")


