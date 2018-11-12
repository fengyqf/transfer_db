#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import datetime
import time
import ConfigParser
import MySQLdb
import difflib
import re

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


'''parse address-string to a list, with each item a tuple contains name and address and reliability
looks like:  [(name1,address1,r1), (name2,address2,r2), ...]
'''
def parse_address(hay,def_name_pool=''):
    rtn=[]
    if not hay:
        return rtn
    #第一组address可能无姓名，暂存于 addr0 后续处理
    addr0=''
    i=0
    for part in [it.strip() for it in hay.split('[')]:
        pieces=part.split(']')
        if len(pieces) >=2:
            names=pieces[0].split(';')
            for name in names:
                rtn.append((name.strip(),pieces[1].strip(),10))
        elif i==0:
            #只暂存到addr0并处理第一组；第二组及以后组，如果没有 ] 则当作不完整部分，直接忽略
            addr0=part
        i+=1
    #特殊情况：有 [ 符号，但开头非 [   #TODO
    ed=[it[0] for it in rtn]
    if addr0:
        if ed:
            #逐个检查def_name_pool中姓名是否已经存在于已输出到rtn里的姓名(ed列表)之中，可适当调整阈值
            for name in def_name_pool.split(';'):
                matchers=[difflib.SequenceMatcher(lambda x: x in " -_",name,it) for it in ed]
                ratios=[mch.ratio() for mch in matchers]
                if max(ratios) < 0.5 :
                    rtn.append((name,addr0,int(10-max(ratios)*10)))
        else:
            names=[it.strip() for it in def_name_pool.split(';')]
            addrs=[it.strip() for it in hay.split(';')]
            if len(names)==len(addrs):
                reliability=3
            else:
                reliability=-1
            for i in range(0,len(names)):
                if i < len(addrs):
                    rtn.append((names[i],addrs[i],reliability))
                else:
                    rtn.append((names[i],addrs[-1],reliability))
    else:
        pass
    return rtn
'''
hay='[Abbasi, Bilal Haider; Liu, Rui; Liu, Chun-Zhao] Chinese Acad Sci, Inst Proc Engn, Natl Key Lab Biochem Engn, Beijing 100190, Peoples R China.   [Saxena, Praveen K.] Univ Guelph, Dept Plant Agr, Guelph, ON N1G 2W1, Canada.   [Abbasi, Bilal Haider] Quai'
hay='Tokyo Med & Dent Univ, Grad Sch Med & Dent Sci, Dept Oral, Bunkyo Ku, Tokyo 1138549, Japan; Tokyo Med & Dent Univ, Grad Sch Med & Dent Sci, Dept Maxillofacial Surg, Bunkyo Ku, Tokyo 1138549, Japan; Tokyo Med & Dent Univ, Grad Sch Med & Dent Sci, Div Oral '
hay='[Garmo, Oyvind A.] Norwegian Inst Water Res NIVA, N-2312 Ottestad, Norway; [Skjelkvale, Brit Lisa; de Wit, Heleen A.; Hogasen, Tore] Norwegian Inst Water Res NIVA, Oslo, Norway; [Colombo, Luca] Univ Appl Sci Southern Switzerland, Canobbio, Switzerland; [C'
pools='Garmo, OA; Skjelkvale, BL; de Wit, HA; Colombo, L; Curtis, C; Folster, J; Hoffmann, A; Hruska, J; Hogasen, T; Jeffries, DS; Keller, WB; Kram, P; Majer, V; Monteith, DT; Paterson, AM; Rogora, M; Rzychon, D; Steingruber, S; Stoddard, JL; Vuorenmaa, J; '
hay='Mem Sloan Kettering Canc Ctr, New York, NY 10021 USA; Univ Duisburg Essen, Univ Hosp, Essen, Germany'
pools='Murali, R; Griewank, KG; Schilling, B; Schimming, T; Moller, I; Moll, I; Schwamborn, M; Sucker, A; Zimmer, L; Schadendorf, D; Hillen, U'
foo=parse_address(hay,pools)
print '\n'
for it in foo:
    print '%25s ~ %s ~ %s' %(it[0],it[2],it[1])
exit()
'''

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


'''提取汉语拼音音序字母
  硬按规则提取，如果作用于英文名上，可能提取出长度为0,1等过短的字符串，使用需注意
'''
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

'''difflib for match full-name and email, both parameters are list
  return a dict contain tuple: each fullnames's email, ratio .
'''
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
        user=emails[i][:pos]
        matchers=[difflib.SequenceMatcher(lambda x: x in " -_",user,fn) for fn in fullnames]
        rates[user]=[mch.ratio() for mch in matchers]
        rates_dbg[user]=["%.3f"%mch.ratio() for mch in matchers]
        rt=max(rates[user])
        idx=rates[user].index(rt)
        mapping[fullnames[idx]]=(emails[i],rt)
        #ratio to small, use yinxu-name again; test for 0.2
        if rt < 0.2:
            yx=[]
            for j in range(0,len(fullnames)):
                tmp=retrive_yinxu(fullnames[j])
                if len(tmp)<2:
                    tmp=''
                yx.append(tmp)
            matchers=[difflib.SequenceMatcher(lambda x: x in " -_",user,fn) for fn in yx]
            rates[user]=[mch.ratio() for mch in matchers]
            rates_dbg[user]=["%.3f"%mch.ratio() for mch in matchers]
            rt_0=max(rates[user])
            if rt_0 > rt :
                # 清理上一步计算结果，并填充新结果
                mapping[fullnames[idx]]=('',0)
                idx=rates[user].index(rt_0)
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




def match_address(addresses,fullnames,shortnames=[]):
    if not fullnames:
        return {}
    ratios={}
    mapping={}
    for it in fullnames:
        mapping[it]=('',0,0)
    rates_dbg={}
    for name in fullnames:
        if not addresses:
            mapping[name]=('',0,0)
            continue
        matchers=[difflib.SequenceMatcher(lambda x: x in " -_",name,addr[0]) for addr in addresses]
        ratios[name]=[mch.ratio() for mch in matchers]
        rates_dbg[name]=["%.3f"%mch.ratio() for mch in matchers]
        rt=max(ratios[name])
        idx=ratios[name].index(rt)
        mapping[name]=(addresses[idx][1],rt,addresses[idx][2])
    return mapping
"""
tmp='[Abbasi, BH; Liu, Rui; Liu, Chun-Zhao] Chinese Acad Sci,... Beijing 100190, Peoples R China.   [Saxena, Praveen K.] Univ Guelph, Dept Plant Agr, Guelph, ON N1G 2W1, Canada.   [Abbasi, Bilal Haider] Quai'
addresses=parse_address(tmp)
tmp=''
tmp='Abbasi  Bilal Haider; Liu  Rui; Saxena  Praveen K.; Liu  Chun-Zhao'
fullnames=[it.strip() for it in tmp.split(';')]
mp=match_address(addresses,fullnames)
print mp
print "\n------------"
for it in mp:
    print "%25s ~ %s" %(it,mp[it])
exit()
"""


def extract_country(hay):
    country_kw=('Afghanistan','Africa','Anguilla','Antilles','Arab','Arabia','Argentina','Armenia','Aruba','Australia','Austria','Azerbaijan','Bahamas','Bahrain','Bangladesh','Barbados','Belarus','Belgium','Belize','Benin','Bermuda','Bhutan','Bolivia','Bouvet','Brazil','Brunei','Bulgaria','Burkina','Burundi','Caledonia','Cambodia','Cameroon','Canada','Cape','Cayman','Central','Chad','Chile','China','Colombia','Comoros','Congo','Costa','Cote','Croatia','Cyprus','Czech','Denmark','Djibouti','Dominica','Egypt','Equador','Equatorial','Eritrea','Estonia','Ethiopia','Falkland','Faroe','Fiji','Finland','France','French','Gabon','Gambia','Georgia','Germany','Ghana','Gibraltar','Greece','Greenland','Grenada','Guadeloupe','Guam','Guatemala','Guinea','Guyana','Haiti','Helena','Honduras','Hong','Hungary','Iceland','India','Indonesia','Ireland','Israel','Italy','Jamaica','Japan','Jordan','Kazakhstan','Kenya','Kingdom','Kiribati','Kitts','Korea','Kuwait','Kyrgyzstan','Lanka','Laos','Latvia','Lebanon','Lesotho','Liberia','Liechtenstein','Lithuania','Luxembourg','Macau','Macedonia','Madagascar','Malawi','Malaysia','Maldives','Mali','Malta','Mariana','Marshall','Martinique','Mauritania','Mayotte','Metropolitan','Mexico','Micronesia','Moldova','Mongolia','Morocco','Mozambique','Namibia','Nauru','Nepal','Neterland','Netherlands','Nevis','Nicaragua','Niger','Nigeria','Norway','Oman','Pakistan','Palau','Panama','Paraguay','Peru','Philippines','Pitcairn','Poland','Portugal','Principe','Qatar','Reunion','Rico','Romania','Russia','Sahara','Salvador','Saudi','Senegal','Seychelles','Singapore','Slovakia','Slovenia','Solomon','Somalia','Spain','Sudan','Suriname','Svalbard','Swaziland','Sweden','Switzerland','Syria','Taiwan','Tajikistan','Tanzania','Thailand','Togo','Tonga','Trinidad','Turkey','Turkmenistan','Turks','Tuvalu','Uganda','Ukraine','United','Uruguay','USA','Uzbekistan','Vanuatu','Vatican','Venezuela','Vietnam','Vincent','Western','Yemen','Yugoslavia','Zaire','Zambia','Zealand','Zimbabwe','Verde','Iran','England','Scotland')
    # removed these: 'Lucia',
    for it in country_kw:
        if hay.find(it) >=0:
            pos_s=hay.rfind(', ')
            if pos_s >= 0 :
                tmp=hay[pos_s+1:].strip()
                for ch in tmp:
                    if '0123456789'.find(ch) >=0:
                        return it
                return hay[pos_s+1:].strip()
            return hay.strip()
    return ''


'''如果有 dep dept 等字样，则将该段作为p1的结束位置
'''
def find_department_pos(hay,start=0):
    hay=hay.lower()
    sgns=['dep','dept','lab','technol','chem','hosp','biol','inst','grp','sci','bioinformat','metabolomforsch']
    flag=''
    pos_e=0
    for sgn in sgns:
        pos=hay.rfind(sgn,start)
        if len(hay) == pos+len(sgn):
            return len(hay)
        if pos > 0 and len(hay) > pos+len(sgn) and not hay[pos+len(sgn)] in 'abcdefghijklmnopqrstuvwxyz' and pos > pos_e:
            pos_e=pos
            flag=sgn
    return hay.find(', ',pos_e+len(flag))
'''
hay='Islamic Azad Univ, Touyserkan Branch, Dept Chem, Fac Scii'
pos= find_department_pos(hay)
print hay[:pos]
exit()
'''

'''pass address string to 4 parts: organization, depart, street, country
    return a list
'''
def parse_subaddr(hay):
    if not hay:
        return ['','','','']
    pos_s=0
    pos_e=hay.find(',')
    if pos_e < 0:
        return [hay.strip(),'','','']
    p0=hay[:pos_e].strip()
    pos_s=pos_e+1
    if hay[pos_s:].count(', ') == 0:
        return [p0,hay[pos_s:].strip(),'',extract_country(hay[pos_s:])]
    elif hay[pos_s:].count(', ') == 1:
        px=hay[pos_s:].split(', ')
        return [p0,px[0].strip(),px[1].strip(),extract_country(px[1])]
    elif hay[pos_s:].count(', ') >= 2:
        pos_e=find_department_pos(hay,pos_s)
        if pos_e <= 0:
            pos_e=hay[pos_s:].find(', ')+pos_s
        p1=hay[pos_s:pos_e].strip()
        pos_s=pos_e+1
        p3=extract_country(hay)
        #只有匹配到国家p3，才认为是完整地址字符串
        #  但如果国家段中有数字，则将其同步放到street段中
        if p3:
            num_in_p3=0
            for ch in p3:
                if '1234567890'.find(ch) >=0:
                    num_in_p3+=1
                    break
            pos_e=hay[pos_s:].rfind(', ')+pos_s
            if pos_e and not num_in_p3:
                p2=hay[pos_s:pos_e].strip()
            else:
                p2=hay[pos_s:].strip()
            return [p0,p1,p2,p3]
        else:
            p2=hay[pos_s:].strip()
            return [p0,p1,p2,p3]
    else:
        return [p0,hay[pos_s:].strip(),'','']
'''
hay='pku, Natl Key Lab, Beijing 100190, Peoples R China.'
hay='Chinese Acad Sci, Tech Inst Phys & Chem, Beijing 100080, Peop'
hay=' Univ Queensland, Sch Engn, ARC Ctr Excellence Funct Nanomat, St Lucia, Qld 4069,'
hay=' Seoul Natl Univ, Dept Mech & Aerosp Engn, Seoul, South Korea.'
hay=' Shanghai Meteorol Bur, Atmospher Chem Lab, Shanghai 200030,'
hay=' Florida State Univ, Dept Math, Tallahassee, FL 32306 USA.'
hay=' Univ Sheffield, Dept Automat Control & Syst Engn, Sheffield S1 3JD, S Yorkshire, England.'
hay=' Natl Taiwan Univ Sci & Technol, Dept Comp Sci & Informat Engn, Taipei, Taiwan.'
hay=' Univ Adelaide, Sch Earth & Environm Sci, Australian Ctr Ancient DNA, Adelaide, SA 5005, Au'
hay='Orthopaed Res Lab, Res Ctr, Hop Sacre Coeur, Montreal, PQ H4J 1C5, Canada.'
hay='Nanjing Univ, Natl Lab, Solid State Microstruct 12345, Nanjing 210093, Peoples R China.'
hay='Natl Ctr Atmospher Res, High Altitude Observ, Boulder, CO 80307 USA.'
hay='Mahidol Univ, Fac Med, Siriraj Hosp, Div Nephrol,Dept Med, Bangkok 10700, Thailand;'
rtn=parse_subaddr(hay)
print 'hay: ',hay,'\n'
print 'p0: ',rtn[0]
print 'p1: ',rtn[1]
print 'p2: ',rtn[2]
print 'p3: ',rtn[3]
exit()
'''




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
    1.  对多 response 分别匹配 [事实上好像并没有这样]
    2.  使用difflib匹配 Author_full 与 address 字段中的名称，命名与简名，及计算出的简名 [done, only use fullname]
    3.  在address中无匹配的author，使用response给出一个地址？ [在结果表中查询相应pp_id的response地址，不在拆分程序中实现]
    4.  按多个简化方式分别计算相似度，从中挑选最佳方式，以之计算匹配结果[好像暂时没有必要，待观察]
    5.  上条中多方式的“最佳”评判标准：显著超过其他，或者简单的最高 [同上条]
    6.  尝试计算出author 的单位名，从address中 [done, `paper_author`.`addr_xxx` 字段]
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
        row_addresses=parse_address(row['address'],row['Authors'])
        buff_emails=match_email(row_emails,author_full)
        buff_addresses=match_address(row_addresses,author_full)
        responser_name=''
        if author_full:
            matchers=[difflib.SequenceMatcher(lambda x: x in " -_",s_shortname_from_response,name) for name in author_full]
            ratios=[mch.ratio() for mch in matchers]
            ratio_max=max(ratios)
            responser_name=author_full[ratios.index(ratio_max)]

        rcd={}
        to_clean_un999_email=0
        for name in author_full:
            rcd[name]={'full_name':name}
            rcd[name]['pp_id']=row['id']
            #name_short=get_shortname(name)
            name_super_short=get_shortname(name,True)

            if name in [it for it in buff_emails if buff_emails[it][1] > 0]:
                rcd[name]['email']=buff_emails[name][0]
                rcd[name]['email_match_ratio']=buff_emails[name][1]
                parse_report['lines_matched_email']+=1
            else:
                rcd[name]['email']=''
                rcd[name]['email_match_ratio']=0

            if name in [it for it in buff_addresses if buff_addresses[it][1] > 0]:
                rcd[name]['address']=buff_addresses[name][0]
                rcd[name]['address_match_ratio']=buff_addresses[name][1]
                rcd[name]['author2addr_reliability']=buff_addresses[name][2]
                parse_report['lines_matched_address']+=1
                subaddr=parse_subaddr(buff_addresses[name][0])
                rcd[name]['addr_organization']=subaddr[0]
                rcd[name]['addr_depart']=subaddr[1]
                rcd[name]['addr_street']=subaddr[2]
                rcd[name]['addr_country']=subaddr[3]
            else:
                rcd[name]['address']=''
                rcd[name]['address_match_ratio']=0
                rcd[name]['author2addr_reliability']=0
                rcd[name]['addr_organization']=''
                rcd[name]['addr_depart']=''
                rcd[name]['addr_street']=''
                rcd[name]['addr_country']=''

            if responser_name==name:
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
                ,rcd[it]['email_match_ratio'],rcd[it]['address_match_ratio'],rcd[it]['full_name']
                ,rcd[it]['author2addr_reliability'],rcd[it]['addr_organization'],rcd[it]['addr_depart'],rcd[it]['addr_street'],rcd[it]['addr_country']))
        cursor.executemany("insert into `paper_author` \
            (`pp_id`, `address`, `response`, `email`, `email_match_ratio`, `address_match_ratio`, `full_name`\
            ,`author2addr_reliability`,`addr_organization`,`addr_depart`,`addr_street`,`addr_country`)\
             values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",values)

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


