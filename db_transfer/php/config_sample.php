<?php
isset($script_root) or exit('config file not accessable');

$cfg['source']='sqlite';        # 目前只支持 mssql/sqlite, 后面配置帐号密码等
$cfg['target']='mysql';         # 目前只支持MySQL
$cfg['debug']='0';              # 没啥用
$cfg['batch_size']='100';       # 每批次从source读取条数
$cfg['sleep']='1';              # 每批准处理完成后暂停时间（秒）
$cfg['start']='';               # source表待导出数据起止位置，主键数字起始（含），留空表不限
$cfg['end']='';                 # ...终止位置（含），留空表不限
$cfg['failed_threshold']='';    # [0-100) 出错阈值百分比，批插入出错超过此限程序终止；留空，表示不容忍错误
$cfg['failed_1st']='1';         # 数字，检查前 N 条插入出错达到此限程序即终止；留空忽略该检查
$cfg['php_memory_limit']='';    # php配置选项，可以带单位KM等，如 128M (php环境通常的默认值)，留空表示默认
$cfg['compact_varchar']=0;      # 是否自动压缩(n)varchar型字段长度。建表字段长度为刚好够用，以节省表空间
$cfg['compact_text']=0;         # 是否自动压缩(n)text型字段长度，压缩为varchar()。同上


$cfg['mssql']['host']='127.0.0.1';
$cfg['mssql']['port']='';
$cfg['mssql']['user']='username';
$cfg['mssql']['passwd']='password';
$cfg['mssql']['db']='my_database';
$cfg['mssql']['table']='my_talbe_name';
$cfg['mssql']['columns']='*';          # 字段列表，目前默认 * , 暂不支持指定
$cfg['mssql']['pk']='id';
$cfg['mssql']['pk_values']='';
# MSSQL 的 pdo 驱动，留空自动逐个尝试，参看 libs.php/connect_mssql()
$cfg['mssql']['driver']='';
# 从mssql读取到的数据，往mysql写入前做的编码转换，出现乱码时考虑使用
$cfg['mssql']['iconv_from']='';
$cfg['mssql']['iconv_to']='';


$cfg['sqlite']['filepath']='/path/to/file.sqlite.db';
$cfg['sqlite']['encryption_key']='';
$cfg['sqlite']['table']='my_talbe_name';
$cfg['sqlite']['columns']='*';          # 字段列表，目前默认 * , 暂不支持指定
$cfg['sqlite']['pk']='id';              # 整形数字的标识字段名，留空时程序自动计算
$cfg['sqlite']['pk_values']='';



$cfg['mysql']['host']='127.0.0.1';
$cfg['mysql']['port']='3306';
$cfg['mysql']['user']='username';
$cfg['mysql']['passwd']='password';
$cfg['mysql']['db']='dbname';
$cfg['mysql']['charset']='utf8';
$cfg['mysql']['table']='';              # 导入目录表名，不指定则与 source 一致


