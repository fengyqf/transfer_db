<?php
isset($script_root) or exit('config file not accessable');

$cfg['source']='mssql';         # 目前只支持mssql/sqlite, 后面配置帐号密码等
$cfg['target']='mysql';         # 目前只支持MySQL
$cfg['debug']='0';              # 没啥用
$cfg['batch_size']='100';       # 每批次从source读取条数
$cfg['sleep']='1';              # 每批准处理完成后暂停时间（秒）
$cfg['start']='1';              # source表待导出数据起止位置，主键数字起始（含），留空表不限
$cfg['end']='';                 # ...终止位置（含），留空表不限


$cfg['mssql']['host']='127.0.0.1';
$cfg['mssql']['port']='';
$cfg['mssql']['user']='username';
$cfg['mssql']['passwd']='password';
$cfg['mssql']['db']='my_database';
$cfg['mssql']['table']='my_talbe_name';
$cfg['mssql']['columns']='*';          # 字段列表，目前默认 * , 暂不支持指定
$cfg['mssql']['pk']='id';
$cfg['mssql']['pk_values']='';


$cfg['sqlite']['filepath']='/path/to/file.sqlite.db';
$cfg['sqlite']['encryption_key']='';
$cfg['sqlite']['table']='my_talbe_name';
$cfg['sqlite']['columns']='*';          # 字段列表，目前默认 * , 暂不支持指定
$cfg['sqlite']['pk']='id';
$cfg['sqlite']['pk_values']='';



$cfg['mysql']['host']='127.0.0.1';
$cfg['mysql']['port']='3306';
$cfg['mysql']['user']='username';
$cfg['mysql']['passwd']='password';
$cfg['mysql']['db']='dbname';
$cfg['mysql']['charset']='utf8';
$cfg['mysql']['table']='tablename';


