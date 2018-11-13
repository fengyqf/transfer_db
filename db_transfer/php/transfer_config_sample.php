<?php
isset($script_root) or exit('config file not accessable');

$cfg['source']='mssql';         # 目前只支持mssql
$cfg['target']='mysql';         # 目前只支持MySQL
$cfg['debug']='0';              # 没啥用
$cfg['batch_size']='100';       # 每批次从source读取条数
$cfg['sleep']='1';              # 每批准处理完成后暂停时间（秒）
$cfg['start']='1';              # source表待导出数据起止位置，主键数字起始（含），留空表不限
$cfg['end']='10000';            # ...终止位置（含），留空表不限

$mssql['host']='127.0.0.1';
$mssql['port']='';
$mssql['user']='username';
$mssql['passwd']='password';
$mssql['db']='my_database';
$mssql['table']='my_talbe_name';
$mssql['columns']='*';          # 字段列表，目前默认 * , 暂不支持指定
$mssql['pk']='id';
$mssql['pk_values']='';




$mysql['host']='127.0.0.1';
$mysql['port']='3306';
$mysql['user']='username';
$mysql['passwd']='password';
$mysql['db']='dbname';
$mysql['charset']='utf8';
$mysql['table']='tablename';


