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


$cfg['mssql']['host']='127.0.0.1';
$cfg['mssql']['port']='';
$cfg['mssql']['user']='username';
$cfg['mssql']['passwd']='password';
$cfg['mssql']['db']='my_database';
$cfg['mssql']['table']='my_talbe_name';
$cfg['mssql']['columns']='*';          # 字段列表，目前默认 * , 暂不支持指定
$cfg['mssql']['pk']='id';
$cfg['mssql']['pk_values']='';
$cfg['mssql']['driver']='';     # MSSQL 的 pdo 驱动，可选值为 sybase,mssql,dblib,sqlsrv,odbc，留空自动逐个尝试


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


