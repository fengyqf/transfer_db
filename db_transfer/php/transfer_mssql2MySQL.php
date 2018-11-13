<?php
#  使用说明
#   1.  本程序使用php把mssql数据导入到MySQL数据表中
#   2.  程序执行需要php环境，并需要启动mssql_*及mysqli 两个扩展
#   3.  除非数据量非常小，强烈推荐在cli中运行；而不是apache, nginx等web环境下中运行，太容易超时
#   4.  php7.0 起抛弃了mssql_*支持， 所以只能使用老版本
#   5.  因为mssql_*扩展不支持ntext类型字段，如果待导数据中有ntext字段，
#           请在配置文件显式指定各字段名，并把ntext类型convert()转换，例如：
#           $mssql['columns']='id,convert(varchar(max),content) as content';
#           这很蛋疼，但目前没有更好的解决方案
#   6.  请事先创建好目标表，本程序不会自动创建


if(php_sapi_name()!='cli'){
    echo "<h3>程序可能要运行相当长的时间，强烈推荐在cli中运行，避免超时中断</h3>\n";
}

# check and load config file 检查并载入文件
$script_root=dirname(__FILE__);
$config_file=$script_root.'/transfer_config.php';
if(file_exists($config_file)){
    require($config_file);
    echo "config file $config_file loaded.\n";
}else{
    exit('[Error] '.$config_file." not found. \n Create it from transfer_config_sample.php please.\n");
}
# check php environment: mssql_* mysqli_*
if(!function_exists('mssql_connect')){
    exit("[Error] mssql_connect() not exists, mssql extension required.\n");
}
if(!function_exists('mysqli_connect')){
    exit("[Error] mysqli_connect() not exists, mysqli extension required.\n");
}
ini_set('mssql.textsize','2147483647');
ini_set('mssql.textlimit','2147483647');

# fix config values 检查修正配置项数值
$cfg['sleep']=(int)$cfg['sleep'];
$cfg['start']=(int)$cfg['start'];
$cfg['end']=(int)$cfg['end'];
$cfg['batch_size']=(int)$cfg['batch_size'];
$batch_size=$cfg['batch_size'];

# connect mssql 连接mssql
if($mssql['port']!=''){
    $mssql['host']=$mssql['host'] . (PHP_OS=='WINNT' ? ',' : ':') .$mssql['port'];
}
$conn=mssql_connect($mssql['host'],$mssql['user'],$mssql['passwd']) or exit(mssql_get_last_message());
mssql_select_db($mssql['db'],$conn) or exit(mssql_get_last_message());


# link MySQL 连接MySQL
if($mysql['port']==''){
    $mysql['port']='3306';
}
$link = new mysqli($mysql['host'],$mysql['user'],$mysql['passwd'],$mysql['db'],$mysql['port']);
if($link->connect_error){
    exit('MySQL link failed: '.$link->connect_error);
}
$link->set_charset($mysql['charset']);


$sql="select min({$mssql['pk']}) as min_pk,max({$mssql['pk']}) as max_pk,count(*) as cnt from {$mssql['table']}";
$res=mssql_query($sql,$conn) or exit(mssql_get_last_message()."\n$sql");
$row = mssql_fetch_assoc($res) or exit(mssql_get_last_message());

$source_min=(int)$row['min_pk'];
$source_max=(int)$row['max_pk'];
$source_count=(int)$row['cnt'];


$pk_from = $cfg['start'] ? $cfg['start'] : $source_min;
$pk_to = $cfg['end'] ? $cfg['end'] : $source_max;
$batch_number_predict=(int)(($pk_to - $pk_from)/$cfg['batch_size']);

echo "\n\nsource pk range [{$source_min}, {$source_max}]\n";
echo "to transfer range [{$pk_from}, {$pk_to}]\n\n";
echo "batch_number_predict: $batch_number_predict \n\n";


# 读取mssql数据表结构，检查其中是否有ntext类型
$sql="select TABLE_CATALOG,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,
    IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH
    from INFORMATION_SCHEMA.COLUMNS
    where TABLE_NAME in  ( select name from sysobjects where type = 'U')
    and table_name='{$mssql['table']}'";
$res=mssql_query($sql);
$columns_names=$columns_cvt=array();
while ($row = mssql_fetch_assoc($res)) {
    $columns_names[]=$row['COLUMN_NAME'];
    if($row['DATA_TYPE']=='ntext'){
        $columns_cvt[]="convert(varchar(max),[{$row['COLUMN_NAME']}]) as [{$row['COLUMN_NAME']}]";
    }else{
        $columns_cvt[]='['.$row['COLUMN_NAME'].']';
    }
}
mssql_free_result($res);
$columns_source=implode(', ',$columns_cvt);
$columns_target='`'.implode('`, `',$columns_names).'`';

$pos=$pk_from;
$batch_num=0;
while($pos <= $pk_to){
    $batch_num++;
    $batch_end=$pos+$batch_size;
    echo "\n ".$batch_num.'/'.$batch_number_predict." [$pos, $batch_end)... ";
    $sql="select $columns_source from {$mssql['table']} where {$mssql['pk']} >= $pos and {$mssql['pk']} < $batch_end";
    #echo "\n\n$sql\n\n";
    $res=mssql_query($sql,$conn) or exit(mssql_get_last_message()."\n$sql");
    $inserted_count=0;
    while ($row = mssql_fetch_assoc($res)) {
        $values = array();
        foreach ($columns_names as $col) {
            if($row[$col]===NULL){
                $values[]='NULL';
            }else{
                $values[] = "'".$link->escape_string(iconv('gbk','utf-8//IGNORE',$row[$col]))."'";
            }
        }
        $sql_values=implode(',',$values);
        $sql="insert into {$mysql['table']} ($columns_target) values($sql_values)";
        #echo "\n\n$sql\n\n";
        $link->query($sql) or exit($link->error."\n".$sql);
        $inserted_count+=1;
    }
    $pos=$batch_end;
    echo "  $inserted_count  records.";
    sleep($cfg['sleep']);
}


echo "\nFinished";


