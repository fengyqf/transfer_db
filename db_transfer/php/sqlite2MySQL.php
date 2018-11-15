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
#   7. 需要 iconv 库
#   8. pdo_sqlsvr 安装太麻烦了，先忽略不管，搞完sqlite再说

if(php_sapi_name()!='cli'){
    echo "<h3>程序可能要运行相当长的时间，强烈推荐在cli中运行，避免超时中断</h3>\n";
}

# check and load config file 检查并载入文件
$script_root=dirname(__FILE__);
$config_file=$script_root.'/config.php';
if(file_exists($config_file)){
    require($config_file);
    #echo "config file $config_file loaded.\n";
}else{
    exit('[Error] '.$config_file." not found. \n Create it from config_sample.php please.\n");
}

# fix config values 检查修正配置项数值
$cfg['sleep']=(int)$cfg['sleep'];
$cfg['start']=(int)$cfg['start'];
$cfg['end']=(int)$cfg['end'];
$cfg['batch_size']=(int)$cfg['batch_size'];
$cfg['failed_threshold']=(int)$cfg['failed_threshold'];

$batch_size=$cfg['batch_size'];
$failed_threshold=$cfg['failed_threshold'];
if(!$cfg['mysql']['table']){
    $cfg['mysql']['table']=$cfg[$cfg['source']]['table'];    # 反正不支持 source 写为 sqlite2 了
}
$target_table=$cfg['mysql']['table'];


# check source and target db-type
if(in_array($cfg['source'], array('sqlite','sqlite2','sqlite3'))) {
    # 非根绝对路径，则默认为相对脚本的相对路径
    if($cfg['sqlite']['filepath'][1]!=':' && $cfg['sqlite']['filepath'][0]!='/' ){
        $data_path=$script_root.'/'.$cfg['sqlite']['filepath'];
    }else{
        $data_path=$cfg['sqlite']['filepath'];
    }
    if(!file_exists($data_path)){
        exit("\n[Error] SQLite file Not Exists or Not accessable:\n$data_path\n");
    }
    if($cfg['source']=='sqlite2'){
        $dsn="sqlite2:$data_path";
    }else{
        $cfg['source']='sqlite';
        $dsn="sqlite:$data_path";
    }
    if(!extension_loaded('pdo_sqlite')){
        exit("\n[Error] PDO driver not exists: pdo_sqlite");
    }
    try{
        $conn=new PDO($dsn);
    }catch(PDOException $e) {
        exit("\n[Error] Connection failed:\n".$e->getMessage()."\n$dsn");
    }
    $source_table=$cfg['sqlite']['table'];
    $source_pk=$cfg['sqlite']['pk'];
}
# connect mssql 连接mssql, and fix environment
# TODO 暂未知PDO_MSSQL下的textsize怎么定义，遇上再说吧；
# TODO 假定在PDO_MSSQL模式下服务器端口的指定方式与mssql_* 一致（winnt与*nix不同），实测后再改
if($cfg['source']=='mssql'){
    ini_set('mssql.textsize','2147483647');
    ini_set('mssql.textlimit','2147483647');
    if($cfg['mssql']['port']!=''){
        $cfg['mssql']['host']=$cfg['mssql']['host'].(PHP_OS=='WINNT'?',':':').$cfg['mssql']['port'];
    }
    if(!extension_loaded('pdo_sqlsrv')){
        exit("\n[Error] PDO driver not exists: pdo_sqlsrv");
    }
    $dsn="sqlsrv:Server={$cfg['mssql']['host']};Database={$cfg['mssql']['db']}";
    try{
        $conn=new PDO($dsn,$cfg['mssql']['user'], $cfg['mssql']['passwd']);
    }catch(PDOException $e) {
        exit("\n[Error] Connection failed:\n".$e->getMessage()."\n$dsn");
    }
    $source_table=$cfg['mssql']['table'];
    $source_pk=$cfg['mssql']['pk'];
}




# 读取数据源字段列表 sqlite
if($cfg['source']=='sqlite'){
    $sql="PRAGMA table_info([{$cfg['sqlite']['table']}])";
    $res=$conn->query($sql);
    $table_info=array();
    while($row=$res->fetch(PDO::FETCH_ASSOC)){
        $table_info[$row['name']]=$row;
    }

    # 构造读取、插入语句的字段列表
    $columns_names=array_keys($table_info);
    $columns_source='[' . implode('], [',$columns_names) . ']';
    $columns_target='`'.implode('`, `',$columns_names).'`';
}


/*
TODO  这里是尝试根据数据源字段自动建表，待继续完成

$field_name=array_keys();
foreach ($table_info as $key => $value) {
    #检查类型做对应转换，有其他类型也这里做转换
    if($value['type']=='Text'){
        $table_info[$key]['to_type']='mediumtext';
    }
}
print_r($table_info);

#读出所有字段信息，抽取其中字符串型，检测最大长度，与MySQL类型映射表中计算得结果目标类型
$data_type_mapping=array('integer'=>'int','tinyint'=>'tinyint','Text'=>'varchar')

*/
$sql="select min({$source_pk}) as min_pk,max({$source_pk}) as max_pk,count(*) as cnt from {$source_table}";
try{
    $res=$conn->query($sql);
    $row=$res->fetch(PDO::FETCH_ASSOC);
    $source_min=(int)$row['min_pk'];
    $source_max=(int)$row['max_pk'];
    $source_count=(int)$row['cnt'];
}catch(PDOException $e){
    exit("[Error] query failed:\n".$e->getMessage()."\n");
}



$pk_from = $cfg['start'] ? $cfg['start'] : $source_min;
$pk_to = $cfg['end'] ? $cfg['end'] : $source_max;
$batch_number_predict=(int)(($pk_to - $pk_from)/$cfg['batch_size']);

echo "\nsource pk range [{$source_min}, {$source_max}]";
echo "\nto transfer range [{$pk_from}, {$pk_to}]";
echo "\nbatch_number_predict: $batch_number_predict \n\n";



# link MySQL 连接MySQL，并准备PDO 匿名参数化的插入 statement
if($cfg['mysql']['port']==''){
    $cfg['mysql']['port']='3306';
}
$dsn="mysql:host={$cfg['mysql']['host']};"
        . ($cfg['mysql']['port'] ? "port={$cfg['mysql']['port']};" : '')
        . "dbname={$cfg['mysql']['db']};"
        . (version_compare(PHP_VERSION, '5.3.6', '>=') ? "charset={$cfg['mysql']['charset']};" : '');
if(version_compare(PHP_VERSION, '5.3.6', '<')) {
    $options = array(PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8',);
}else{
    $options = NULL;
}
try{
    $link = new PDO($dsn, $cfg['mysql']['user'], $cfg['mysql']['passwd'], $options);
    $link->setAttribute(PDO::ATTR_ERRMODE,PDO::ERRMODE_EXCEPTION);
}catch(PDOException $e){
    exit("\n[Error] MySQL Connect failed:\n".$e->getMessage()."\n$dsn");
}
echo "\n\nconnected: $dsn    {$cfg['mysql']['user']}/{$cfg['mysql']['passwd']}  \n";
# 插入语句，匿名参数化查询
$params=array();
for($i=0; $i < count($columns_names); $i++){
    $params[]='?';
}
$sql="insert into {$target_table} ($columns_target) values(" . implode(', ', $params ) . ')';
unset($params);
try{
    $insertor=$link->prepare($sql);
}catch(PDOException $e){
    exit("\n[Error] MySQL insert prepare failed:\n".$e->getMessage()."\n$sql");
}

$pos=$pk_from;  # 当前处理到的 pk 位置
$batch_num=0;   # 当前批次号
$total_inserted=$total_failed=0;    # 总计数
while($pos <= $pk_to){
    $batch_num++;
    $batch_end=$pos+$batch_size;
    echo "\n ".$batch_num.'/'.$batch_number_predict."  [$pos, $batch_end)...  ";
    $sql="select $columns_source from {$source_table} 
            where {$source_pk} >= $pos and {$source_pk} < $batch_end";
    try{
        $res=$conn->query($sql);
    }catch(PDOException $e){
        exit("\n[Error] query failed:\n".$e->getMessage()."\n");
    }
    if(!$res){
        exit("\n[Error] query failed:\n$sql\n");
    }
    $inserted_count=$failed_count=0;
    while ($row=$res->fetch(PDO::FETCH_ASSOC)) {
        $values = array();
        foreach ($columns_names as $col) {
            # fuuuking mssql need encoding convert
            if($cfg['source']=='mssql'){
                $values[] = iconv('gbk','utf-8//IGNORE',$row[$col]);
            }else{
                $values[] = $row[$col];
            }
        }
        try{
            $insertor->execute($values);
            $inserted_count+=1;
        }catch(PDOException $e) {
            echo "\n[Warning] insert failed:\n".$e->getMessage()."\n\n".var_export($values);
            $failed_count+=1;
        }
    }
    $pos=$batch_end;
    echo "  Success: $inserted_count    Faile: $failed_count";
    # 检测当前批次出错率是否超出阈值
    $failed_ratio=0;
    if($failed_count || $inserted_count){
        $failed_ratio=$failed_count/($failed_count+$inserted_count)*100;
    }
    if($failed_ratio > $failed_threshold){
        exit("\n\n[Error] To many error occered\nYou can change \$cfg['failed_threshold'] to ignore");
    }
    sleep($cfg['sleep']);

    $total_inserted+=$inserted_count;
    $total_failed+=$failed_count;
}



echo "\nFinished\n\nTotal Success: $total_inserted     Total failed: $total_failed\n\n\n";




