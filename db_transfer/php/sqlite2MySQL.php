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
$cfg['failed_1st']=(int)$cfg['failed_1st'];

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


# 这里是尝试根据数据源字段自动建表，待继续完成
# 遍历字段信息 $table_info ，对所有支持的类型处理
# 这里是对sqlite的table_info, mssql等其他看做兼容的时候再确定是整合一起还是分别处理
$create_table_info=array();
$un_supported_column_type=0;    # 自动建表功能不支持的字段数，如有，且目标表不存在要报错
#   元素为待创建表的字段类型等信息:  字段名，类型，是否允许NULL，默认值，是否主键
#       name, type, null, default, pk
foreach ($table_info as $col => $info) {
    $item['name']=$info['name'];
    $item['null']= ($info['notnull']==0) ? ($info['pk'] ? FALSE : TRUE) : FALSE ;
    $item['default']= $info['dflt_value'];
    $item['pk']= $info['pk'] ? TRUE : FALSE;
    $type_str=strtolower($info['type']);
    # 假定带 int 字样的都是int, 同时还有big的为bigint
    $type='';
    $unsigned_flag=(strpos($type_str,'unsigned')!==FALSE) ? 'UNSIGNED ' : '';
    if(strpos($type_str,'int')!==FALSE){
        # int 型，再检查是否需要 bigint, UNSIGNED
        if(strpos($type_str,'big')!==FALSE){
            $type=$unsigned_flag.'BIGINT';
        }else{
            $type=$unsigned_flag.'INT';
        }
    }elseif(strpos($type_str,'real')!==FALSE || strpos($type_str,'float')!==FALSE){
        $type=$unsigned_flag.'FLOAT';       # float型，4字节
    }elseif(strpos($type_str,'double')!==FALSE){
        $type=$unsigned_flag.'FLOAT';       # double型，8字节
    }elseif(strpos($type_str,'decimal')!==FALSE){
        $type=$info['type'];                # DECIMAL，照原样转mysql
    }elseif(strpos($type_str,'char')!==FALSE
        || strpos($type_str,'text')!==FALSE
        || strpos($type_str,'numeric')!==FALSE
        || strpos($type_str,'boolean')!==FALSE
        || strpos($type_str,'date')!==FALSE ){
            # [v][v]char* 及其他一些极可能为字符串的类型，需要计算长度 ....
        $type='VARCHAR';
    }else{
        # 未知类型，如果需要程序自动建表（目标不存在）时要报错
        $un_supported_column_type+=1;
        $type='_UNSUPPORTED_';
    }
    $item['type']=$type;
    $create_table_info[$col]=$item;
}




# 检查源表主键id数字极值及记录条数
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
$batch_number_predict=ceil(($pk_to - $pk_from)/$cfg['batch_size']);

echo "\nsource pk range [{$source_min}, {$source_max}]";
echo "\nto transfer range [{$pk_from}, {$pk_to}]";
echo "\nbatch_number_predict: $batch_number_predict \n\n";



# link MySQL 连接MySQL，
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


# 检查目标表是否存在，不存在则创建表
$sql="select count(TABLE_NAME) as cnt from INFORMATION_SCHEMA.TABLES
    where TABLE_SCHEMA='{$cfg['mysql']['db']}' and TABLE_NAME='$target_table'";
$res=$link->query($sql);
$row=$res->fetch(PDO::FETCH_ASSOC);
if( (int)$row['cnt'] == 0){
    # 目标表不存在
    echo "\ntarget table `$target_table` Not exists, try to create it...\n";
    if($un_supported_column_type){
        # 自动建表功能不支持的字段，列出报错
        echo "\n\ntarge table not exists, and following columns NOT supported by auto-create-table\n";
        foreach ($create_table_info as $col => $info) {
            if($info['type']=='_UNSUPPORTED_'){
                print "    [{info['name']}]  {$table_info['type']}\n";
            }
        }
        exit("\nYou can create target table manually, or change the source column type.\n");
    }
    # 计算text型字符长度
    if($cfg['source']=='mssql'){
        $funlen='len';
        $mka='[';
        $mkb=']';
    }elseif($cfg['source']=='sqlite'){
        $funlen='length';
        $mka='[';
        $mkb=']';
    }else{
        exit("\nOther Database Engine NOT completed\n");
    }
    $columns=array();
    foreach ($create_table_info as $col => $info) {
        if($info['type']=='VARCHAR'){
            $columns[]="max($funlen($mka".$info['name']."$mkb)) as $mka". $info['name'] . $mkb;
        }
    }
    if($columns){
        echo "\nretrive text columns length:";
        $sql='Select '. implode(', ',$columns) ." from $mka$source_table$mkb";
        try{
            $res=$conn->query($sql);
        }catch(PDOException $e) {
            exit("\n[Error] Connection failed:\n".$e->getMessage()."\n$dsn");
        }
        $row=$res->fetch(PDO::FETCH_ASSOC);
        foreach ($row as $name => $length) {
            if($length <= 255){
                $create_table_info[$name]['type'].='('.$length.')';
            }elseif($length >= 16777215){
                $create_table_info[$name]['type'] ='LONGTEXT';
                $create_table_info[$name]['default']=NULL;
            }else{
                $create_table_info[$name]['type'] ='MEDIUMTEXT';
                $create_table_info[$name]['default']=NULL;
            }
            print "\n  [$name]:  $length  =>   {$create_table_info[$name]['type']}";
        }
    }
    # 构造建表语句
    $columns=array();
    $pk_name='';
    foreach ($create_table_info as $col => $info) {
        # name, type, null, default, pk
        $buff= '`'.$info['name'].'`'
                .' '. $info['type']
                . ($info['null'] ? ' NULL' : ' NOT NULL')
                . ($info['default'] ? ' default '.$info['default'] : '');
        $columns[]=$buff;
        if($info['pk']){
            $pk_name=$info['name'];
        }
    }
    $sql="CREATE TABLE `$target_table`(\n  "
            . implode(",\n  ",$columns)
            . ($pk_name ? ",\n  PRIMARY KEY (`".$pk_name."`)" : '')
            . "\n) DEFAULT CHARSET=".$cfg['mysql']['charset'];
    echo "\n\nCreating target table:\n----------------\n$sql\n----------------\n";
    try{
        $link->query($sql);
    }catch(PDOException $e){
        exit("[Error] create target table failed:\n".$e->getMessage()."\n".$sql);
    }
}



# 并准备PDO 匿名参数化的插入 statement，匿名参数化查询
echo "\n\npreparing insert statement";
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


# 分批遍历数据源，并使用前面 $insertor 插入数据
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
        # 检查无成功却连续失败
        if( ($total_inserted==0 && $inserted_count==0 && $failed_count >= $cfg['failed_1st']) ){
            exit("\n\nFirst 10 lines All failed");
        }
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
            echo "\n[Warning] insert failed:\n".$e->getMessage()
                    . "\n\n".$insertor->queryString."\n".var_export($values);
            $failed_count+=1;
        }
    }
    $pos=$batch_end;
    echo "  Success: $inserted_count    Fail: $failed_count";
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



echo "\n\nFinished\n\nTotal\n    Success: $total_inserted\n    Failed: $total_failed\n\n\n";




