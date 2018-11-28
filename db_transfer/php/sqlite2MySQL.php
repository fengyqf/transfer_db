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
if($cfg['php_memory_limit']){
    ini_set ('memory_limit', $cfg['php_memory_limit']);
}



# 连接数据源，并根据不同源类型定义一些全局变量等
if($cfg['source']=='sqlite') {
    # 非根绝对路径，则默认为相对脚本的相对路径
    if($cfg['sqlite']['filepath'][1]!=':' || $cfg['sqlite']['filepath'][0]!='/' ){
        $data_path=$cfg['sqlite']['filepath'];
    }else{
        $data_path=$script_root.'/'.$cfg['sqlite']['filepath'];
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
    # 全局变量：源数据库的计算长度的函数 $funlen    字符名、表名的括号 $mka, mkb (下同)
    $funlen='length';$mka='['; $mkb=']';
}
# connect mssql 连接mssql, and fix environment
# TODO 暂未知PDO_MSSQL下的textsize怎么定义，遇上再说吧；
# TODO 假定在PDO_sqldrv模式下服务器端口的指定方式与mssql_* 一致（winnt与*nix不同），实测后再改

# 据 php 手册：  PDO驱动 - MS SQL Server (PDO) - PDO_DBlib DSN 一节，其dns前缀为 sybase,mssql,dblib
#   PHP 5.3+ 以后不再支持，所以只能使用旧版本php。
#   sybase: if PDO_DBLIB was linked against the Sybase ct-lib libraries,
#   mssql: if PDO_DBLIB was linked against the Microsoft SQL Server libraries,
#   dblib: if PDO_DBLIB was linked against the FreeTDS libraries
# 据 php 手册：  PDO驱动 - MS SQL Server (PDO) - PDO_SQLSRV DSN  一节，其dns前缀为 sqlsrv
#   sqlsrv 支持 SQL Server 2005 +, 只支持windows版php, linux 要使用 ODBC
#     下载v3.0  http://msdn.microsoft.com/en-us/sqlserver/ff657782.aspx
#     下载v2.0  http://download.microsoft.com/download/C/D/B/CDB0A3BB-600E-42ED-8D5E-E4630C905371/SQLSRV20.EXE
#     系统要求  http://msdn.microsoft.com/en-us/library/cc296170.aspx
#   odbc for linux:  http://www.microsoft.com/download/en/details.aspx?id=28160
#
#   经验表明 sqlsrv，说需要安装sql server native client 2008，而这货的在msdn上下载链接已死，坑
#   因此，拟首选支持PDO_DBLib的三个驱动
elseif($cfg['source']=='mssql'){
    ini_set('mssql.textsize','2147483647');
    ini_set('mssql.textlimit','2147483647');
    if($cfg['mssql']['port']!=''){
        $cfg['mssql']['host']=$cfg['mssql']['host'].(PHP_OS=='WINNT'?',':':').$cfg['mssql']['port'];
    }
    if(!extension_loaded('pdo_sqlsrv')){
        exit("\n[Error] PDO driver not exists: pdo_sqlsrv");
    }
    # 找可用驱动
    $pdodrivers= $cfg['mssql']['driver'] ? array($cfg['mssql']['driver'])
                     : array('mssql','dblib','sybase','odbc','sqlsrv');
    foreach ($pdodrivers as $dsn_prefix) {
        # 逐个尝试可用的 PDO_DBLIB/php_pdo_mssql 可用驱动，win32 版 php5.2自带的是 mssqql
        # win32 php 5.2 下测试：驱动不存在时，竟然 PDO::code 是 0，只好用message判断
        # 按手册dsn支持 charset 参数，但实际使用中 charset=gb2312 与 charset=utf-8 似乎并没有任何区别，忽略了
        # TODO : odbc 待测试
        if($dsn_prefix=='sqlsrv'){
            $dsn="sqlsrv:Server={$cfg['mssql']['host']};Database={$cfg['mssql']['db']};";
        }elseif($dsn_prefix=='odbc'){
            $dsn="odbc:Driver={SQL Native Client};Server={$cfg['mssql']['host']};Database={$cfg['mssql']['db']};";
        }else{
            $dsn="$dsn_prefix:host={$cfg['mssql']['host']};dbname={$cfg['mssql']['db']};";
        }
        try{
            $conn=new PDO($dsn,$cfg['mssql']['user'], $cfg['mssql']['passwd']);
        }catch(PDOException $e) {
            $mesg=$e->getMessage();
            if(strpos($mesg,'not find driver')!==FALSE){
                echo "\nMSSQL pdo driver $dsn_prefix: Not found.";
            }else{
                exit("\n[Error] Connection failed:\n$mesg\n$dsn\n");
            }
        }
        if($conn){
            echo "\nConnection established with $dsn_prefix driver\n";
            break;
        }
    }
    if(!$conn){
        exit("\n[Error] connect mssql failed. may be you need try another pdo driver\n\n");
    }
    $source_table=$cfg['mssql']['table'];
    $source_pk=$cfg['mssql']['pk'];
    $funlen='len'; $mka='['; $mkb=']';
}

$conn ? $conn->setAttribute(PDO::ATTR_ERRMODE,PDO::ERRMODE_EXCEPTION)
    : exit("\n[Error] unexpected error: no source db connection!");



# 读取数据源字段信息到数组 $table_info, 键为字段名，值为关联数组（键为 name, type, null, default, pk, length）
# 并构造读取、插入语句的字段列表，拼接为sql字符串     $columns_source, $columns_target
#   含兼容mssql的ntext/nvarchar(max)/datetime类型转换convert(...)
$table_info=array();
if($cfg['source']=='sqlite'){
    $sql="PRAGMA table_info([{$cfg['sqlite']['table']}])";
    $res=$conn->query($sql);
    while($row=$res->fetch(PDO::FETCH_ASSOC)){
        $table_info[$row['name']]=
            array(
                'name'=>$row['name'],
                'type'=>$row['type'],
                'null'=>(($row['notnull']==0) ? ($row['pk'] ? FALSE : TRUE) : FALSE),
                'default'=>$row['dflt_value'],
                'pk'=>($row['pk'] ? TRUE : FALSE),
                'length'=>NULL,
            );
        if($row['pk'] && !$source_pk){
            $source_pk=$row['name'];
        }
    }
    $columns_names=array_keys($table_info);
    $columns_source='[' . implode('], [',$columns_names) . ']';
    $columns_target='`'.implode('`, `',$columns_names).'`';
}
elseif($cfg['source']=='mssql'){
    $sql="select COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,
        IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH
        from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{$source_table}'";
    $res=$conn->query($sql);
    $columns_cvt=array();
    while($row=$res->fetch(PDO::FETCH_ASSOC)){
        $table_info[$row['COLUMN_NAME']]=
            array(
                'name'=>$row['COLUMN_NAME'],
                'type'=>$row['DATA_TYPE'],
                'null'=>(bool)$row['IS_NULLABLE'],
                'default'=>$row['COLUMN_DEFAULT'],
                'pk'=> FALSE,
                'length'=>(int)$row['CHARACTER_MAXIMUM_LENGTH'],
            );
        if($row['DATA_TYPE']=='ntext' ||
                ($row['CHARACTER_MAXIMUM_LENGTH']=='-1' && $row['DATA_TYPE']=='nvarchar')){
            $columns_cvt[]="convert(varchar(max),[{$row['COLUMN_NAME']}]) as {$row['COLUMN_NAME']}";
        }elseif($row['DATA_TYPE']=='smalldatetime' || $row['DATA_TYPE']=='datetime') {
            $columns_cvt[]="convert(varchar(19),[{$row['COLUMN_NAME']}],120) as {$row['COLUMN_NAME']}";
        }else{
            $columns_cvt[]="[{$row['COLUMN_NAME']}]";
        }
    }
    # 通过sql语句查标识列名
    $sql="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.columns WHERE TABLE_NAME='{$source_table}'
            AND  COLUMNPROPERTY(OBJECT_ID('{$source_table}'),COLUMN_NAME,'IsIdentity')=1";
    try{
        $res=$conn->query($sql);
        if($row=$res->fetch(PDO::FETCH_ASSOC)){
            $source_pk=$row['COLUMN_NAME'];
            $table_info[$source_pk]['pk']=TRUE;
            echo "\nmssql identity column found: {$row['COLUMN_NAME']}";
        }
    }catch(PDOException $e) {
        exit("[Error] query failed:\n".$e->getMessage()."\n".$sql);
    }
    $columns_names=array_keys($table_info);
    $columns_source=implode(', ', $columns_cvt );
    $columns_target='`'.implode('`, `',$columns_names).'`';
    unset($columns_cvt);
}


# 配置中没有指定 pk，查表结构也没有 pk 列，尝试使用第一列(列名为id且类型为int)
if(!$source_pk){
    reset($table_info);
    $tmp=current($table_info);
    if(strpos($tmp['type'],'int')!==FALSE && strtolower($tmp['name'])=='id' ){
        $source_pk=$tmp['name'];
    }else{
        exit("\n\nPrimaryKey (pk) column not found automatically, please config it in config.php\n");
    }
    unset($tmp);
}


# ## 类型转换规则
# 因为要同时兼容sqlite, mssql, 未来可能还会支持其他数据库，所以并非支持所有类型，部分类型也会丢失数据
# 但 int, bigint, (var)char, text等最常用的主要类型会完美兼容
# data/time 相关类型，尽量支持，可能会丢失部分数据（如 mssql的time类型的毫秒部分）
# float/real/double 相关类型尽量支持，也可能丢失数据(暂未完美测试 TODO )


# 尝试根据数据源字段自动建表
# 遍历字段信息 $table_info ，对所有支持的类型处理
# 这里是对sqlite的table_info, mssql等其他看做兼容的时候再确定是整合一起还是分别处理
$create_table_info=array();
$un_supported_column_type=0;    # 自动建表功能不支持的字段数，如有，且目标表不存在要报错
#   元素为待创建表的字段类型等信息:  字段名，类型，是否允许NULL，默认值，是否主键
#       name, type, null, default, pk
foreach ($table_info as $col => $info) {
    $item['name']=$info['name'];
    $item['null']= $info['null'];
    $item['default']= $info['default'];
    $item['pk']= $info['pk'] ? TRUE : FALSE;
    $type_str=strtolower($info['type']);
    # 假定带 int 字样的都是int, 同时还有big的为bigint
    $type='';
    $flag=(strpos($type_str,'unsigned')!==FALSE) ? 'UNSIGNED ' : '';
    # int 型，再检查是否需要 bigint, UNSIGNED
    if(strpos($type_str,'bigint')!==FALSE){
        $type='BIGINT';
    }elseif(strpos($type_str,'int')!==FALSE){
        $type='INT';
    }elseif(strpos($type_str,'bit')!==FALSE){
        $type='TINYINT';       # tinyint for bit (mssql only)
    }elseif(strpos($type_str,'float')!==FALSE){
        $type='FLOAT';       # float型，4字节，忽略 (M,D)
    }elseif(strpos($type_str,'real')!==FALSE || strpos($type_str,'double')!==FALSE){
        $type='DOUBLE';       # double型，8字节，忽略 (M,D)
    }elseif(strpos($type_str,'decimal')!==FALSE){
        $type=$info['type'];                # DECIMAL，照原样转mysql
    }elseif(strpos($type_str,'datetime')!==FALSE){
        $type='DATETIME';                # datetime,smalldatetime都转为datetime
    }elseif(strpos($type_str,'date')!==FALSE){
        $type='DATE';                # date
    }elseif(strpos($type_str,'time')!==FALSE){
        $type='TIME';                # time， mssql time类型包含毫秒，而MySQL似乎不支持毫秒，所以会丢失数据
    }elseif(substr($type_str,0,4)=='char' || substr($type_str,0,5)=='nchar'){
        $type='CHAR';       # CHAR 型
    }elseif(strpos($type_str,'boolean')!==FALSE){
        $type='TINYINT';                # boolean use tinyint instead
    }elseif(strpos($type_str,'varchar')!==FALSE
            || strpos($type_str,'numeric')!==FALSE
            || strpos($type_str,'boolean')!==FALSE ){
        # [v][v]char* 及其他一些极可能为字符串的类型，需要计算长度，还有下面的text
        $type='VARCHAR';
    }elseif(strpos($type_str,'text')!==FALSE){
        # (n)text 要单独列出来。 因为后面计算长度时，对于text长度计算还要convert()转类型
        $type='TEXT';
    }else{
        # 未知类型，如果需要程序自动建表（目标不存在）时要报错
        $un_supported_column_type+=1;
        $type='_UNSUPPORTED_';
    }
    $item['type']=$flag.$type;
    $create_table_info[$col]=$item;
}




# 检查源表主键id数字极值及记录条数
$sql="select min($mka{$source_pk}$mkb) as min_pk,max($mka{$source_pk}$mkb) as max_pk,count(*) as cnt from $mka{$source_table}$mkb";
try{
    $res=$conn->query($sql);
    $row=$res->fetch(PDO::FETCH_ASSOC);
    $source_min=(int)$row['min_pk'];
    $source_max=(int)$row['max_pk'];
    $source_count=(int)$row['cnt'];
}catch(PDOException $e){
    if($e->getCode()=='HY000'){
        echo "\ntable {$source_table} seems not found in source database.\n\n";
    }
    exit("[Error] query failed:\n".$e->getMessage()."\n".$sql);
}



$pk_from = $cfg['start'] ? $cfg['start'] : $source_min;
$pk_to = $cfg['end'] ? $cfg['end'] : $source_max;
$batch_number_predict=ceil(($pk_to - $pk_from)/$cfg['batch_size']);

echo "\nsource pk range [{$source_min}, {$source_max}]";
echo "\nto transfer range [{$pk_from}, {$pk_to}]";
echo "\nbatch_number_predict: $batch_number_predict \n";



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
                print "    [{$info['name']}]  {$table_info[$info['name']]['type']}\n";
            }
        }
        exit("\nYou can create target table manually, or change the source column type.\n");
    }
    # 抽取待计算长度的列(查表结构得到的 VARCHAR, TEXT)，存储在临时数组 $columns 中
    # 使用max(length(col)) as col 查表，然后将结果更新到 $create_table_info() 中，
    $columns=array();
    foreach ($create_table_info as $col => $info) {
        if($info['type']=='VARCHAR'){
            $columns[]="max($funlen($mka".$info['name']."$mkb)) as $mka". $info['name'] . $mkb;
        }elseif( $info['type']=='TEXT' && $cfg['source']=='mssql'){
            # mssql 下(n)text计算长度要转成nvarchar(max)
            $columns[]="max($funlen(convert(varchar(max),$mka".$info['name']."$mkb))) as $mka". $info['name'] . $mkb;
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
                # max(length(FN)) is NULL if all value NULL of this column
                $length=(!$length) ? 1 : $length;
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
    # 构造建表语句，每个字段的相关子语句字符串放到 columns() 数组中
    $columns=array();
    $pk_name='';
    foreach ($create_table_info as $col => $info) {
        # name, type, null, default, pk
        $columns[]= '`'.$info['name'].'`'
                .' '. $info['type']
                . ($info['null'] ? ' NULL' : ' NOT NULL')
                . ($info['default'] ? ' default '.$info['default'] : '');
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
$chars_b=ceil(log10($batch_number_predict));    # 输出消息中的字符串宽度
$chars_n=ceil(log10($source_max));
$chars_s=ceil(log10($batch_size));
echo "\ntransfer data...";
while($pos <= $pk_to){
    $batch_num++;
    $batch_end=$pos+$batch_size;
    echo "\n".sprintf("%{$chars_b}d",$batch_num).'/'.$batch_number_predict
        ."  [".sprintf("%{$chars_n}d",$pos).", ".sprintf("%{$chars_n}d",$batch_end).")...  ";
    $sql="select $columns_source from $mka{$source_table}$mkb
            where $mka{$source_pk}$mkb >= $pos and $mka{$source_pk}$mkb < $batch_end";
    try{
        $res=$conn->query($sql);
    }catch(PDOException $e){
        exit("\n[Error] query failed:\n".$e->getMessage()."\n$sql\n\n");
    }
    $inserted_count=$failed_count=0;
    while ($row=$res->fetch(PDO::FETCH_ASSOC)) {
        # 检查无成功却连续失败
        if( ($total_inserted==0 && $inserted_count==0 && $failed_count >= $cfg['failed_1st']) ){
            exit("\n\nFirst 10 lines All failed");
        }
        $values = array();
        foreach ($columns_names as $col) {
            # fuuuking mssql need encoding convert, skip NULL.
            # seems all mssql rs columns are string except NULL
            if($cfg['source']=='mssql' && $row[$col]!==NULL){
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
            if($e->getCode()==23000){
                exit();
            }
        }
    }
    $pos=$batch_end;
    echo "  Success: ".sprintf("%{$chars_s}d",$inserted_count)."    Fail: $failed_count";
    $res->closeCursor();
    # 检测当前批次出错率是否超出阈值
    $failed_ratio=0;
    if($failed_count || $inserted_count){
        $failed_ratio=$failed_count/($failed_count+$inserted_count)*100;
    }
    if($failed_ratio > $failed_threshold){
        exit("\n\n[Error] To many error occered\nYou can change \$cfg['failed_threshold'] to ignore");
    }
    !$inserted_count or sleep($cfg['sleep']);

    $total_inserted+=$inserted_count;
    $total_failed+=$failed_count;
}



echo "\n\nFinished.\n\nTotal summary:\n    Success: $total_inserted\n    Failed:  $total_failed\n\n\n";




