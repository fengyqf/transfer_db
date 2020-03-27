<?php
isset($script_root) or exit('config file not accessable');


# -- function start --------------------------------------

# TODO 测试
function connect_sqlite($cfg){
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
    return $conn;
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
#   [x] TODO 下面关于sqlsrv的经验，似乎并不是，相当有点神奇，以后再做各种环境下的兼容测试吧
#   经验表明 sqlsrv，说需要安装sql server native client 2008，而这货的在msdn上下载链接已死，坑
#   因此，拟首选支持PDO_DBLib的三个驱动
function connect_mssql($cfg){
    # 先针对 mssql_* 库做环境设置，且不管是不是使用 mssql_*
    if(extension_loaded('mssql')){
        ini_set('mssql.textsize','2147483647');
        ini_set('mssql.textlimit','2147483647');
        # Available since PHP 5.1.2 when built with FreeTDS 7.0 or greater
        ini_set('mssql.charset', 'utf-8');
    }
    # $host,$port,$user,passwd,db,table
    $host=$cfg['mssql']['host'];
    if(isset($cfg['mssql']['port']) && $cfg['mssql']['port']){
        $host=$host.(PHP_OS=='WINNT'?',':':').$cfg['mssql']['port'];
    }
    $user=$cfg['mssql']['user'];
    $passwd=$cfg['mssql']['passwd'];
    $db=$cfg['mssql']['db'];
    $driver=isset($cfg['mssql']['driver']) ? $cfg['mssql']['driver'] : '';

    # 从各种pdo驱动中找可用的，首选配置中指定的$driver
    # 其中 odbc 冒号后面的是驱动标识，在构造dns时按冒号拆分拼接成 odbc:Driver={drv-name};...的形式
    $pdo_drivers= array(
        'sqlsrv','dblib',
        'odbc:SQL Server Native Client 11.0',
        'odbc:SQL Server Native Client 10.0',
        'odbc:SQL Server',
        'odbc:SQL Native Client',
        'odbc:unixODBC',
        'odbc:FreeTDS',
        'odbc','mssql','sybase',
    );

    if($driver){
        $pdo_drivers = array_merge(array($driver),$pdo_drivers);
    }

    foreach ($pdo_drivers as $dsn_prefix) {
        # 逐个尝试可用的 PDO_DBLIB/php_pdo_mssql 可用驱动，win32 版 php5.2自带的是 mssqql
        # win32 php 5.2 下测试：驱动不存在时，竟然 PDO::code 是 0，只好用message判断
        # 按手册dsn支持 charset 参数，但实际使用中 charset=gb2312 与 charset=utf-8 似乎并没有任何区别，忽略了
        # TODO : odbc 待测试
        echo "trying pdo driver $dsn_prefix ...... ";
        if($dsn_prefix=='sqlsrv'){
            $dsn="sqlsrv:Server={$host};Database={$db};";
        }elseif(substr($dsn_prefix,0,4)=='odbc'){
            if(substr($dsn_prefix,4,1)==':' && strlen($dsn_prefix)>5){
                $odbc_drv=substr($dsn_prefix,5);
            }else{
                $odbc_drv='SQL Server';
            }
            $dsn="odbc:Driver={{$odbc_drv}};Server={$host};Database={$db};";
        }else{
            $dsn="$dsn_prefix:host={$host};dbname={$db};";
        }
        try{
            $conn=new PDO($dsn,$user, $passwd);
            echo " Succeed!";
        }catch(PDOException $e) {
            $mesg=$e->getMessage();
            if(substr($dsn,0,4)=='odbc'){
                echo " Failed.";
            }elseif(strpos($mesg,'not find driver')!==FALSE){
                echo " Not found.";
            }else{
                exit("\n[Error] Connection failed:\n$mesg\n$dsn\n");
            }
        }
        echo "\n";
        if(isset($conn) && $conn){
            echo "\nConnection established with $dsn_prefix driver\n";
            break;
        }
    }
    if(!isset($conn) || !$conn){
        exit("\n[Error] All pdo driver tried, but failed.\n\n");
    }
    return $conn;
}





# 读取数据源字段信息到数组 $table_info, 键为字段名，值为关联数组（键为 name, type, null, default, pk, length）
# 并构造读取、插入语句的字段列表，拼接为sql字符串     $columns_source, $columns_target
#   含兼容mssql的ntext/nvarchar(max)/datetime类型转换convert(...)

function get_table_info_sqlite($conn,$table){
    $table_info=array();
    $sql="PRAGMA table_info([{$table}])";
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
    }
    $res->closeCursor();
    return $table_info;
}



function get_table_info_mssql($conn,$table){
    $source_table=$table;
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
        }elseif(strtolower($row['DATA_TYPE'])=='uniqueidentifier' ){
            $columns_cvt[]="convert(char(36),[{$row['COLUMN_NAME']}]) as {$row['COLUMN_NAME']}";
        }elseif($row['DATA_TYPE']=='smalldatetime' || $row['DATA_TYPE']=='datetime') {
            $columns_cvt[]="convert(varchar(19),[{$row['COLUMN_NAME']}],120) as {$row['COLUMN_NAME']}";
        }else{
            $columns_cvt[]="[{$row['COLUMN_NAME']}]";
        }
    }
    $res->closeCursor();
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
    return array($table_info,$columns_cvt);
}



# 从 $table_info 数组中检查主键列，返回主键列名，若无则返回 NULL
function get_pk_clumn_name($table_info){
    $source_pk=NULL;
    foreach($table_info as $name=>$info){
        if($info['pk']){
            $source_pk=$info['name'];
            return $source_pk;
        }
    }
    # 查表结构也没有 pk 列，尝试使用第一列(若列名为id且类型为int)
    reset($table_info);
    $tmp=current($table_info);
    if(strpos($tmp['type'],'int')!==FALSE && strtolower($tmp['name'])=='id' ){
        $source_pk=$tmp['name'];
    }
    return $source_pk;
}

# ## 类型转换规则
# 因为要同时兼容sqlite, mssql, 未来可能还会支持其他数据库，所以并非支持所有类型，部分类型也会丢失数据
# 但 int, bigint, (var)char, text等最常用的主要类型会完美兼容
# data/time 相关类型，尽量支持，可能会丢失部分数据（如 mssql的time类型的毫秒部分）
# float/real/double 相关类型尽量支持，也可能丢失数据(暂未完美测试 TODO )

# 尝试根据数据源字段自动建表，返回关联数组，
#   元素为待创建表的字段类型等信息:  
#       字段名，类型，是否允许NULL，默认值，是否主键
#       name, type, null, default, pk
# 这里是对sqlite的table_info, mssql等其他看做兼容的时候再确定是整合一起还是分别处理
function get_create_table_info($table_info){
    # 遍历字段信息 $table_info ，对所有支持的类型处理
    $create_table_info=array();
    # 自动建表功能不支持的字段数，如有，且目标表不存在要报错，这个内部变量似乎并没什么用了
    $un_supported_column_type=0;    
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
        }elseif($type_str=='uniqueidentifier'){
            $type='CHAR(36)';       # uniqueidentifier型 guid，以CHAR(36)转存
        }else{
            # 未知类型，如果需要程序自动建表（目标不存在）时要报错
            $un_supported_column_type+=1;
            $type='_UNSUPPORTED_';
        }
        $item['type']=$flag.$type;
        $create_table_info[$col]=$item;
    }
    return $create_table_info;
}


function get_supported_column_count($create_table_info){
    $cnt=0;
    foreach ($create_table_info as $name => $info) {
        if($info['type']=='_UNSUPPORTED_'){
            $cnt+=1;
        }
    }
    return $cnt;
}


# -- function end --------------------------------------


