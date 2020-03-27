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
require($script_root.'/libs.php');
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
$cfg['compact_varchar']=(int)$cfg['compact_varchar'];
$cfg['compact_text']=(int)$cfg['compact_text'];

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
    $conn=connect_sqlite($cfg);
    $source_table=$cfg['sqlite']['table'];
    $source_pk=$cfg['sqlite']['pk'];
    # 全局变量：源数据库的计算长度的函数 $funlen    字符名、表名的括号 $mka, mkb (下同)
    $funlen='length';$mka='['; $mkb=']';
}elseif($cfg['source']=='mssql'){
    $conn=connect_mssql($cfg);
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
    $table=$cfg['sqlite']['table'];
    $table_info=get_table_info_sqlite($conn,$table);
    # TODO 下面三个全局变量有点丑陋
    $columns_names=array_keys($table_info);
    $columns_source='[' . implode('], [',$columns_names) . ']';
    $columns_target='`'.implode('`, `',$columns_names).'`';
}
elseif($cfg['source']=='mssql'){
    $table=$cfg['mssql']['table'];
    # 有点恶心，函数返回两个值，第二个值是一组类型转换的数组
    list($table_info,$columns_cvt)=get_table_info_mssql($conn,$table);
    # TODO 下面三个全局变量有点丑陋
    $columns_names=array_keys($table_info);
    $columns_source=implode(', ', $columns_cvt );
    $columns_target='`'.implode('`, `',$columns_names).'`';
    unset($columns_cvt);
}


# 源表主键，若未配置指定，则在$table_info 数组中尝试寻找
if(!$source_pk){
    $source_pk=get_pk_clumn_name($table_info);
}
if(!$source_pk){
    exit("\n\nPrimaryKey (pk) column not found automatically, please config it in config.php\n");
}




# ## 类型转换规则
# 因为要同时兼容sqlite, mssql, 未来可能还会支持其他数据库，所以并非支持所有类型，部分类型也会丢失数据
# 但 int, bigint, (var)char, text等最常用的主要类型会完美兼容
# data/time 相关类型，尽量支持，可能会丢失部分数据（如 mssql的time类型的毫秒部分）
# float/real/double 相关类型尽量支持，也可能丢失数据(暂未完美测试 TODO )


# 尝试根据数据源字段自动建表
# 遍历字段信息 $table_info ，对所有支持的类型处理
# 这里是对sqlite的table_info, mssql等其他看做兼容的时候再确定是整合一起还是分别处理
$create_table_info=get_create_table_info($table_info);




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
#echo "\n\n$dsn\n\n";
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
    # 检查是否有自动建表功能不支持的字段数，如有报错
    $un_supported_column_type=get_supported_column_count($create_table_info);
    if($un_supported_column_type){
        echo "\n\ntarge table not exists, and following columns NOT supported by auto-create-table\n";
        foreach ($create_table_info as $col => $info) {
            if($info['type']=='_UNSUPPORTED_'){
                print "    [{$info['name']}]  {$table_info[$info['name']]['type']}\n";
            }
        }
        exit("\nYou can create target table manually, or change the source column type.\n");
    }
    # 抽取待计算长度的列(create_table_info[][type]为 VARCHAR, TEXT 列)，存储在临时数组 $columns 中
    # 使用max(length(col)) as col 查表，然后将结果更新到 $create_table_info() 中，
    $columns=array();
    foreach ($create_table_info as $col => $info) {
        # 先判断是否是mssql下ntext并且待计算
        if($cfg['compact_text'] && $info['type']=='TEXT' && $cfg['source']=='mssql'){
            # mssql 下(n)text计算长度要转成nvarchar(max),
            # text型本身不需要，但前面计算类型时把ntext与text放在一起，转一下也没害处
            $columns[]="max($funlen(convert(varchar(max),$mka".$info['name']."$mkb))) as $mka". $info['name'] . $mkb;
        }elseif($cfg['compact_varchar'] && $info['type']=='VARCHAR'
            || $cfg['compact_text'] && $info['type']=='TEXT' ){
            $columns[]="max($funlen($mka".$info['name']."$mkb)) as $mka". $info['name'] . $mkb;
        }
    }
    if($columns){
        echo "\nretrive text columns length:";
        $sql='Select '. implode(', ',$columns) ." from $mka$source_table$mkb";
        try{
            $res=$conn->query($sql);
        }catch(PDOException $e) {
            exit("\n[Error] Connection failed:\n".$e->getMessage()."\n$sql");
        }
        $row=$res->fetch(PDO::FETCH_ASSOC);
        foreach ($row as $name => $length) {
            if($length <= 255){
                # max(length(FN)) is NULL if all value NULL of this column
                $length=(!$length) ? 1 : $length;
                $create_table_info[$name]['type'] ='VARCHAR('.$length.')';
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
        if($source_pk==$info['name']){
            $info['null']=0;
            $info['pk']=1;
        }
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

$iconv=0;
if(isset($cfg['mssql']['iconv_from']) && $cfg['mssql']['iconv_from']
    && isset($cfg['mssql']['iconv_to']) && $cfg['mssql']['iconv_to']){
    $iconv=1;
    $iconv_from=$cfg['mssql']['iconv_from'];
    $iconv_to=$cfg['mssql']['iconv_to'];
}

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
        if( $total_inserted==0 && $inserted_count==0 && $failed_count >= $cfg['failed_1st'] ){
            if($cfg['failed_1st']){
                exit("\n\nFirst ${cfg['failed_1st']} lines All failed. configure item cfg['failed_1st'] ");
            }
        }
        $values = array();
        foreach ($columns_names as $col) {
            # fuuuking mssql need encoding convert, skip NULL.
            # seems all mssql rs columns are string except NULL
            if($cfg['source']=='mssql' && $row[$col]!==NULL && $iconv==1){
                $values[] = @iconv($iconv_from,$iconv_to.'//IGNORE',$row[$col]);
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

    if (version_compare(PHP_VERSION, '5.3.0') >= 0) {
        gc_collect_cycles();
    }
}



echo "\n\nFinished.\n\nTotal summary:\n    Success: $total_inserted\n    Failed:  $total_failed\n\n\n";




