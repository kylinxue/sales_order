-- 在hive中建立RDS库表
drop database if exists rds cascade;
create database rds;

use rds; 
-- 建立客户过渡表 
create table customer ( 
    customer_number int comment 'number', 
    customer_name varchar( 30) comment 'name', 
    customer_street_address varchar(30) comment 'address', 
    customer_zip_code int comment 'zipcode', 
    customer_city varchar( 30) comment 'city', customer_state varchar( 2) comment 'state' );

-- 建立产品过渡表
create table product ( 
    product_code int comment 'code', 
    product_name varchar(30) comment 'name', 
    product_category varchar(30) comment 'category' ); 
    
-- 建立销售订单过渡表 
create table sales_order ( 
    order_number int comment 'order number', 
    customer_number int comment 'customer number', 
    product_code int comment 'product code', 
    order_date timestamp comment 'order date', 
    entry_date timestamp comment 'entry date', 
    order_amount decimal( 10 , 2 ) comment 'order amount' );

------------------------------------------------------------------
-- 在hive中建立TDS库表
-- 建立数据仓库数据库 
drop database if exists dw cascade; 
create database dw;

use dw;

-- 建立日期维度表
create table date_dim (
    date_sk int comment 'surrogate key' ,
    date0 date comment 'date, yyyy-mm-dd' ,   -- hive1.2.2不允许字段名为date
    month tinyint ,
    month_name varchar(9) ,
    quarter tinyint ,
    year smallint )
comment 'date dimension table'
row format delimited fields terminated by ','
stored as textfile;

-- 建立客户维度表
create table customer_dim (
    customer_sk int comment 'surrogate key' ,
    customer_number int ,
    customer_name varchar(50) ,
    customer_street_address varchar(50) ,
    customer_zip_code int ,
    customer_city varchar(30) ,
    customer_state varchar(2) ,
    version int ,                           -- 版本？？？
    effective_date date ,
    expiry_date date)
clustered by (customer_sk) into 8 buckets   -- 按照主键进行分桶
stored as orc tblproperties('transactional'='true');

-- 产品维度表
create table product_dim (
    product_sk int ,
    product_code int ,
    product_name varchar(30) ,
    product_category varchar(30) ,
    version int ,
    effective_date date ,
    expiry_date date)
clustered by (product_sk) into 8 buckets   -- 按照主键进行分桶
stored as orc tblproperties('transactional'='true');

-- 订单维度表
create table order_dim (
    order_sk int ,
    order_number int ,
    version int ,
    effective_date date ,
    expiry_date date)
clustered by (order_sk) into 8 buckets
stored as orc tblproperties ('transactional'='true');

-- 销售订单事实表
create table sales_order_fact (
    order_sk int ,
    product_sk int ,
    customer_sk int ,
    order_date_sk int ,
    order_amount decimal(10, 2) )
clustered by (order_sk) into 8 buckets
stored as orc tblproperties('transactional'='true');
----------------------------------------------------------------------------

-- 预装载21年的日期维度 [使用shell脚本]


-- hive进行身份证验证
-- 思路：判断身份证号码的长度、省份代码、年与月份通过是否闰年2月匹配、校验位查询
select * 
from
    (select trim(upper(idcard)) idcard from t) t1
where
    length(idcard) <> 18
    or
    substr(idcard, 1, 2) not in  -- 省份代码不正确
    ('11','12','13','14','15','21','22','23','31', 
    '32','33','34','35','36','37','41','42','43', 
    '44','45','46','50','51','52','53','54','61', 
    '62','63','64','65','71','81','82','91')
    or
    -- undo
-----------------------------------------------------------
-- 去重
-- mysql 去重操作
select * from t t1
where t1.id=(select min(t2.id) from t t2
             where t1.name=t2.name and t1.address=t2.address);

-- hive
select * 
from 
  (select name, address, min(id)
   from t 
   group by name, address) t1

-- hive使用窗口函数
select 
from t1.id, t1.name, t1.address
  (select id, name, address
    row_number() over (distribute by name, address sort by id) as rn
  from t) t1
where t1.rn=1;

-- 单词统计
-- method 1
create table tt as
select word, count(1)
from doc lateral view explode(split(line, ' ')) words as word
group by word;
-- method 2
select words.word, count(1) as cnt
from
  (select explode(split(line, ' ') ) word from doc) as words
group by words.word;

----------------------------------------------------------------

 