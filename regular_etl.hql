
-- 设置变量支持事务
set hive.support.concurrentcy=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.    -- undo
set hive.compactor.initiator.on=true;
set hive.compactor.worker.threads=1;

-- 设置缓慢变化维 scd的生效时间和过期时间
-- scd2重新产生一条变化的记录，原记录的过期时间需要修改
set hivevar:cur_date=current_date();
set hivevar:pre_date=date_add(${hivevar:cur_date}, -1);
set hivevar:max_date=cast('2200-01-01' as date);

-- 设置cdc的last_load不变【为上一次加载的日期】，current_load为当前日期
insert overwrite table rds.cdc_time
select last_load, ${hivevar:cur_date} from rds.cdc_time;

-- 从之前没有过期的记录中筛选当前这一次过期的记录，并修改过期日期
update customer_dim
set expiry_date=${hivevar:pre_date}
where customer_dim.customer_sk in
(select a.customer_sk
 from 
   (select customer_sk, customer_number, customer_street_address
    from customer_dim 
    where expiry_date=${hivevar:max_date}) a    -- 选出之前没有过期的记录
left join rds.customer b
on a.customer_number=b.customer_number
where b.customer_number is null      -- 源数据已经删除但维度表还存在
   or a.customer_street_address<>b.customer_street_address);  -- 过滤出源数据修改了地址信息的记录

-- 处理customer_street_addresses列上scd2的新增行
insert into customer_dim
select 
    row_number() over (order by t1.customer_number)+t2.sk_max,
    t1.customer_number, 
    t1.customer_name,
    t1.customer_street_address,
    t1.customer_zip_code,
    t1.customer_city,
    t1.customer_state,
    t1.version,
    t1.effective_date,
    t1.expiry_date
from
(
  select 
    t2.customer_number customer_number,
    t2.customer_name customer_name,
    t2.customer_street_address customer_street_address,
    t2.customer_zip_code,
    t2.customer_city,
    t2.customer_state,
    t1.version+1 version,
    ${hivevar:pre_date} effective_date,
    ${hivevar:max_date} expiry_date
  from customer_dim t1
  inner join rds.customer t2
  on t1.customer_number=t2.customer_number
  and t1.expiry_date=${hivevar:pre_date}
  left join customer_dim t3
  on t1.customer_number=t3.customer_number
  and t3.expiry_date=${hivevar:max_date}
  where t1.customer_street_address<>t2.customer_street_address 
        and t3.customer_sk is null
) t1   
cross join
(select coalesce(max(customer_sk), 0) sk_max from customer_dim) t2;

-- 处理customer_name列上的scd1 【scd1不保存历史记录，原纪录上直接更新】
drop table if exists tmp;
create table tmp as
select a.customer_sk, a.customer_number, b.customer_name,
       a.customer_street_address, a.customer_zip_code, a.customer_city,
       a.customer_state, a.version, a.effective_date, a.expiry_date
from customer_dim a, rds.customer b
-- 过滤出原表中的用户名修改的记录
where a.customer_number=b.customer_number and (a.customer_name <> b.customer_name);