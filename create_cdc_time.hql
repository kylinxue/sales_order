-- 记录上一次加载过程的起、止时间
use rds;
drop table if exists cdc_time ;
create table cdc_time(
    last_load date comment '上一次加载时间' ，
    current_load date comment '当前加载时间'  --etl开始时设置此值，结束是设置以上2个值
);

set hivevar:last_load=date_add(current_date(), -1);

-- 如下语句报错  Failed to recognize predicate '<EOF>'. 
             -- Failed rule: 'regularBody' in statement
insert overwrite table cdc_time 
select ${hivevar:last_load}, ${hivevar:last_load};
