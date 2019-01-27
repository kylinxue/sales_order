-- 初始装载在开始使用数据仓库前执行一次
use dw;
-- 清空维度表 一天内多次执行init_etl.hql，仅保留最后一天内最后的数据版本
truncate table customer_dim;
truncate table product_dim;
truncate table order_dim;
truncate table sales_order_fact;
-- 装载 客户维度表
insert into customer_dim
select
  row_number() over (order by t1.customer_number)+t2.sk_max ,
  t1.customer_number, t1.customer_name, t1.customer_street_address, 
  t1.customer_zip_code, t1.customer_city, t1.customer_state,
  1, '2016-03-01', '2200-01-01'   -- version | effective_date | expiry_date
from rds.customer t1
cross join (select coalesce(max(customer_sk), 0) sk_max from customer_dim) t2;
-- 
insert into product_dim
select
  fow_number() over (order by t1.product_code)+t2.sk_max,
  product_code, product_name, product_category,
  1, '2016-03-01', '2200-01-01'
from rds.product t1
cross join (select coalesce(max(product_sk), 0) sk_mak from product_dim) t2;
-- 
insert into order_dim
select
  row_number() over (order by t1.order_number)+t2.sk_max,
  order_number, 
  1, '2016-03-01', '2200-01-01'
from rds.sales_order t1
cross join (select coalesce(max(order_sk), 0) sk_max from order_dim) t2;
-- 装载销售订单事实表
insert into sales_order_fact
select
  order_sk, customer_sk, product_sk, date_sk, order_amount
from
 rds.sales_order a, order_dim b, customer_dim c, product_dim d, date_dim effective_date
where a.order_number = b.order_number
and a.customer_number=c.customer_number
and a.product_code=d.product_code
and to_date(a.order_date)=e.date;