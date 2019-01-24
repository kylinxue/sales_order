-- 建立源数据库
drop database if exists source;
create database source;

use source;
--建立客户表
create tabble customer(
    customer_number int not null auto_increment primary key comment '客户标号，主键' ,
    customer_name   varchar(50) ,
    customer_street_address varchar(50) ,
    customer_zip_code int ,
    customer_city varchar(30) ,
    customer_state varchar(2) comment '所在省份'
);
--建立产品表
create table product(
    product_code int not null auto_increment primary key ,
    product_name varchar(30) ,
    product_category varchar(30) comment '产品类型'
);
--建立销售订单表
create table sales_order(
    order_number int not null auto_increment primary key ,
    customer_number int ,
    product_code int ,
    order_date datetime ,
    entry_date datetime ,
    order_amount decimal(10, 2) ,
    foreign key (customer_number) references customer (customer_number) on delete cascade on update cascade,
    foreign key (product_code) references product (product_code) on delete cascade on update cascade 
);

-- 向客户表添加数据 自增键不需要插入
insert into customer (customer_name, customer_street_address, customer_zip_code, customer_city, customer_state) 
values 
('really large customers', '7500 louise dr.', 17050, 'mechanicsburg','pa'), 
('small stores', '2500 woodland st.', 17055, 'pittsburgh','pa'), 
('medium retailers',' 1111 ritter rd.', 17055,' pittsburgh','pa'), 
('good companies',' 9500 scott st.', 17050,' mechanicsburg','pa'), 
('wonderful shops',' 3333 rossmoyne rd.', 17050,' mechanicsburg','pa'), 
('loyal clients',' 7070 ritter rd.', 17055,' pittsburgh','pa'), 
('distinguished partners',' 9999 scott st.', 17050,' mechanicsburg','pa');

-- 向产品表添加数据
insert into product (product_name, product_category)
values
('hard disk drive', 'storage'), 
('floppy drive', 'storage'), 
('lcd panel', 'monitor');

-- 生成100条销售订单表测试数据
drop procedure if exists generate_sales_order_data;
delimiter //
create procedure generate_sales_order_data()
begin
    drop table if exists temp_sales_order_data;
    create table temp_sales_order_data as select * from sales_order where 1=0;

    set @start_date := unix_timestamp('2016-03-01');
    set @end_date := unix_timestamp('2016-07-01');
    set @i :=1;

    while @i<=100 do
        set @customer_number := floor(1+rand()*6);
        set @product_code := floor(1+rand()*2);
        set @order_date := from_unixtime(@start_date + rand() *(@end_date - @start_date));
        set @amount := floor(1000 + rand()*9000);
        insert into temp_sales_order_data values(@i, @customer_number, @product_code, @order_date, @order_date, @amount);
        set @i := @i+1;
    end while;

    truncate table sales_order;
    -- 保证sales_order表中的自增键顺序和订单日期顺序一致
    insert into sales_order
    select null, customer_number, product_code, order_date, entry_date, order_amount
    from temp_sales_order_data order by order_date;
    commit;
end
//
delimiter ;

call generate_sales_order_data();
