#!/bin/bash
# 初始装载在开始使用数据仓库前执行一次
----------------------------------------------------------------------------------
# 建立Sqoop增量导入作业，以order_number作为检查列，初始last-value是0
sqoop job --delete myjob_incremental_import
sqoop job --create myjob_incremental_import \
-- \
import \
--connect "jdbc:mysql://10.173.32.6:3306/source?characterEncoding=UTF-8" \
--username root --password Gg/ru,.#5 \
--table sales_order \
--columns "order_number, customer_number, product_code, order_date, entry_date, order_amount" \
--hive-import \
--hive-table rds.sales_order \
--incremental append \
--check-column order_number \
--last-value 0
# 首次抽取，将全部数据导入RDS库
sqoop import --connect "jdbc:mysql://10.173.32.6:3306/source?characterEncoding=UTF-8" \
--username root --password Gg/ru,.#5 \
--table customer \
--hive-import --hive-overwrite \
--hive-table rds.customer 
sqoop import --connect "jdbc:mysql://10.173.32.6:3306/source?characterEncoding=UTF-8" \
--username root --password Gg/ru,.#5 \
--table product \
--hive-import --hive-overwrite \
--hive-table rds.product
----------------------------------------------------------------------------------
# 清空sales_order表,实现幂等操作
beeline -u jdbc:hive2://bigdata4:10003/dw -e "TRUNCATE TABLE rds.sales_order"
# 执行增量导入，因为是初次导入，所以此次导入全部数据
sqoop job --exec myjob_incremental_import

beeline -u jdbc:hive2://bigdata4:10003/dw -f init_etl.hql


