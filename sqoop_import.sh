
# 全量抽取mysql中的source.customer --> rds.customer
sqoop import --connect "jdbc:mysql://10.173.32.6:3306/source?characterEncoding=UTF-8" \
 --username root --password Gg/ru,.#5 \
--table customer \
--hive-import --hive-overwrite \
--hive-table rds.customer

# 全量抽取mysql中的source.product --> rds.product
sqoop import --connect "jdbc:mysql://10.173.32.6:3306/source?characterEncoding=UTF-8" \
 --username root --password Gg/ru,.#5 \
 --table product \
 --hive-import --hive-overwrite \
 --hive-table rds.product

 # 增量导入
 # step1：建立增量导入作业
 sqoop job --create myjob_1 \
 -- \
 import \
 --connect "jdbc:mysql://10.173.32.6:3306/source?characterEncoding=UTF-8&user=root&password=Gg/ru,.#5" \
 --table sales_order \
 --columns "order_number, customer_number, product_code, order_date, entry_date, order_amount" \
 --where "entry_date < current_date()" \
 --hive-import \
 --hive-table rds.sales_order \
 --incremental append \
 --check-column entry_date \
 --last-value '1900-01-01'

 # step2：查看作业中保存的last-value

 # step3：执行作业
