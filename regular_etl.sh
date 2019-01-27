#!/bin/bash
# 整体拉取customer、product表数据
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
 # 执行增量导入
 sqoop job --exec myjob_incremental_import
 # 装载维度表和事实表
 beeline -u jdbc:hive2://bigdata4:10003/dw -f regular_etl.hql