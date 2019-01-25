#!/bin/bash
# example: ./date_dim_generate.sh 2018-10-25 2018-12-10
date1="$1"
date2="$2"
tempdate=`date -d "$date1" +%F`
tempdateSec=`date -d "$date1" +%s`
enddateSec=`date -d "$date2" +%s`
min=1
max=`expr \( $enddateSec - $tempdateSec \) / \( 24 \* 60 \* 60 \) + 1`

# cat /dev/null > ./date_dim.csv   #清空date_dim.csv

while [ $min -le $max ]
do
    month=`date -d "$tempdate" +%m`
    month_name=`date -d "$tempdate" +%B`
    quarter=`echo $month | awk '{print int(($0-1)/3)+1}'`   # $0是awk的参数，代表echo出的所有字符串，不是shell的$0参数
    year=`date -d "$tempdate" +%Y`
    echo ${min}","${tempdate}","${month}","${month_name}","${quarter}","${year}  # >> ./date_dim.csv
    tempdate=`date -d "+$min day $date1" +%F`
    tempdateSec=`date -d "+$min day $date1" +%s`
    min=`expr $min + 1`
done

hdfs dfs -put -f date_dim.csv /user/4800613/hive/warehouse/dw.db/date_dim/