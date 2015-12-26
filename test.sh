#!/usr/bin/env bash
echo "start"
date
while true
do
    date
    time curl -XPOST localhost:11003/key -d '{"_test_": "_value_"}'
    datasize=`du  -s /Users/zhanghu/node0 | cut -f1`
    echo "data size: $datasize blocks"
    if [ $datasize -gt 1000000 ]
    then
        echo "end"
        date
        exit 1
    fi

    sleep 300
done
