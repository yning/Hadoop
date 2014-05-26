#! /bin/sh

hadoop jar /usr/share/hadoop/contrib/streaming/hadoop-streaming-1.2.1.jar \
 -input input/train \
 -output idf \
 -mapper idf.py \
 -file "idf.py" \
 -reducer aggregate
hadoop fs -getmerge idf/ idf.txt