#! /bin/sh

hadoop fs -rmr output3
rm result.txt

hadoop jar /usr/share/hadoop/contrib/streaming/hadoop-streaming-1.2.1.jar \
 -input input/test \
 -output output3 \
 -mapper mapper_predict.py \
 -file "mapper_predict.py" \
 -reducer reducer_predict.py \
 -file "reducer_predict.py" \
 -cacheFile 'hdfs://head:8020/user/xguan/centroid.txt#centroid.txt' \
 -cacheFile 'hdfs://head:8020/user/xguan/centroid.txt#invertedIndex.txt'