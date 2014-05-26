#! /bin/sh
#clean previous result
hadoop fs -rmr output1
hadoop fs -rmr output2
hadoop fs -rmr output3
rm result.txt
rm centroid.txt
rm invertedIndex.txt

#centroid
hadoop jar /usr/share/hadoop/contrib/streaming/hadoop-streaming-1.2.1.jar \
 -input input/train \
 -output output1 \
 -mapper mapper_centroid.py \
 -file "mapper_centroid.py" \
 -reducer reducer_centroid.py \
 -file "reducer_centroid.py" \
 -combiner combiner_centroid.py \
 -file "combiner_centroid.py"

hadoop fs -getmerge output1/ centroid.txt
hadoop fs -copyFromLocal centroid.txt .

#invertedIndex
hadoop jar /usr/share/hadoop/contrib/streaming/hadoop-streaming-1.2.1.jar \
 -input input/train \
 -output output2 \
 -mapper mapper_invertedIndex.py \
 -file "mapper_invertedIndex.py" \
 -reducer reducer_invertedIndex.py \
 -file "reducer_invertedIndex.py" \
 -combiner reducer_invertedIndex.py

hadoop fs -getmerge output2/ invertedIndex.txt
hadoop fs -copyFromLocal invertedIndex.txt .


hadoop jar /usr/share/hadoop/contrib/streaming/hadoop-streaming-1.2.1.jar \
 -input input/test \
 -output output3 \
 -mapper mapper_predict.py \
 -file "mapper_predict.py" \
 -reducer reducer_predict.py \
 -file "reducer_predict.py" \
 -cacheFile 'hdfs://head:8020/user/xguan/centroid.txt#centroid.txt' \
 -cacheFile 'hdfs://head:8020/user/xguan/centroid.txt#invertedIndex.txt'

