
hadoop jar /usr/share/hadoop/contrib/streaming/hadoop-streaming-1.2.1.jar -files stopwords.txt -input 'data/' -output 'test-count' -file SRC/wcByYearMapper.py -file SRC/wcByYearReducer.py -mapper 'wcByYearMapper.py' -reducer 'wcByYearReducer.py'

hadoop fs -rm test-count/_SUCCESS

mkdir test-count

hadoop fs -get test-count/* test-count

hadoop jar /usr/share/hadoop/contrib/streaming/hadoop-streaming-1.2.1.jar -files yearcount -input 'test-count/' -output 'test-avg' -file SRC/wcAvgByYearMapper.py -mapper 'wcAvgByYearMapper.py'

mkdir test-avg

hadoop fs -get test-avg/* test-avg

hadoop jar /usr/share/hadoop/contrib/streaming/hadoop-streaming-1.2.1.jar -input 'test-count/' -output 'test-allcount' -file SRC/allcountMapper.py -mapper 'allcountMapper.py' -reducer aggregate

mkdir test-allcount

hadoop fs -get test-allcount/* test-allcount

rm -r test-allcount/_SUCCESS

hadoop jar /usr/share/hadoop/contrib/streaming/hadoop-streaming-1.2.1.jar -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator -D mapred.text.key.comparator.options=-n -input test-allcount/ -output test-sort -file SRC/popularMapper.py -mapper 'popularMapper.py'

mkdir test-sort

hadoop fs -get test-sort/* test-sort

hadoop jar /usr/share/hadoop/contrib/streaming/hadoop-streaming-1.2.1.jar -D mapred.reduce.tasks=1 -input 'test-allcount/' -output 'test-maxmin' -file SRC/maxminMapper.py  -mapper 'maxminMapper.py'

mkdir test-max

hadoop fs -get test-max/* test-max

hadoop fs -rmr test-count

hadoop fs -rmr test-allcount

hadoop fs -rmr test-avg

hadoop fs -rmr test-sort

hadoop fs -rmr test-max


