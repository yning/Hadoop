##########################################
#
#README
#Nearest Centroid Map reduce
#Xin Guan and Yue Ning CS757 project
##########################################

We use map reduce frame work and streaming method, implement the 
Nearest Centroid classifier for multilabel documents classification.
idf.py:
	get the idf count for each word

mapper_invertedIndex.py reducer_invertedIndex.py
	get inverted index, reduce the computation

tfidf:
	change the presentation to tfidf

mapper_centroid.py combiner_centroid.py reducer_centroid.py 
	get the centroids

We find the centroids cannot load to nodes, so we need to reduce
the file size. The files with '_cut' are used for the reduced nearest
centroids method. We only keep the top 10 words in each centroid,
and use the centroid file to get the inverted index.

run.sh is training part

runTest.sh is predict part