#! /usr/bin/python26
import sys
import math
N = math.log(1596613)
def cosine(x,y):
	#x,y are dict
	sim = 0.0
	abs_x = 0.0
	abs_y = 0.0
	for key,value in x.items():
		if key in y:
			sim+= value*y[key]
		abs_x +=value*value
	for key,value in y.items():
		abs_y+=value*value
	sim = sim/math.sqrt(abs_y*abs_x)
	return sim

def findLabel(x,labels,threshold):
	maxValue = max(x)
	index = list()
	for i in range(len(x)):
		if x[i]>maxValue*threshold:
			index.append(labels[i])
	label = ""
	for i in index:
		label+=str(i)+","

	return label.strip(",")

def findLabelSet(x,labels,threshold):
	if len(x)==0:
		index = set()
		index.add(0)
		return index
	maxValue = max(x)
	index = set()
	for i in range(len(x)):
		if x[i]>maxValue*threshold:
			index.add(labels[i])

	return index


#load inverted index
f_ii =open("invertedIndex.txt","r")
invertedIndex = dict()
for l in f_ii:
	word,labelstr = l.strip().split()
	labels = labelstr.split(",")
	labelset = list()
	for label in labels:
		labelset.append(int(label))
	invertedIndex[int(word)]=labelset
f_ii.close()

#load centroid
f_c = open("centroid_top10.txt","r")
centroid = dict()
for l in f_c:
	label,freqstr = l.strip().split()
	freqs = freqstr.split(";")
	freqset = dict()
	for f in freqs:
		word,freq = f.split(":")
		wordid =int(word)
		freq = float(freq)
		freqset[wordid]=freq
	centroid[int(label)]=freqset
f_c.close()

f_i = open("ignoredLabel.txt","r")
ignoredLabel=set()
for l in f_i:
	ignoredLabel.add(int(l.strip()))
f_i.close()

f_idf = open("idf_cut.txt","r")
idf = dict()
for l in f_idf:
	wordid,count = l.strip().split()
	idf[int(wordid)]=int(count)
f_idf.close()

for line in sys.stdin:
	temp_line = line.strip().replace(", ",",")
	temp = temp_line.split()
	labels =temp[0]
	words=temp[1:]
	candidateLabelSet = set()
	#for word find candidate labelset
	test = dict()
	for w in words:
		word,freq = w.split(":")
		if int(word) in invertedIndex:
			candidateLabelSet.update(invertedIndex[int(word)])
			tfidf = float(freq)*(N-math.log(idf[int(word)]))			
			test[int(word)]=tfidf
	#for centroid in candidate labelset
	sims = list()
	candidateLabelSet = list(candidateLabelSet)
	for l in candidateLabelSet:
		#calcuate cosine
		cent = centroid[l]
	 	sims.append(cosine(cent,test))
	#find max, find max*0.6
	predict_labels = findLabelSet(sims,candidateLabelSet,0.6)
	#print words[0]+"\t"+str(predict_labels)
	truth_lables = set([int(x) for x in labels.split(",") if int(x) not in ignoredLabel])
	#print str(truth_lables)+"\t"+str(predict_labels)
	#tp
	tp_set =predict_labels.intersection(truth_lables)
	#fp
	fp_set=predict_labels.difference(truth_lables)
	#fn
	fn_set=truth_lables.difference(predict_labels)
	tp = 0
	fp = 0
	fn = 0
	for l in predict_labels.union(truth_lables):
		if l in tp_set:
			tp =1
		if l in fp_set:
			fp =1
		if l in fn_set:
			fn = 1
		print str(l)+("\t{0}\t{1}\t{2}".format(tp,fp,fn))
		tp = 0
		fp = 0
		fn = 0






