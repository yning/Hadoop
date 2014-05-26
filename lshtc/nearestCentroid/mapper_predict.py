#! /usr/bin/python
import sys
import math
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
	maxValue = max(x)
	index = set()
	for i in range(len(x)):
		if x[i]>maxValue*threshold:
			index.add(labels[i])

	return index


#load inverted index
f_ii =open("invertedIndex.txt","r")
invertedIndex = dict()
for l in f_ii.readlines():
	word,labelstr = l.strip().split()
	labels = labelstr.split(",")
	labelset = list()
	for label in labels:
		labelset.append(int(label))
	invertedIndex[int(word)]=labelset
f_ii.close()

#load centroid
f_c = open("centroid.txt","r")
centroid = dict()
for l in f_c.readlines():
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

for line in sys.stdin:
	temp = line.strip().replace(", ",",")
	words = temp.split()
	if words[0]!='Data':
		candidateLabelSet = set()
		#for word find candidate labelset
		test = dict()
		for w in words[1:]:
			word,freq = w.split(":")
			if int(word) in invertedIndex:
				candidateLabelSet.update(invertedIndex[int(word)])
				test[int(word)]=float(freq)
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
		truth_lables = set([int(x) for x in words[0].split(",")])
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
			print str(l)+("\t{}\t{}\t{}".format(tp,fp,fn))






