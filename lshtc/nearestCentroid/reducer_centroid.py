#! /usr/bin/python
import sys
import math
N = math.log(1596613)
f = open("idf.txt","r")
s = dict()
for l in f:
	wordid,count = l.strip().split()
	s[int(wordid)]=int(count)

words_list = dict()
doc_count  = 0
last_label = ''
for line in sys.stdin:
	label,wordstr_in = line.strip().split()
	if last_label=='':
		last_label = label
	
	if last_label!=label:
		wordstr_out = ""
		for key,value in words_list.items():
			wordstr_out+=str(key)+(":%.4f;" % ((value/doc_count)*(N-math.log(s[key])))
		print last_label+"\t"+wordstr_out.strip(";")
		doc_count = 0
		words_list.clear()
		last_label=label
	words = wordstr_in.split(";")
	doc_count+=1
	for word in words:
		wordid,freq = word.split(":")
		wordid = int(wordid)
		freq = float(freq)
		if wordid not in words_list:
			words_list[wordid]=freq
		else:
			words_list[wordid]+=freq
#last one
wordstr_out = ""
for key,value in words_list.items():
	wordstr_out+=str(key)+(":%.4f;" % ((value/doc_count)*(N-math.log(s[key])))
print last_label+"\t"+wordstr_out.strip(";")
