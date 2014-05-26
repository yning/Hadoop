#! /usr/bin/python26
import sys
import math
f = open("idf.txt","r")
s = dict()
for l in f:
	wordid,count = l.strip().split()
	s[int(wordid)]=int(count)

doc_count  = 0
last_label = ''
for line in sys.stdin:
	label,wordstr_in = line.strip().split()
	wordlist = wordstr_in.split(";")
	wordstr_out = ""
	for word in wordlist:
		for key,value in word.split(":"):
			wordstr_out+=key+(":%.4f;" % (float(value)-(float(value)/N)*math.log(s[int(key)])))
		print last_label+"\t"+wordstr_out.strip(";")