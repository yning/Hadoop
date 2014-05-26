#! /usr/bin/python
import sys
f = open("ignoredLabel.txt","r")
s = set()
for l in f:
	s.add(l.strip())

for line in sys.stdin:
	temp = line.strip().replace(", ",",")
	words = temp.split()
	if words[0]!='Data':
		labels = words[0].split(',')
		maxCount = 0;
		for word in words[1:]:
			wordid,freq = word.split(":")
			if int(freq)>maxCount:
				maxCount = int(freq)
		wordstr = ""
		for word in words[1:]:
			wordid,freq = word.split(":")
			augFreq = 0.5 + 0.5*int(freq)/maxCount
			wordstr +=wordid+(":%.4f;" %augFreq)
		for label in labels:
			if label not in s:
				print label+'\t'+wordstr.strip(";")

