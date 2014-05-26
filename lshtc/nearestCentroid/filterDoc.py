#! /usr/bin/python26
import sys

f = open('ignoredLabel.txt','r')
ignoredLabel = set()
for l in f:
	ignoredLabel.add(l)

for line in sys.stdin:
	temp = line.strip().replace(", ",",")
	words = temp.split()
	if words[0]!='Data':
		labels = words[0].split(',')
		flag = False
		for label in labels:
			if label not in ignoredLabel:
				flag =True
		if flag:
			print line.strip()

