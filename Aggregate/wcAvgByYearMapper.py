#! /usr/bin/env python

import sys,re,string,os
from os import listdir
from os.path import isfile, join

yearfile = open('yearcount','r')
yearcount = list()

for stw in yearfile:
	stw = stw.strip()
	temp = list()
	fields = stw.split('\t')
	temp.append(fields[0])
	temp.append(fields[1])
	yearcount.append(temp)

for line in sys.stdin:#filename in sys.stdin:
	line = line.strip()
	fields = line.split('\t')
	(word, year) = fields[0].split(',')
	count = fields[1]
	for item in yearcount:
		if year == item[0]:
			print word + ',' + year + '\t' + str(float(count)/float(item[1]))
