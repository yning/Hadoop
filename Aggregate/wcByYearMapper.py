#! /usr/bin/env python

import sys,re,string,os
from os import listdir
from os.path import isfile, join

filename = os.environ['map_input_file']
parts = filename.split('/')
year = parts[len(parts)-1].split('.')[0][0:4]
stopfile = open('stopwords.txt','r')
stopwords = list()
for stw in stopfile:
	stw = stw.strip()
	stopwords.append(stw)

for line in sys.stdin:
	line = re.sub(r'<[^>]*>','',line)
	line = re.sub(r'<[^>]*','',line)
	line = re.sub(r'[^>]*>','',line)
	line = re.sub(r'[0-9]*','',line)
	line = line.translate(string.maketrans("",""), string.punctuation)
	fields = line.split()
	if len(fields) >0:
		for word in fields:
			word = word.lower()
			if word not in stopwords:
				print word.strip() + "," + year +"\t" + "1"
