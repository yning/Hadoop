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

for line in sys.stdin:#filename in sys.stdin:
#	base = os.path.basename(onefile)
	#file_path = dir_path + filename.strip()
	#rfile = open(file_path,'r')
	#cat = subprocess.Popen(["hadoop","fs","-cat",file_path],stdout=subprocess.PIPE)
	#allstring =rfile.read()
	#allstring = re.sub(r'<[^>]*>','',allstring)
	#year = filename[0:4]
#	filename = os.path.splittext(base)[0][0:3]
	#for line in rfile.readlines():
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
				print year +"\t" + "1"
