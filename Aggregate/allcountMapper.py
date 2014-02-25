#! /usr/bin/env python

import sys,re,string,os

for line in sys.stdin:
	line = line.strip()
	fields = line.split('\t')
	(word,year) = fields[0].split(',')
	count = fields[1]

	print "LongValueSum:" + word + '\t' + str(count)

