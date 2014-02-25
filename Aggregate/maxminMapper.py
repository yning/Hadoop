#! /usr/bin/env python26

import sys,re,string,os

max_count = 0
max_key = ""
min_count = 10000
min_key = ""

for line in sys.stdin:
	line = line.strip()
	(word, count_s) = line.split('\t')
	count = int(count_s)
	if count > max_count:
		max_count = count
		max_key = word
	if count < min_key:
		min_count = count
		min_key = word

print "%s\t%s" %(max_key, str(max_count))
print "%s\t%s" %(min_key, str(min_count))