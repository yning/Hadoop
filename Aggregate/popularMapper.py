#! /usr/bin/env python

import sys,re,string,os

for line in sys.stdin:
	line = line.strip()
	(key, count) = line.split('\t')
	print "%s\t%s" %(count, key)