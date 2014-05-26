#! /usr/bin/python
import sys
for line in sys.stdin:
	temp = line.strip().replace(", ",",")
	words = temp.split()
	if words[0]!='Data':
		for word in words[1:]:
			wordid,freq = word.split(":")
			print "LongValueSum:"+wordid+"\t1"

