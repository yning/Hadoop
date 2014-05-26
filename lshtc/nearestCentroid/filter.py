#! /usr/bin/python
import sys
for line in sys.stdin:
	label,count = line.strip().split()
	if int(count)<6:
		print label

