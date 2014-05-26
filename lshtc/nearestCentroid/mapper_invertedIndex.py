#! /usr/bin/python
import sys
for line in sys.stdin:
	temp = line.strip().replace(", ",",")
	words = temp.split()
	if words[0]!='Data':
		for word in words[1:]:
			word_id = word.split(":")[0]
			print word_id+"\t"+words[0]

