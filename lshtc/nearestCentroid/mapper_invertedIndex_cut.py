#! /usr/bin/python26
import sys
for line in sys.stdin:
	label,words = line.strip().split()
	wordlist = words.split(";")
	for word in wordlist:
		wordid = word.split(":")[0]
		print wordid+"\t"+label

