#! /usr/bin/python26
import sys

f = open("invertedIndex.txt","r")
wordlist = set()
for l in f:
	wordid = l.strip().split()[0]
	wordlist.add(int(wordid))

for line in sys.stdin:
	wordid,idf = line.strip().split()
	if int(wordid) in wordlist:
		print line.strip()