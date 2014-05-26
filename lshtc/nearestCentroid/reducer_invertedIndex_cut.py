#! /usr/bin/python26
import sys
last_word =""
labelset=set()
for line in sys.stdin:
	word,label_in= line.strip().split()
	if last_word=="":
		last_word = word
	if last_word!=word:
		labelstr = ""
		for label in labelset:
			labelstr+=str(label)+","
		print last_word+"\t"+labelstr.strip(",")
		labelset.clear()
		last_word = word
	if int(label_in) not in labelset:
		labelset.add(int(label_in))
	
labelstr = ""
for label in labelset:
	labelstr+=str(label)+","
print last_word+"\t"+labelstr.strip(",")
labelset.clear()
