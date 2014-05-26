import sys,re,string
from os import listdir
from os.path import isfile, join
from collections import Counter

filein = open("data/train.csv","r")#sys.argv[1]
fileout = open("labelset.txt","a")	
num = 0
alllabels =[]
for line in filein:
	if "Data" not in line:
		newline = line.strip()
		data = re.split(' |, ', newline)
		for idx in data:
			wordcount = list()
			if ":" not in idx:
				num+=1
				#print ('%d' %int(wordcount[0]))
				alllabels.append(int(idx))

'''print 'length : %d' % num
allwords = [0]*num
j = 0
for line in filein:
	if "Data" not in line:
		newline = line.strip()
		data = re.split(' |, ', newline)
		for idx in data:
			wordcount = list()
			if ":" in idx:
				wordcount = idx.split(':')
				allwords[j] = int(wordcount[0])
				j+=1
'''
c = Counter(alllabels)
i = 0
print 'dict length: %d'%len(c)
for item in c:
	if i == len(c)-1:
		fileout.write(str(item))
	else:
		fileout.write(str(item)+",")
		i+=1
