#! /usr/bin/python26
import sys
num_class = 0
MaF = 0.0
MaP = 0.0
MaR = 0.0
for line in sys.stdin:
	temp = line.strip().split()
	num_class +=1
	MaP += float(temp[1])
	MaR += float(temp[2])
if num_class*(MaP+MaR)==0:
	print "num_class:{0},MaP:{1},MaR:{2}".format(num_class,MaP,MaR)
else:
	MaF = MaP*MaR*2/(num_class*(MaP+MaR))
	print MaF