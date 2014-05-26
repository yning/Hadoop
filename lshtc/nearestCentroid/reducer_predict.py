#! /usr/bin/python26
import sys
last_label = ""
tp = 0
fp = 0
fn = 0
MaP = 0.0
MaR = 0.0
for line in sys.stdin:
	temp = line.strip().split()
	if last_label=="":
		last_label = temp[0]

	if last_label !=temp[0]:
		if tp != 0:
			MaP = float(tp)/(tp+fp)
			MaR = float(tp)/(tp+fn)
		print last_label+"\t{0}\t{1}".format(MaP,MaR)
		tp=0
		fp=0
		fn=0
		MaP=0.0
		MaR=0.0
		last_label=temp[0]
	tp+=int(temp[1])
	fp+=int(temp[2])
	fn+=int(temp[3])
if tp != 0:
	MaP = float(tp)/(tp+fp)
	MaR = float(tp)/(tp+fn)
print last_label+"\t{0}\t{1}".format(MaP,MaR)
