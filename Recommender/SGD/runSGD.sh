javac -classpath /usr/share/hadoop/hadoop-core-1.2.1.jar -d CLASSES SRC/recommenderSGD.java
jar -cvf SRC/recommenderSGD.jar -C CLASSES/ .

hadoop fs -rmr rmseSGD-r*
hadoop fs -rmr outputSGD-r*
hadoop fs -rmr testSGD-r*
hadoop fs -rmr matrixSGD-r*

time hadoop jar SRC/recommenderSGD.jar recommenderSGD input/r1.train input/r1.test\
	20 5 71567 65133 8000044 2000010 outputSGD-r1 rmseSGD-r1 matrixSGD-r1 testSGD-r1 

time hadoop jar SRC/recommenderSGD.jar recommenderSGD input/r2.train input/r2.test\
	20 5 71567 65133 8000044 2000010 outputSGD-r2 rmseSGD-r2 matrixSGD-r2 testSGD-r2 

time hadoop jar SRC/recommenderSGD.jar recommenderSGD input/r3.train input/r3.test\
	20 5 71567 65133 8000044 2000010 outputSGD-r3 rmseSGD-r3 matrixSGD-r3 testSGD-r3 

time hadoop jar SRC/recommenderSGD.jar recommenderSGD input/r4.train input/r4.test\
	20 5 71567 65133 8000044 2000010 outputSGD-r4 rmseSGD-r4 matrixSGD-r4 testSGD-r4 

time hadoop jar SRC/recommenderSGD.jar recommenderSGD input/r5.train input/r5.test\
	20 5 71567 65133 8000040 2000014 outputSGD-r5 rmseSGD-r5 matrixSGD-r5 testSGD-r5 
