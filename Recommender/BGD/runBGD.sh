javac -classpath /usr/share/hadoop/hadoop-core-1.2.1.jar -d CLASSES SRC/recommenderGD.java
jar -cvf SRC/recommenderGD.jar -C CLASSES/ .

hadoop fs -rmr rmseGD-r*
hadoop fs -rmr outputGD-r*
hadoop fs -rmr testGD-r*
hadoop fs -rmr matrixGD-r*

time hadoop jar SRC/recommenderGD.jar recommenderGD input/r1.train input/r1.test\
	20 5 71567 65133 8000044 2000010  outputGD-r1 rmseGD-r1 matrixGD-r1 testSGD-r1 

time hadoop jar SRC/recommenderGD.jar recommenderGD input/r2.train input/r2.test\
	20 5 71567 65133 8000044 2000010  outputGD-r2 rmseGD-r2 matrixGD-r2 testSGD-r2

time hadoop jar SRC/recommenderGD.jar recommenderGD input/r3.train input/r3.test\
20 5 71567 65133 8000044 2000010  outputGD-r3 rmseGD-r3 matrixGD-r3 testSGD-r3 

time hadoop jar SRC/recommenderGD.jar recommenderGD input/r4.train input/r4.test\
	20 5 71567 65133 8000044 2000010  outputGD-r4 rmseGD-r4 matrixGD-r4 testSGD-r4 

time hadoop jar SRC/recommenderGD.jar recommenderGD input/r5.train input/r5.test\
	20 5 71567 65133 8000040 2000014  outputGD-r5 rmseGD-r5 matrixGD-r5 testSGD-r5 