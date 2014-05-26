
javac -classpath /usr/share/hadoop/hadoop-core-1.2.1.jar -d CLASSES SRC/*.java
jar -cvf SRC/mnbController.jar -C CLASSES/ .


time hadoop jar SRC/mnbController.jar mnbController -D train=input/train.csv -D test=input/smalltest\
	-D output=output/dir [-D reducers=10]
