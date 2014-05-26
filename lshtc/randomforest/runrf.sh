javac -classpath /usr/share/hadoop/hadoop-core-1.2.1.jar -d CLASSES3 SRC3/*.java
jar -cvf SRC3/Controller.jar -C CLASSES3/ .


time hadoop jar SRC3/Controller.jar Controller -D train=input/newtrain.txt\
	-D test=input/r1new.test\
	-D labelset=input/newlabels.txt -D dictionary=input/newdict.txt\
	-D output=output2/dir [-D reducers=10]
