===================================================================================
*                                                                                 *
*                              Shell files                                        *
*                                                                                 *
===================================================================================
- runrf.sh: run random forest hadoop program on distributed mode.

===================================================================================
*                                                                                 *
*                              Input files                                        *
*                                                                                 *
===================================================================================

- Required files:
In order to do 5-fold cross validation, we need the following input files:

r1.train, r1.test, r2.train, r2.test, r3.train, r3.test, r4.train, r4.test, r5.train, r5.test 

===================================================================================
*                                                                                 *
*                              Source files                                       *
*                                                                                 *
===================================================================================
- Controller.java:
Control mapreduce jobs to work in order and compute MaF, MaP, MaR for the test document set.

- InformationGainMap.java:
Given input training data, output the required data format for reducer.

- InformationGainReduce.java:
Compute information gain for each word in the training examples.

- TestRandomForestMap.java:
Given the trained random forest and test data, output the predicted label of current tree.

- TestRandomForestReduce.java:
Collect outputs from all decision trees for document and generate multi labels for one document.
