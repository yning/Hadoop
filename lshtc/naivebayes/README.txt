===================================================================================
*                                                                                 *
*                              Shell files                                        *
*                                                                                 *
===================================================================================
runmnb.sh:
run multinormial naive bayes algorithm on map reduce in distributed mode.

===================================================================================
*                                                                                 *
*                              Input files                                        *
*                                                                                 *
===================================================================================

Required files:
In order to do 5-fold cross validation, we need the following input files:

r1.train, r1.test, r2.train, r2.test, r3.train, r3.test, r4.train, r4.test, r5.train, r5.test 

===================================================================================
*                                                                                 *
*                              Source files                                       *
*                                                                                 *
===================================================================================
mnbController.java:
Control mapreduce jobs to work in order and compute MaF, MaP, MaR for the test document set.

MNBwordcountMap.java, MNBwordcountReduce.java:
Count frequence of words and labels.

ComputePriorMap.java, ComputePriorReduce.java
Given the input data, compute prior probabilities.

TestDataMapper.java, TestModelMapper.java, TestReduce.java:
Join the trained model with test data.

ClassifyMap.java, ClassifyReduce.java:
Classify test documents given the joined data.
