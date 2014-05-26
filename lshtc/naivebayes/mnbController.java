/* ************************ *
*         Yue Ning          *
*     Partner: Xin Guan     *
*          CS 757           *
*      Course Project       *
* ************************* */
import java.io.IOException;
import java.util.*;
import java.util.Random;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.lang.*;
import java.io.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.System;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.DataInput;   
import java.io.DataInputStream; 
import java.io.InputStreamReader; 
import java.io.DataOutput;  
//import java.nio.channels.FileChannel;
import java.lang.Math;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.*;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.jobcontrol.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class mnbController extends Configured implements Tool
{
    public static void delete(Configuration conf, Path path) throws IOException{
        FileSystem fs = path.getFileSystem(conf);
        if(fs.exists(path)){
            fs.delete(path, true);
        }
    }
    @Override
    public int run(String[] args) throws Exception 
    {  
        try {
            //Configuration testconf = new Configuration();
            Configuration jobconf = getConf();
            Path inputTrainPath = new Path(jobconf.get("train"));
            Path inputTestPath = new Path(jobconf.get("test"));
            Path output = new Path(jobconf.get("output"));

            //Path labelset = new Path(jobconf.get("labelset"));
            Path wcoutput = new Path(output.getParent(), "count");
            Path prioroutput = new Path(output.getParent(), "prior");
            Path model = new Path(output.getParent(), "model");
            Path joined = new Path(output.getParent(),"joined");

            //Path testjoinPath = new Path(output.getParent(), "testjoin");
            Path classifyoutput = new Path(output.getParent(), "classify");
            Path evalPath = new Path(output.getParent(), "eval");

            mnbController.delete(jobconf, wcoutput);
            Job wcjob = new Job(jobconf, "MNBwordcount");
            wcjob.setJarByClass(mnbController.class);

            wcjob.setMapperClass(MNBwordcountMap.class);
            //job.setCombinerClass(MNBwordcountCombine.class);
            wcjob.setReducerClass(MNBwordcountReduce.class);

            wcjob.setMapOutputKeyClass(Text.class);
            wcjob.setMapOutputValueClass(Text.class);

            wcjob.setOutputKeyClass(Text.class);
            wcjob.setOutputValueClass(Text.class);

            wcjob.setInputFormatClass(TextInputFormat.class);
            wcjob.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(wcjob, inputTrainPath);
            FileOutputFormat.setOutputPath(wcjob, wcoutput);

            if(!wcjob.waitForCompletion(true)) {
              System.out.println("ERROR: MNBwordcount Job Failed!!!");
            }


            mnbController.delete(jobconf, prioroutput);
            Job priorjob = new Job(jobconf, "ComputePrior");
            priorjob.setJarByClass(mnbController.class);

            priorjob.setMapperClass(ComputePriorMap.class);
            priorjob.setReducerClass(ComputePriorReduce.class);

            priorjob.setMapOutputKeyClass(Text.class);
            priorjob.setMapOutputValueClass(Text.class);

            priorjob.setOutputKeyClass(Text.class);
            priorjob.setOutputValueClass(Text.class);

            priorjob.setInputFormatClass(TextInputFormat.class);
            priorjob.setOutputFormatClass(TextOutputFormat.class);
            TextInputFormat.addInputPath(priorjob, wcoutput);
            FileOutputFormat.setOutputPath(priorjob, prioroutput);
			//job2.addDependingJob(job);
            if(!priorjob.waitForCompletion(true)) { 
                System.out.println("ERROR: ComputePrior Job Failed!!!");
            }


            mnbController.delete(jobconf, model);
            Job trainjob = new Job(jobconf, "invertedindex");
            trainjob.setJarByClass(mnbController.class);

            trainjob.setMapperClass(InvertedIndexMap.class);
            //job2.setCombinerClass(MNBwordcountCombine.class);
            trainjob.setReducerClass(InvertedIndexReduce.class);

            trainjob.setMapOutputKeyClass(Text.class);
            trainjob.setMapOutputValueClass(Text.class);

            trainjob.setOutputKeyClass(Text.class);
            trainjob.setOutputValueClass(Text.class);

            trainjob.setInputFormatClass(TextInputFormat.class);
            trainjob.setOutputFormatClass(TextOutputFormat.class);

            TextInputFormat.addInputPath(trainjob, wcoutput);
            FileOutputFormat.setOutputPath(trainjob, model);
            if(!trainjob.waitForCompletion(true)) {
                System.out.println("ERROR: InvertedIndex Job Failed!!!");
            }

            mnbController.delete(jobconf, joined);
            Job testjoinjob = new Job(jobconf, "TestJoin");
            testjoinjob.setJarByClass(mnbController.class);
            testjoinjob.setMapperClass(TestModelMapper.class);
            testjoinjob.setMapperClass(TestDataMapper.class);
            testjoinjob.setReducerClass(TestReduce.class);
            testjoinjob.setMapOutputKeyClass(Text.class);
            testjoinjob.setMapOutputValueClass(Text.class);
            testjoinjob.setOutputKeyClass(Text.class);
            testjoinjob.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(testjoinjob, model, KeyValueTextInputFormat.class, TestModelMapper.class);
            MultipleInputs.addInputPath(testjoinjob, inputTestPath, TextInputFormat.class, TestDataMapper.class);
            FileOutputFormat.setOutputPath(testjoinjob, joined);

            if(!testjoinjob.waitForCompletion(true)) {
              System.out.println("ERROR: Test Job Failed!!!");
            }

            //mnbController.delete(jobconf, wcoutput);
            mnbController.delete(jobconf, classifyoutput);
            //mnbController.delete(jobconf, model);
           
            //jobconf = classifyjob.getConfiguration();
            Configuration classifyconf = new Configuration();
            DistributedCache.addCacheFile(new Path(prioroutput.toString()+"/part-r-00000").toUri(), classifyconf);
            //DistributedCache.addCacheFile(labelset.toUri(), classifyconf);
            Job classifyjob = new Job(classifyconf, "Classify");
            classifyjob.setJarByClass(mnbController.class);

            classifyjob.setMapperClass(ClassifyMap.class);
            classifyjob.setReducerClass(ClassifyReduce.class);

            classifyjob.setMapOutputKeyClass(Text.class);
            classifyjob.setMapOutputValueClass(Text.class);

            classifyjob.setOutputKeyClass(Text.class);
            classifyjob.setOutputValueClass(Text.class);

            classifyjob.setInputFormatClass(KeyValueTextInputFormat.class);
            classifyjob.setOutputFormatClass(TextOutputFormat.class);
            TextInputFormat.addInputPath(classifyjob, joined); //the trained model as input
            FileOutputFormat.setOutputPath(classifyjob, classifyoutput);

            if(!classifyjob.waitForCompletion(true)) {
                System.out.println("ERROR: Evaluation Job Failed!!!");
            }

            int i,j;
            int truecount = 0, falsecount = 0;
	        int count_real = 0;
	        int count_pred = 0;
	        double macro_p= 0.0, macro_r= 0.0, macro_f1;
	        String resultfile = classifyoutput.toString()+"/part-r-00000";
	        Path resultpath = new Path(resultfile);
	        FileSystem hdfs = FileSystem.get(classifyconf);
	        FSDataInputStream fsinput = hdfs.open(resultpath);
	        BufferedReader resultin = new BufferedReader(new InputStreamReader(fsinput));
	        String line = "";
	        HashMap<Integer, Double> MacroR = new HashMap<Integer, Double>();
	        HashMap<Integer, Double> MacroP = new HashMap<Integer, Double>();
	        while((line = resultin.readLine())!= null) {
	        	String[] pairs = line.split("\t");
			    if(pairs.length == 2) {
					String[] reallabels = pairs[0].split(",");
	                String[] predicted = pairs[1].split(",");
	                count_pred += predicted.length;
	                count_real += reallabels.length;
	                for (i = 0; i < reallabels.length; i++) {
	                    for (j = 0; j < predicted.length; j++) {
	                        if (predicted[j].equals(reallabels[i])) {
	                        	truecount++;
	                        	MacroR.put(Integer.parseInt(reallabels[i]), (double)truecount/(double)count_real);
	                        	MacroP.put(Integer.parseInt(reallabels[i]), (double)truecount/(double)count_pred);
	                        }
	                        else falsecount ++;
	                    }
	                }
			    }          
	        }
	        fsinput.close();
	        resultin.close();
 			if (count_real == 0 || count_pred == 0) {
	            System.out.println("ERROR: Divided by ZERO!");
	        }
	        else {
	            //macro_r = (float)truecount / (float)count_real;
	            //macro_p = (float)truecount / (float)count_pred;
	            for (Integer label : MacroR.keySet()){
	            	double recall = MacroR.get(label);
	            	double precision = MacroP.get(label);
	            	macro_r += recall;
	            	macro_p += precision;
	            }
	            macro_r /= MacroR.size();
	            macro_p /= MacroP.size();
				macro_f1 = (2 * macro_r * macro_p) / (macro_r + macro_p);
	            //output_key.set("recall");
	            //output_value.set(Float.toString(recall));
	            //context.write(output_key,output_value);
	            //Path outFile = new Path(classifyoutput.toString()+"/result");
                //if (hdfs.exists(outFile)) hdfs.delete(outFile, true);
	        	//FSDataOutputStream fsoutput = hdfs.create(outFile);
	        	//fsoutput.writeUTF("Macro Recall:"+Float.toString(macro_r)+"\n");
	        	//fsoutput.writeUTF("Macro Precision:"+Float.toString(macro_p)+"\n");
	        	//fsoutput.writeUTF("Macro F1:"+Float.toString(macro_f1)+"\n");
	            System.out.println("Macro Recall:"+Double.toString(macro_r));
	            System.out.println("Macro Precision:"+Double.toString(macro_p));
			    System.out.println("Macro F1:"+Double.toString(macro_f1));
	            //output_key.set("precision");
	            //output_value.set(Float.toString(precision));
	            //context.write(output_key,output_value);
	            //fsoutput.close();
	        }
            mnbController.delete(jobconf, wcoutput);

        } 
        catch(Exception e){
            e.printStackTrace();
        }
        System.out.println("Job finished!!!");
        return 0;
    }//run function

    public static void main(String[] args) throws Exception
    {
        int exitcode = ToolRunner.run(new mnbController(), args);
        System.exit(exitcode);
    } 
}