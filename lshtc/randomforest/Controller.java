/* ************************ *
*         Yue Ning          *
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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Controller extends Configured implements Tool
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
            Configuration jobconf = getConf();

            Path dict = new Path(jobconf.get("dictionary"));
            Path inputTrainPath = new Path(jobconf.get("train"));
            Path inputTestPath = new Path(jobconf.get("test"));
            Path output = new Path(jobconf.get("output"));

            Path gainoutput = new Path(output.getParent(),"gain");
            Path testoutput = new Path(output.getParent(),"eval");
            DistributedCache.addCacheFile(dict.toUri(), jobconf);
            Controller.delete(jobconf, gainoutput);
            Job gainjob= new Job(jobconf, "InfoGain");
            gainjob.setJarByClass(Controller.class);

            gainjob.setMapperClass(InformationGainMap.class);
            gainjob.setReducerClass(InformationGainReduce.class);

            gainjob.setMapOutputKeyClass(Text.class);
            gainjob.setMapOutputValueClass(Text.class);

            gainjob.setOutputKeyClass(Text.class);
            gainjob.setOutputValueClass(DecisionTree.class);

            gainjob.setInputFormatClass(TextInputFormat.class);
            gainjob.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.addInputPath(gainjob, inputTrainPath);
            FileOutputFormat.setOutputPath(gainjob, gainoutput);

            if(!gainjob.waitForCompletion(true)) {
              System.out.println("ERROR: Info Gain Job Failed!!!");
            }

            DistributedCache.addCacheFile(inputTestPath.toUri(), jobconf);
            Controller.delete(jobconf, testoutput);
            Job testjob= new Job(jobconf, "Test");
            testjob.setJarByClass(Controller.class);

            testjob.setMapperClass(TestRandomForestMap.class);
            testjob.setReducerClass(TestRandomForestReduce.class);

            testjob.setMapOutputKeyClass(Text.class);
            testjob.setMapOutputValueClass(Text.class);

            testjob.setOutputKeyClass(Text.class);
            testjob.setOutputValueClass(Text.class);

            testjob.setInputFormatClass(SequenceFileInputFormat.class);
            testjob.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(testjob, gainoutput);
            FileOutputFormat.setOutputPath(testjob, testoutput);

            if(!testjob.waitForCompletion(true)) {
              System.out.println("ERROR: Hierarchy Job Failed!!!");
            }

            int i,j;
            int truecount = 0, falsecount = 0;
            int count_real = 0;
            int count_pred = 0;
            double macro_p= 0.0, macro_r= 0.0, macro_f1;
            String resultfile = testoutput.toString()+"/part-r-00000";
            Path resultpath = new Path(resultfile);
            FileSystem hdfs = FileSystem.get(jobconf);
            FSDataInputStream fsinput = hdfs.open(resultpath);
            BufferedReader resultin = new BufferedReader(new InputStreamReader(fsinput));
            String line = "";
            HashMap<Integer, Double> MacroR = new HashMap<Integer, Double>();
            HashMap<Integer, Double> MacroP = new HashMap<Integer, Double>();
            while((line = resultin.readLine())!= null) {
                String[] oneline = line.split("\t");
                int docid = Integer.parseInt(oneline[0]);
                String[] pairs = oneline[1].split(":");
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
                System.out.println("Macro Recall:"+Double.toString(macro_r));
                System.out.println("Macro Precision:"+Double.toString(macro_p));
                System.out.println("Macro F1:"+Double.toString(macro_f1));
            }
            

        } 
        catch(Exception e){
            e.printStackTrace();
        }
        System.out.println("Job finished!!!");
        return 0;
    }//run function

    public static void main(String[] args) throws Exception
    {
        int exitcode = ToolRunner.run(new Controller(), args);
        System.exit(exitcode);
    } 
}
