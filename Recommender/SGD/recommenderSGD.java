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
import java.io.DataOutput;  
import java.nio.channels.FileChannel;
import java.lang.Math;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.filecache.*;
//import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;

public class recommenderSGD
{
  
    public static float[] stringToVector(String input){

        if (input != null) {
            String[] inputarr = input.split(",");
            int size = inputarr.length;
            float[] result = new float[size];
            for(int i = 0; i < size; i++) {
                result[i] = Float.parseFloat(inputarr[i]);
            }
            return result;
        }
        else return null;
    }
    public static void initializeVector(float[] arr, int size){
        for(int i = 0; i < size; i++){
            arr[i]= 1.0f;
        }
    }
    public static float[] vectorPlus(float[] arr1, float[] arr2){
        int size1 = arr1.length;
        int size2 = arr2.length;
        float[] sum = new float[size1];
        initializeVector(sum, size1);
        if (size1 == size2){
            for (int i =0; i < size1; i++){
                sum[i] = arr1[i]+arr2[i];
            }
        }
        return sum;
    }
    public static float[] vectorMinus(float[] arr1, float[] arr2){
        int size1 = arr1.length;
        int size2 = arr2.length;
        float[] result = new float[size1];
        initializeVector(result, size1);
        if (size1 == size2){
            for (int i =0; i < size1; i++){
                result[i] = arr1[i]-arr2[i];
            }
        }
        return result;
    }
    public static float vectorMultiplication(float[] arr1, float[] arr2) {
        int size1 = arr1.length;
        int size2 = arr2.length;
        float sum = 0.0f;
        if (size1 == size2){
            for (int i =0; i < size1; i++){
                sum += arr1[i]*arr2[i];
            }
        }
        return sum;
    }
    public static float[] numberMultiplyVector(float num, float[] arr2) {
        int size2 = arr2.length;
        float[] result = new float[size2];
        initializeVector(result, size2);
        for (int i =0; i < size2; i++){
            result[i] = num*arr2[i];
        }
        return result;
    }
    public static class SGDMap extends Mapper<LongWritable,Text,Text,Text>
    {
        private Text word = new Text();
        private long users;
        private long items;
        private int count;
        private int round;
        private int nfactors;
        private Text output_value = new Text();
        private Text output_key = new Text();
        private HashMap<String, String> readData;
        @Override
        public void setup(Context context) throws IOException, InterruptedException 
        {
            super.setup(context);
            users = context.getConfiguration().getLong("users",100L);
            items = context.getConfiguration().getLong("items",100L);
            count = context.getConfiguration().getInt("count",100);
            round = context.getConfiguration().getInt("round",100);
			nfactors = context.getConfiguration().getInt("nfactors",100);
            readData = new HashMap<String,String>();
            try {
                Path[] cacheFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (cacheFile != null && cacheFile.length > 0) {
                    String line = "";
                    BufferedReader br =  new BufferedReader(new FileReader(cacheFile[0].toString()));
                    while((line = br.readLine())!= null) {
                        String[] ucount_str = line.split("\t");
                        if (ucount_str.length==2) readData.put(ucount_str[0],ucount_str[1]);
                    }
                    br.close();
                }
            }
            catch(IOException e) {
                System.out.println(e.toString());
            }
        }
      
    
        @Override
        public void map(LongWritable key, Text value, Context context) 
                        throws IOException, InterruptedException
        {

            String[] tokens = value.toString().split("(\t)|(\\:{2})");
            if (tokens.length >= 3) {
                String uid = tokens[0];
                String mid = tokens[1];
                float rating = Float.parseFloat(tokens[2]);
                int u_id = Integer.parseInt(uid);
                Random rg = new Random();
                int randomint = rg.nextInt(10);
                if (randomint!=0 && (u_id % randomint) == 0) {
                    String q_mid = readData.get("q,"+mid);
                    float[] q_i = stringToVector(q_mid);
                    
                    float[] temp_q = new float[nfactors];
                    initializeVector(temp_q, nfactors);

                    
                    String p_uid =  readData.get("p,"+uid);
                    float[] p_x =  stringToVector(p_uid);

                    float[] temp_p = new float[nfactors];
                    initializeVector(temp_p, nfactors);

                    for (int f = 0; f < nfactors; f++){
                        temp_q[f] = -2*(rating - vectorMultiplication(q_i, p_x))*p_x[f];                
                        output_key.set("q,"+mid);
                        output_value.set(Float.toString(temp_q[f]));
                        context.write(output_key,output_value);

                        temp_p[f] = -2*(rating - vectorMultiplication(q_i, p_x))*q_i[f];
                        output_key.set("p,"+uid);
                        output_value.set(Float.toString(temp_p[f]));
                        context.write(output_key,output_value);
                    }
                }
            }
        }
    }
     public static class SGDCombiner extends Reducer<Text,Text,Text,Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                            throws IOException, InterruptedException
        {
            String index = key.toString();
            float sum_part = 0.0f;
            String first="";
            String second = "";
            String[] arr = index.split(",");
            if(arr.lentgh == 2){
                first = arr[0];
                second = arr[1];
            }
            for(Text temp : values) {
                float middlepart = Integer.parseInt(temp.toString());
                sum_part += middlepart;
            }
            context.write(new Text(first), new Text(second+":"+Float.toString(sum_part)));
        }
    }
    public static class SGDReduce extends Reducer<Text,Text,Text,Text>
    {
        private long users;
        private long items;
        private int count;
        private int round;
        private int nfactors; 
        private HashMap<String, String> readData;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            users = context.getConfiguration().getLong("users",100L);
            items = context.getConfiguration().getLong("items",100L);
            count = context.getConfiguration().getInt("count",100);
            round = context.getConfiguration().getInt("round",100);
            nfactors = context.getConfiguration().getInt("nfactors",100);
            readData = new HashMap<String,String>();
            try {
                Path[] cacheFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (cacheFile != null && cacheFile.length > 0) {
                    String line = "";
                    BufferedReader br =  new BufferedReader(new FileReader(cacheFile[0].toString()));
                    while((line = br.readLine())!= null) {
                        String[] ucount_str = line.split("\t");
                        if (ucount_str.length==2) readData.put(ucount_str[0],ucount_str[1]);
                    }
                    br.close();
                }
            }
            catch(IOException e) {
                System.out.println(e.toString());
            }
        }

        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                            throws IOException, InterruptedException
        {
            StringBuilder vec_output = new StringBuilder();
            float eta = 0.0000001f, lambda_1 = 0.001f, lambda_2 = 0.001f;
            String index = key.toString();
            int u,m,j;
          

            if (index.equals("q")) {
                List<Integer> item_index = new ArrayList<Integer>();
                float sum_q = 0.0f;
                //initializeVector(sum, 100);
                String var = "";
                int i,f;
               // while (values.hasNext()) {
                for(Text temp : values) {
                    String[] arr = temp.toString().split(":");
                    if (arr.length == 2) {
                        i = Integer.parseInt(arr[0]);
                        if (!item_index.contains(i)) item_index.add(i);
                        float middlepart = Float.parseFloat(arr[1]);
                        sum_q += middlepart;
                    }
                }
                float delta_q_if = sum_q;
                for (m = 1; m <= items; m++) { 
                    String line = readData.get(index+","+Integer.toString(m));
                    if (line!=null){
                        if (!item_index.contains(new Integer(m)))
                             context.write(new Text(index+","+Integer.toString(m)), new Text(line));
                        else {
                            String[] arr = line.split(",");
                            // update all values in Q
                            for (j = 0; j < arr.length; j++){
                                delta_q_if += 2 * lambda_2 * Float.parseFloat(arr[j]);
                                arr[j] =  Float.toString(Float.parseFloat(arr[j]) - eta * delta_q_if);
                            }
                            // update one column in Q matrix
                            //delta_q_if += 2 * lambda_2 * Float.parseFloat(arr[round%nfactors]);
                            //arr[round%nfactors] =  Float.toString(Float.parseFloat(arr[round%nfactors]) - eta * delta_q_if);
                            StringBuilder newline = new StringBuilder();
                            for (j = 0; j < arr.length; j++) {
                                if (j < arr.length-1)  newline.append(arr[j]+",");
                                else newline.append(arr[j]);
                            }
                            //readData.put((index+","+Integer.toString(m)),newline.toString());
                            context.write(new Text(index+","+Integer.toString(m)),new Text(newline.toString()));
                        }
                    }
                }
                item_index.clear();
            } 
            else if (index.equals("p")) {
                List<Integer> user_index = new ArrayList<Integer>();
                float sum_p = 0.0f;
                //initializeVector(sum, 100);
                String var = "";
                int x,f;
               // while (values.hasNext()) {
                for (Text temp : values) {
                    String[] arr = temp.toString().split(":");
                    if (arr.length == 2) {
                        x = Integer.parseInt(arr[0]);
                        if (!user_index.contains(x)) user_index.add(x);
                        float middlepart = Float.parseFloat(arr[1]);
                        sum_p += middlepart;
                    }
                }
                float delta_p_if = sum_p;
                for (u = 1; u <= users; u++) {
                    String line = readData.get(index+","+Integer.toString(u));
                    if (line!=null) {
                        if (!user_index.contains(new Integer(u)))
                            context.write(new Text(index+","+Integer.toString(u)), new Text(line));
                        else {
                            String[] arr = line.split(",");
                            for (j = 0; j < arr.length; j++){
                                delta_p_if += 2 * lambda_2 * Float.parseFloat(arr[j]);
                                arr[j] =  Float.toString(Float.parseFloat(arr[j]) - eta * delta_p_if);
                            }

                            //delta_p_if += 2 * lambda_2 * Float.parseFloat(arr[round%nfactors]);
                           // arr[round%nfactors] =  Float.toString(Float.parseFloat(arr[round%nfactors]) - eta * delta_p_if);
                            StringBuilder newline = new StringBuilder();
                            for (j = 0; j < arr.length; j++) {
                                if (j < arr.length-1)  newline.append(arr[j]+",");
                                else newline.append(arr[j]);
                            }
                            //readData.put((index+","+Integer.toString(u)), newline.toString());
                            context.write(new Text(index+","+Integer.toString(u)), new Text(newline.toString()));
                        }
                    }
                }//for each user
                user_index.clear();
            }//elseif p
        }
    }

    public static class RMSEMap extends Mapper<LongWritable,Text,Text,Text>
    {
        private Text output_value = new Text();
        private Text output_key = new Text("1");
        private HashMap<String, String> readData;
       

        @Override
        public void setup(Context context) throws IOException, InterruptedException 
        {
            super.setup(context);
            readData = new HashMap<String,String>();
            try {
                Path[] cacheFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (cacheFile != null && cacheFile.length > 0) {
                    String line = "";
                    BufferedReader br =  new BufferedReader(new FileReader(cacheFile[0].toString()));
                    while((line = br.readLine())!= null) {
                        String[] ucount_str = line.split("\t");
                        if (ucount_str.length==2) readData.put(ucount_str[0],ucount_str[1]);
                    }
                    br.close();
                }
            }
            catch(IOException e) {
                System.out.println(e.toString());
            }
        }
      
    
        @Override
        public void map(LongWritable key, Text value, Context context) 
                        throws IOException, InterruptedException
        {
            String[] tokens = value.toString().split("(\t)|(\\:{2})");
            if (tokens.length >= 3) {
                String uid = tokens[0];
                String mid = tokens[1];
                float rating = Float.parseFloat(tokens[2]);

                String q_mid = readData.get("q,"+mid);
                float[] q_i = stringToVector(q_mid);

                String p_uid =  readData.get("p,"+uid);
                float[] p_x =  stringToVector(p_uid);

                double diff = Math.pow((double)(rating - vectorMultiplication(q_i, p_x)),2.0); 
                output_value.set(Double.toString(diff));             
                context.write(output_key,output_value);
            }
        }
    }
    public static class RMSEReduce extends Reducer<Text,Text,Text,Text>
    {
        private int round;
        private int count;
        private Text output_value = new Text();
        private Text output_key = new Text();
       
 		@Override
        public void setup(Context context) throws IOException, InterruptedException 
        {
            super.setup(context);
            count = context.getConfiguration().getInt("count",100);
            round = context.getConfiguration().getInt("round",100);
            
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                            throws IOException, InterruptedException
        {
            String index = key.toString();
            double sum_error = 0.0;
            double root_error = 0.0;
            for(Text temp : values) {
                String str = temp.toString();
                double diff = Double.parseDouble(str);
                sum_error += diff;
            }
            root_error = Math.sqrt(sum_error/(double)count);
            output_key.set(Integer.toString(round));
            output_value.set(Double.toString(root_error));//here square root error
            context.write(output_key,output_value);
        }
    }

    public static class TestMap extends Mapper<LongWritable,Text,Text,Text>
    {
        private Text output_value = new Text();
        private Text output_key = new Text("1");
        private HashMap<String, String> readData;
       

        @Override
        public void setup(Context context) throws IOException, InterruptedException 
        {
            super.setup(context);
            readData = new HashMap<String,String>();
            try {
                Path[] cacheFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (cacheFile != null && cacheFile.length > 0) {
                    String line = "";
                    BufferedReader br =  new BufferedReader(new FileReader(cacheFile[0].toString()));
                    while((line = br.readLine())!= null) {
                        String[] ucount_str = line.split("\t");
                        if (ucount_str.length==2) readData.put(ucount_str[0],ucount_str[1]);
                    }
                    br.close();
                }
            }
            catch(IOException e) {
                System.out.println(e.toString());
            }
        }
    
        @Override
        public void map(LongWritable key, Text value, Context context) 
                        throws IOException, InterruptedException
        {
            String[] tokens = value.toString().split("(\t)|(\\:{2})");
            if (tokens.length >= 3) {
                String uid = tokens[0];
                String mid = tokens[1];
                float rating = Float.parseFloat(tokens[2]);

                String q_mid = readData.get("q,"+mid);
                float[] q_i = stringToVector(q_mid);

                String p_uid =  readData.get("p,"+uid);
                float[] p_x =  stringToVector(p_uid);

                double diff = Math.pow((double)(rating - vectorMultiplication(q_i, p_x)),2.0); 
                output_value.set(Double.toString(diff));             
                context.write(output_key,output_value);
            }
        }
    }
    public static class TestReduce extends Reducer<Text,Text,Text,Text>
    {
        private long users;
        private long items;
        private int count;
        private int round;
        private Text output_value = new Text();
        private Text output_key = new Text();
       
        @Override
        public void setup(Context context) throws IOException, InterruptedException 
        {
            super.setup(context);
            users = context.getConfiguration().getLong("users",100L);
            items = context.getConfiguration().getLong("items",100L);
            count = context.getConfiguration().getInt("count",100);
            round = context.getConfiguration().getInt("round",100);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                            throws IOException, InterruptedException
        {
            String index = key.toString();
            double sum_error = 0.0;
            double root_error = 0.0;
            for(Text temp : values) {
                String str = temp.toString();
                double diff = Double.parseDouble(str);
                sum_error += diff;
            }
            root_error = Math.sqrt(sum_error/(double)count);
            output_key.set(Integer.toString(round));
            output_value.set(Double.toString(root_error));//here square root error
            context.write(output_key,output_value);
        }
    }
    public static void main(String[] args) throws Exception 
    {

        if(args.length!=12){
          System.out.println("wrong input parameter: traininput testinput numofIterations users items count_train count_test");
          return;
        }
        
        int iteration = 0;
        Path inputTrainPath = new Path(args[0]);
        Path intputTestPath = new Path(args[1]);
       
        int numIteration = Integer.parseInt(args[2]);
        int nfactors = Integer.parseInt(args[3]);
        long users = Long.parseLong(args[4]);
        long items = Long.parseLong(args[5]);

        int count_train = Integer.parseInt(args[6]);
        int count_test = Integer.parseInt(args[7]);
        Path outputPath = new Path(args[8]);//outputSGD
        Path outputRMSE = new Path(args[9]);//rmseSGD
        Path pqmatrix = new Path(args[10]);//matrixSGD
        Path outputTest = new Path(args[11]);//testSGD

        Path bgdout = null;
        Path rmseout = null;
        int exitCode=0, exitCode2 = 0;
       
        try {
            //for test, comment out while first
            //jobconf is for SGD 
            while(iteration<numIteration) {
                Configuration jobconf = new Configuration();
                if (iteration == 0) DistributedCache.addCacheFile(new Path("matrixSGD/pqmatrix").toUri(), jobconf);
                else {
                	int lasti = iteration - 1;
                	DistributedCache.addCacheFile(new Path(args[10]+"round"+lasti+"/part-r-00000").toUri(), jobconf);
                }
                jobconf.setLong("users",users);
                jobconf.setLong("items",items);
                jobconf.setInt("count",count_train);
                jobconf.setInt("round", iteration);
 				jobconf.setInt("nfactors", nfactors);

                Job job = new Job(jobconf);
                job.setJobName("recommenderSGD"+iteration);
                job.setJarByClass(recommenderSGD.class);

                job.setMapperClass(SGDMap.class);
                job.setCombinerClass(SGDCombiner.class);
                job.setReducerClass(SGDReduce.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                TextInputFormat.addInputPath(job, inputTrainPath);

                bgdout = new Path(outputPath,"round"+iteration);
                FileOutputFormat.setOutputPath(job, bgdout);

                exitCode = job.waitForCompletion(true) ? 0 : 1;
                if( exitCode == 0) {              
                } else {
                  System.out.println("Job Failed!!!");
                }

                FileSystem hdfs = FileSystem.get(jobconf);
                FileUtil.copy(hdfs, bgdout, hdfs, pqmatrix, false, true, jobconf);
                
                if(iteration>=0) {
                    //This job is for computing rmse to decide if it converges
                    Configuration jobrmse = new Configuration();
	                DistributedCache.addCacheFile(new Path(args[8]+"/round"+iteration+"/part-r-00000").toUri(), jobrmse);
                    jobrmse.setLong("users",users);
                    jobrmse.setLong("items",items);
                    jobrmse.setInt("count",count_train);
                    jobrmse.setInt("round", iteration);

                    Job job2 = new Job(jobrmse);
                    job2.setJobName("RMSE"+iteration);
                    job2.setJarByClass(recommenderSGD.class);

                    job2.setMapperClass(RMSEMap.class);
                    job2.setReducerClass(RMSEReduce.class);
                    //job.setCombinerClass(CombinerDeltaValue.class);

                    job2.setMapOutputKeyClass(Text.class);
                    job2.setMapOutputValueClass(Text.class);
                    job2.setOutputKeyClass(Text.class);
                    job2.setOutputValueClass(Text.class);
                    job2.setInputFormatClass(TextInputFormat.class);
                    job2.setOutputFormatClass(TextOutputFormat.class);
                    TextInputFormat.addInputPath(job2, inputTrainPath);

                    rmseout = new Path(outputRMSE,"round"+iteration);
                    FileOutputFormat.setOutputPath(job2, rmseout);

                    exitCode2 = job2.waitForCompletion(true) ? 0 : 1;
                    if(exitCode2 == 0) {  
	                    if (iteration >= 1) {
		                    String line_c = "";
		                    String line_b = "";
		                    float diff_error = 0.0f;
		                    int last = iteration-1;
		                    Path prior = new Path(outputRMSE,"round"+last);
                            FileSystem fs = FileSystem.get(jobrmse);
                            Path current = new Path(rmseout.toString()+"/part-r-00000");
                            Path before = new Path(prior.toString()+"/part-r-00000");
		                    BufferedReader br_current =  
		                    		new BufferedReader(new InputStreamReader(fs.open(current)));
		                    BufferedReader br_before =  
		                    		new BufferedReader(new InputStreamReader(fs.open(before)));
		                    line_c = br_current.readLine();
		                    line_b = br_before.readLine();
		                    String[] arr_c = line_c.split("\t");
		                    String[] arr_b = line_b.split("\t");
		                    if (arr_c.length == 2 && arr_b.length == 2) {
		                        diff_error =  Float.parseFloat(arr_b[1]) - Float.parseFloat(arr_c[1]);
		                    }
		                    br_current.close();
		                    br_before.close();
		                    if ((diff_error < 0.2 && diff_error > 0.0) | iteration > 10 ) {
                                break;
                            }
	                	}            
                    } 
                    else {
                      System.out.println("Job Failed!!!");
                    } 
                }
                iteration++;
            }//while
            return;
        }
        catch(Exception e){
            e.printStackTrace();
        }

        //this job is responsible for testing on test data
		Configuration jobtest = new Configuration();
        DistributedCache.addCacheFile(new Path(args[10]+"/round"+iteration+"/part-r-00000").toUri(), jobtest);
           
        jobtest.setLong("users",users);
        jobtest.setLong("items",items);
        jobtest.setInt("count",count_test);
        jobtest.setInt("round", iteration);

        Job job = new Job(jobtest);
        job.setJobName("Test"+iteration);
        job.setJarByClass(recommenderSGD.class);

        job.setMapperClass(TestMap.class);
        job.setReducerClass(TestReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, intputTestPath);
        FileOutputFormat.setOutputPath(job, outputTest);

        exitCode = job.waitForCompletion(true) ? 0 : 1;
        if( exitCode == 0) {              
        } else {
          System.out.println("Job Failed!!!");
        }
        System.out.println("Job finished!!!");
    }//main function 
}//recommenderSGD class