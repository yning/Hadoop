import java.io.IOException;
import java.util.*;
import java.util.Date;
import java.util.ArrayList;
import java.lang.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.System;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.*;
//import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PairsLift 
{
    public  static class PrePairMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
    {
        private Text word = new Text();
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
                throws IOException 
        {
            String[] tokens = value.toString().split("\\t|\\:{2}");
            if (tokens.length >= 3) {
                float rating = Float.parseFloat(tokens[2]);
                String mid = tokens[1];
                if(rating >= 4.0) 
                    output.collect(new Text(tokens[0]), new Text(mid));
                //output: (userid movieid)
            }
        }      
    }
    public static class PrePairReduce extends MapReduceBase implements Reducer<Text,Text,Text,Text> 
    {
        //private IntWritable one = new IntWritable(1);

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) 
        	     throws IOException 
        {
            int count = 0;

            ArrayList movies = new ArrayList();
            while (values.hasNext()) {
              String movieid = values.next().toString();
              movies.add(movieid);
            }
            int size = movies.size();
            for (int i = 0; i < size; i++) {
                String mid = (String)movies.get(i);
                //wordpair.setWord(mid.toString());
                for (int j = 0; j < size; j++) {
                    String mid_2 = (String)movies.get(j);
                    if (!mid_2.equals(mid)) {
                        //wordpair.setNeighbor(mid_2.toString());
                        String pair = mid + "," + mid_2;
                        //System.out.println(pair);
                        output.collect(new Text(pair),  new Text("1"));
                    }
                }
                output.collect(new Text(mid+",*"), new Text("1"));
                //output.collect(new Text("*,"+mid), one);
            }
            movies.clear();
        }
    }

     public static class PairsOccurrenceMap
      extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
    {
        private IntWritable one = new IntWritable(1);
      
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
                throws IOException 
        {
            String[] tokens = value.toString().split("\t");
            String mid_pair = tokens[0];
            String count = tokens[1];
            //if (mid_pair.contains("*"))
                output.collect(new Text(mid_pair), new Text(count));
           // else
            //    output.collect(new Text(mid_pair), new Text(Integer.toString(count)+","+Integer.toString(this.usercount)));
        }      
    }
    public static class PairsOccurrenceReduce extends MapReduceBase implements Reducer<Text,Text,Text,Text> 
    {
        //
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) 
               throws IOException 
        {
            int sum = 0;
            //String usercount ="";
            while (values.hasNext()) {
                //String[] value = values.next().toString().split(",");
                String value = values.next().toString();
                sum += Integer.parseInt(value);
                //usercount = value[1];
            }

            output.collect(key, new Text(Integer.toString(sum)));
        }
    }
    public static class PairsCondMap
      extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
    {
        //private IntWritable one = new IntWritable(1);
        private int usercount = 0;
        public void configure(JobConf conf) {
            try {
                Path[] cacheFile = DistributedCache.getLocalCacheFiles(conf);
                if (cacheFile != null && cacheFile.length > 0) {
                    String line = "";
                    BufferedReader br =  new BufferedReader(new FileReader(cacheFile[0].toString()));
                    if ((line = br.readLine())!=null) {
                        String[] ucount_str = line.split("\t");
                        usercount = Integer.parseInt(ucount_str[0]);
                        //System.out.println("usercount is:"+usercount);
                    }
                    br.close();
                }
            }
            catch(IOException e) {
                System.out.println(e.toString());
            }
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
                throws IOException 
        {
            String[] tokens = value.toString().split("\t");
            String mid = tokens[0];
            String freq_usercount = tokens[1];
            int sum_all = 0;
            String[] pair = mid.split(",");
            if (pair.length == 2) {
                String oneid = pair[0];
                String another = pair[1];
                StringBuilder rest = new StringBuilder(another);
                rest.append(":" + freq_usercount);
                //output.collect(new Text(frequency), new Text(pairs));
                if (another.equals("*"))
                    output.collect(new Text(oneid), new Text(rest.toString() + "," + Integer.toString(usercount)));
                else 
                    output.collect(new Text(oneid), new Text(rest.toString()));
            }
        }      
    }
    public static class PairsCondReduce extends MapReduceBase implements Reducer<Text,Text,Text,Text> 
    {
        //
        
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) 
               throws IOException 
        {
            int sum = 0;
            int usercount = 0;
            HashMap<String, String> thiskey = new HashMap<String, String>();
            while (values.hasNext()) {
                String[] rest = values.next().toString().split(":"); 
                if (rest.length==2) {
                    String anotherid = rest[0];
                    if (anotherid.equals("*")) {
                        String[] freq_ucount = rest[1].split(",");
                        String freq = freq_ucount[0];
                        String ucount = freq_ucount[1];
                        sum = Integer.parseInt(freq);
                        usercount = Integer.parseInt(ucount);
                        //usercount = Integer.parseInt(ucount);
                    }
                    else {
                        thiskey.put(anotherid,rest[1]);

                    }
                }
            }
            Set <Map.Entry<String, String>> all = thiskey.entrySet();
            Iterator <Map.Entry<String, String>> maps = all.iterator();
            if (sum!=0 && usercount!=0) {
                float p_key = (float) sum /(float) usercount;
                String p_k_str =  String.format("%f", p_key);
                output.collect(key, new Text(p_k_str));
                while (maps.hasNext()) {
                    Map.Entry<String, String> entry = maps.next();
                    String nbr = entry.getKey();
                    String count = entry.getValue();
                    float cond = (float) Integer.parseInt(count)/ (float) sum;
                    String cd = String.format("%f", cond);
                    output.collect(new Text(nbr), new Text(key.toString()+","+cd));     
                }
            }   
        }
    }
    public static class PairsLiftMap
      extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
    {
        //private IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
                throws IOException 
        {
                String[] tokens = value.toString().split("\t");
                if (tokens.length == 2) {
                    output.collect(new Text(tokens[0]), new Text(tokens[1]));
                }
        }      
    }
     public static class PairsLiftReduce extends MapReduceBase implements Reducer<Text,Text,Text,Text> 
    {
        //
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) 
               throws IOException 
        {
            HashMap<String, Float> thiskey = new HashMap<String, Float>();
            float p_key =0.0f;
            while (values.hasNext()) {
                String value = values.next().toString(); 
                if (value.contains(",")) {
                    //System.out.println("To get conditional probability!");
                    String[] id_cond = value.split(",");
                    if (id_cond.length == 2) thiskey.put(id_cond[0],Float.parseFloat(id_cond[1]));
                }
                else {
                    //System.out.println("To get margin probability!");
                    p_key = Float.parseFloat(value);
                }
            }
            Set <Map.Entry<String, Float>> all = thiskey.entrySet();
            Iterator <Map.Entry<String, Float>> maps = all.iterator();
            if (p_key!=0.0f) {
                while (maps.hasNext()) {
                    Map.Entry<String, Float> entry = maps.next();
                    String nbr = entry.getKey();
                    Float cond = entry.getValue();
                    float lift = (float)cond / p_key;
                    String lift_str = String.format("%f", lift);
                    output.collect(new Text(key.toString()+","+nbr), new Text(lift_str));
                }
            }   
        }
    }
     public  static class ShufflePairsMap
      extends MapReduceBase implements Mapper<LongWritable, Text, FloatWritable, Text> 
    {
    	//private IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, OutputCollector<FloatWritable, Text> output, Reporter reporter) 
                throws IOException 
        {
        		String[] tokens = value.toString().split("\t");
        		String pairs_id = tokens[0];
        		String lift_str = tokens[1];
        		//output.collect(new Text(frequency), new Text(pairs));
                float lift = Float.parseFloat(lift_str);
                if (lift >=1.2)
				    output.collect(new FloatWritable(lift), new Text(pairs_id));
        }      
    }
    public static void main(String[] args) throws Exception 
    {
        //JobControl jobsctrl = new JobControl("jobs");

        //Configuration conf = new Configuration();
      
        //JobConf job = new JobConf(conf, "PairsOccurrence");
        JobConf conf1 = new JobConf(PairsLift.class);
        JobConf conf2 = new JobConf(PairsLift.class);
        JobConf conf3 = new JobConf(PairsLift.class);
        JobConf conf4 = new JobConf(PairsLift.class);
        JobConf conf5 = new JobConf(PairsLift.class);
        conf1.setJobName("Prepair");
        conf2.setJobName("PairsOccurrence");
		conf3.setJobName("PairsConditional");
        conf4.setJobName("PairsLift");
        conf5.setJobName("Shuffle");
        DistributedCache.addCacheFile(new Path(args[6]).toUri(), conf3);

        conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(Text.class);
        conf2.setOutputKeyClass(Text.class);//change the type for output (sum, pair) to get top frequent pairs
        conf2.setOutputValueClass(Text.class);
        conf3.setOutputKeyClass(Text.class);//change the type for output (sum, pair) to get top frequent pairs
        conf3.setOutputValueClass(Text.class);
        conf4.setOutputKeyClass(Text.class);//change the type for output (sum, pair) to get top frequent pairs
        conf4.setOutputValueClass(Text.class);
        conf5.setOutputKeyClass(FloatWritable.class);//change the type for output (sum, pair) to get top frequent pairs
        conf5.setOutputValueClass(Text.class);

        conf1.setMapperClass(PrePairMap.class);
        conf1.setReducerClass(PrePairReduce.class);

        conf2.setMapperClass(PairsOccurrenceMap.class);
        conf2.setReducerClass(PairsOccurrenceReduce.class);
        //conf2.setPartitionerClass(movieidPartition.class);
        

        conf3.setMapperClass(PairsCondMap.class);
        conf3.setReducerClass(PairsCondReduce.class);

        conf4.setMapperClass(PairsLiftMap.class);
        conf4.setReducerClass(PairsLiftReduce.class);

        conf5.setMapperClass(ShufflePairsMap.class);
        conf5.setNumReduceTasks(1);

        conf1.setInputFormat(TextInputFormat.class);
        conf1.setOutputFormat(TextOutputFormat.class);
        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);
        conf3.setInputFormat(TextInputFormat.class);
        conf3.setOutputFormat(TextOutputFormat.class);
        conf4.setInputFormat(TextInputFormat.class);
        conf4.setOutputFormat(TextOutputFormat.class);
        conf5.setInputFormat(TextInputFormat.class);
        conf5.setOutputFormat(TextOutputFormat.class);

      
        FileInputFormat.setInputPaths(conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf1, new Path(args[1]));

        FileInputFormat.setInputPaths(conf2, new Path(args[1]));//??
        FileOutputFormat.setOutputPath(conf2, new Path(args[2]));//??

        FileInputFormat.setInputPaths(conf3, new Path(args[2]));//??
        FileOutputFormat.setOutputPath(conf3, new Path(args[3]));//??

        FileInputFormat.setInputPaths(conf4, new Path(args[3]));//??
        FileOutputFormat.setOutputPath(conf4, new Path(args[4]));//??

        FileInputFormat.setInputPaths(conf5, new Path(args[4]));//??
        FileOutputFormat.setOutputPath(conf5, new Path(args[5]));//??

        Job job1 = new Job(conf1);
        Job job2 = new Job(conf2);
        Job job3 = new Job(conf3);
        Job job4 = new Job(conf4);
        Job job5 = new Job(conf5);
        //jobsctrl.addJob(job1);
        //jobsctrl.addJob(job2);
        //jobsctrl.addJob(job3);
        //jobsctrl.addJob(job4);
        //jobsctrl.addJob(job5);
        job2.addDependingJob(job1);
        job3.addDependingJob(job2);
        job4.addDependingJob(job3);
        job5.addDependingJob(job4);
        //jobsctrl.run();

        JobClient.runJob(conf1);
        JobClient.runJob(conf2);
        JobClient.runJob(conf3);
        JobClient.runJob(conf4);
        JobClient.runJob(conf5);
        //conf.waitForCompletion(true);
        System.out.println("Job finished!!!");
    } 
}