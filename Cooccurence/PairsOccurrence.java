import java.io.IOException;
import java.util.*;
import java.util.Date;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
//import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PairsOccurrence //extends Configured implements Tool
{
    public  static class PrePairMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
    {
        private Text word = new Text();
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
                throws IOException 
        {
            String[] tokens = value.toString().split("\\t|\\:{2}");
            if (tokens.length >= 3) {
                Float rating = Float.parseFloat(tokens[2]);
                String mid = tokens[1];
                if(rating >= 4.0f) 
                    output.collect(new Text(tokens[0]), new Text(mid));
            }
        }      
    }
    public static class PrePairReduce extends MapReduceBase implements Reducer<Text,Text,Text,Text> 
    {
        //private IntWritable one = new IntWritable(1);

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) 
        	     throws IOException 
        {
        	ArrayList movies = new ArrayList();
            while (values.hasNext()) {
              String movieid = values.next().toString();
              movies.add(movieid);
            }
            int size = movies.size();
            for (int i = 0; i < size; i++) {
                String mid = (String)movies.get(i);
                for (int j = 0; j < size; j++) {
                    String mid_2 = (String)movies.get(j);
                    if (!mid_2.equals(mid)) {
                        String pair = mid + "," + mid_2;
                        output.collect(new Text(pair), new Text("1"));
                    }
                }
            }
            movies.clear();
        }
    }
     public  static class PairsOccurrenceMap
      extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> 
    {
    	private IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) 
                throws IOException 
        {
        		String[] tokens = value.toString().split("\t");
				output.collect(new Text(tokens[0]), one);
        }      
    }
    public static class PairsOccurrenceReduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> 
    {
    	//
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output, Reporter reporter) 
               throws IOException 
        {
            int sum = 0;
            while (values.hasNext()) {
              sum += values.next().get();
            }
            output.collect(key,new IntWritable(sum));
        }
    }
     public  static class ShufflePairsMap
      extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> 
    {
    	//private IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) 
                throws IOException 
        {
        		String[] tokens = value.toString().split("\t");
        		String pairs = tokens[0];
        		String frequency = tokens[1];
        		//output.collect(new Text(frequency), new Text(pairs));
				output.collect(new IntWritable(Integer.parseInt(frequency)), new Text(pairs));
        }      
    }

   // public int run(String[] args) throws Exception 
    public static void main(String[] args) throws Exception
    {
        
        //JobControl jobsctrl = new JobControl("jobs");

        //Configuration conf = new Configuration();
      
        JobConf conf1 = new JobConf(PairsOccurrence.class);
        JobConf conf2 = new JobConf(PairsOccurrence.class);
        JobConf conf3 = new JobConf(PairsOccurrence.class);
        conf1.setJobName("Prepair");
        conf2.setJobName("PairsOccurrence");
		conf3.setJobName("Topfrequent");

        conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(Text.class);
        conf2.setOutputKeyClass(Text.class);//change the type for output (sum, pair) to get top frequent pairs
        conf2.setOutputValueClass(IntWritable.class);
        conf3.setOutputKeyClass(IntWritable.class);//change the type for output (sum, pair) to get top frequent pairs
        conf3.setOutputValueClass(Text.class);

        conf1.setMapperClass(PrePairMap.class);
        conf1.setReducerClass(PrePairReduce.class);

        conf2.setMapperClass(PairsOccurrenceMap.class);
        conf2.setReducerClass(PairsOccurrenceReduce.class);

        conf3.setMapperClass(ShufflePairsMap.class);
        conf3.setNumReduceTasks(1);
        //conf3.setReducerClass(ShufflePairsReduce.class);

        conf1.setInputFormat(TextInputFormat.class);
        conf1.setOutputFormat(TextOutputFormat.class);
        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);
        conf3.setInputFormat(TextInputFormat.class);
        conf3.setOutputFormat(TextOutputFormat.class);
      
        FileInputFormat.setInputPaths(conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf1, new Path(args[1]));

        FileInputFormat.setInputPaths(conf2, new Path(args[1]));//??
        FileOutputFormat.setOutputPath(conf2, new Path(args[2]));//??

        FileInputFormat.setInputPaths(conf3, new Path(args[2]));//??
        FileOutputFormat.setOutputPath(conf3, new Path(args[3]));//??

        Job job1 = new Job(conf1);
        Job job2 = new Job(conf2);
        Job job3 = new Job(conf3);
        //jobsctrl.addJob(job1);
        //jobsctrl.addJob(job2);
        //jobsctrl.addJob(job3);
        job2.addDependingJob(job1);
        job3.addDependingJob(job2);
        //jobsctrl.run();
        JobClient.runJob(conf1);
        JobClient.runJob(conf2);
        JobClient.runJob(conf3);

        System.out.println("Job finished!!!");

        //JobClient.runJob(conf);
        //conf.waitForCompletion(true);

    } 
     /*   public static void main(String[] args) throws Exception
    {
        long starttime = new Date().getTime();
        int res = ToolRunner.run(new Configuration(), new PairsOccurrence(), args);
        long endtime = new Date().getTime();
        long diff = endtime -starttime;
        System.out.printf("Time spent: %l",diff);
        System.exit(res);
    }*/
}