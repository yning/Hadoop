import java.io.IOException;
import java.util.*;
import java.util.Date;
import java.util.ArrayList;
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

public class PairsConditional 
{
    public  static class PrePairMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
    {
        private Text word = new Text();
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
                throws IOException 
        {
            String[] tokens = value.toString().split("(\\t)|(\\:{2})");
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
        private IntWritable one = new IntWritable(1);

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
                //wordpair.setWord(mid.toString());
                for (int j = 0; j < size; j++) {
                    String mid_2 = (String)movies.get(j);
                    if (!mid_2.equals(mid)) {
                        //wordpair.setNeighbor(mid_2.toString());
                        String pair = mid + "," + mid_2;
                        //System.out.println(pair);
                        output.collect(new Text(pair), new Text("1"));
                    }
                }
                output.collect(new Text(mid+",*"), new Text("1"));
            }
            movies.clear();
        }
    }

     public static class PairsOccurrenceMap
      extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> 
    {
    	private IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) 
                throws IOException 
        {
        		String[] tokens = value.toString().split("\t");
                int count = Integer.parseInt(tokens[1]);
				output.collect(new Text(tokens[0]), new IntWritable(count));
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
    public static class PairsConditionalMap
      extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
    {
        //private IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
                throws IOException 
        {
                String[] tokens = value.toString().split("\t");
                String[] pairs = tokens[0].split(",");
                String frequency = tokens[1];
                if (pairs.length == 2) {
                    String oneid = pairs[0];
                    String another = pairs[1];
                    StringBuilder left = new StringBuilder(another);
                    left.append(":"+frequency);
                    //output.collect(new Text(frequency), new Text(pairs));
                    output.collect(new Text(oneid), new Text(left.toString()));
                }
        }      
    }
    public static class PairsConditionalReduce extends MapReduceBase implements Reducer<Text,Text,Text,Text> 
    {
        //
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) 
               throws IOException 
        {
            int sum = 0;
            HashMap<String, String> thiskey = new HashMap<String, String>();
            while (values.hasNext()) {
                String[] left = values.next().toString().split(":"); 
                if (left.length==2){
                    if (left[0].equals("*"))
                        sum = Integer.parseInt(left[1]);
                    else thiskey.put(left[0],left[1]);
                }
            }
            Set <Map.Entry<String, String>> all = thiskey.entrySet();
            Iterator <Map.Entry<String, String>> maps = all.iterator();
            if (sum!=0) {
                while (maps.hasNext()) {
                    Map.Entry<String, String> entry = maps.next();
                    String nbr = entry.getKey();
                    String count = entry.getValue();
                    float cond = (float) Integer.parseInt(count)/ (float) sum;
                    String cd = String.format("%f", cond);
                    output.collect(new Text(nbr+"|"+key.toString()), new Text(cd));
                }
            }   
        }
    }
     public  static class ShufflePairsMap
      extends MapReduceBase implements Mapper<LongWritable, Text, FloatWritable, Text> 
    {
    	//private IntWritable one = new IntWritable(1);
        private HashMap <String, String> movies = new HashMap<String, String>();
        public void configure(JobConf conf) {
            try {
                Path[] cacheFile = DistributedCache.getLocalCacheFiles(conf);
                if (cacheFile != null && cacheFile.length > 0) {
                    String line = "";
                    BufferedReader br =  new BufferedReader(new FileReader(cacheFile[0].toString()));
                    try {
                        while ((line = br.readLine())!=null) {
                            //System.out.println(line);
                            String[] movie_info = line.split("(\\|)|(\\:{2})");
                            if(movie_info.length >= 2) {
                                String movieid = movie_info[0];
                                String moviename = movie_info[1];
                                if(!movies.containsKey(movieid)) movies.put(movieid, moviename);
                            }
                        }
                    }catch (IOException e){
                        System.out.println(e);
                    }
                    br.close();
                }
            }
            catch(IOException e) {
                System.out.println(e.toString());
            }
        }
        public void map(LongWritable key, Text value, OutputCollector<FloatWritable, Text> output, Reporter reporter) 
                throws IOException 
        {
        		String[] tokens = value.toString().split("\t");
        		String[] pairs_id = tokens[0].split("\\|");
        		String frequency = tokens[1];
        		//output.collect(new Text(frequency), new Text(pairs));
                float freq = Float.parseFloat(frequency);

                if (freq >=0.8) {
                    StringBuilder pairs_name = new StringBuilder();
                    for (int i = 0; i < pairs_id.length; i++) {
                        String mid1 = pairs_id[i];
                        String mname = movies.get(mid1);
                        pairs_name.append(mname);
                        if (i==0) pairs_name.append("|");
                    }

                    output.collect(new FloatWritable(freq), new Text(pairs_name.toString()));
                }
                /*if (freq >=0.8)
                {
				    output.collect(new FloatWritable(freq), new Text(tokens[0]));
                }*/
        }      
    }
    public static void main(String[] args) throws Exception 
    {
        //JobControl jobsctrl = new JobControl("jobs");

        //Configuration conf = new Configuration();
      
        //JobConf job = new JobConf(conf, "PairsOccurrence");
        JobConf conf1 = new JobConf(PairsConditional.class);
        JobConf conf2 = new JobConf(PairsConditional.class);
        JobConf conf3 = new JobConf(PairsConditional.class);
        JobConf conf4 = new JobConf(PairsConditional.class);
        conf1.setJobName("Prepair");
        conf2.setJobName("PairsOccurrence");
		conf3.setJobName("PairsConditional");
        conf4.setJobName("Shuffle");
        DistributedCache.addCacheFile(new Path(args[5]).toUri(), conf4);

        conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(Text.class);
        conf2.setOutputKeyClass(Text.class);//change the type for output (sum, pair) to get top frequent pairs
        conf2.setOutputValueClass(IntWritable.class);
        conf3.setOutputKeyClass(Text.class);//change the type for output (sum, pair) to get top frequent pairs
        conf3.setOutputValueClass(Text.class);
        conf4.setOutputKeyClass(FloatWritable.class);//change the type for output (sum, pair) to get top frequent pairs
        conf4.setOutputValueClass(Text.class);

        conf1.setMapperClass(PrePairMap.class);
        conf1.setReducerClass(PrePairReduce.class);

        conf2.setMapperClass(PairsOccurrenceMap.class);
        conf2.setReducerClass(PairsOccurrenceReduce.class);
        //conf2.setPartitionerClass(movieidPartition.class);
        conf3.setMapperClass(PairsConditionalMap.class);
        conf3.setReducerClass(PairsConditionalReduce.class);

        conf4.setMapperClass(ShufflePairsMap.class);
        conf4.setNumReduceTasks(1);

        conf1.setInputFormat(TextInputFormat.class);
        conf1.setOutputFormat(TextOutputFormat.class);
        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);
        conf3.setInputFormat(TextInputFormat.class);
        conf3.setOutputFormat(TextOutputFormat.class);
        conf4.setInputFormat(TextInputFormat.class);
        conf4.setOutputFormat(TextOutputFormat.class);
      
        FileInputFormat.setInputPaths(conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf1, new Path(args[1]));

        FileInputFormat.setInputPaths(conf2, new Path(args[1]));//??
        FileOutputFormat.setOutputPath(conf2, new Path(args[2]));//??

        FileInputFormat.setInputPaths(conf3, new Path(args[2]));//??
        FileOutputFormat.setOutputPath(conf3, new Path(args[3]));//??

        FileInputFormat.setInputPaths(conf4, new Path(args[3]));//??
        FileOutputFormat.setOutputPath(conf4, new Path(args[4]));//??

        Job job1 = new Job(conf1);
        Job job2 = new Job(conf2);
        Job job3 = new Job(conf3);
        Job job4 = new Job(conf4);

        //jobsctrl.addJob(job1);
        //jobsctrl.addJob(job2);
        //jobsctrl.addJob(job3);
        //jobsctrl.addJob(job4);
        job2.addDependingJob(job1);
        job3.addDependingJob(job2);
        job4.addDependingJob(job3);
        //jobsctrl.run();

        JobClient.runJob(conf1);
        JobClient.runJob(conf2);
        JobClient.runJob(conf3);
        JobClient.runJob(conf4);
        System.out.println("Job finished!!!");
        //conf.waitForCompletion(true);
    } 
}