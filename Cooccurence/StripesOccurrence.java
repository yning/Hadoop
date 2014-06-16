import java.io.IOException;
import java.util.*;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
//import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Partitioner;
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.jobcontrol.Job;
//import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

//import java.util.logging.Level;
//import java.util.logging.FileHandler;
//import java.util.logging.SimpleFormatter;
//import java.io.ByteArrayOutputStream;
//import java.io.DataOutputStream;
import java.util.TreeMap;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class StripesOccurrence 
{

    public  static class PreStripesMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
    {
        private Text word = new Text();
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
                throws IOException 
        {
            String[] tokens = value.toString().split("(\t)|(\\:{2})");
            if (tokens.length >= 3) {
                Float rating = Float.parseFloat(tokens[2]);
                String mid = tokens[1];
                if(rating >= 4.0f) 
                    output.collect(new Text(tokens[0]), new Text(mid));
            }
        }      
    }
    public static class PreStripesReduce extends MapReduceBase implements Reducer<Text,Text,Text,Text> 
    {
        //private IntWritable one = new IntWritable(1);
        //private WordPair wordPair = new WordPair();
        //private WordPair wordpair = new WordPair();
        //ArrayList movies = new ArrayList();

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) 
        	     throws IOException 
        {
        	ArrayList movies = new ArrayList();
            while (values.hasNext()) {
              Text movieid = values.next();
              movies.add(movieid.toString());
            }
            int size = movies.size();
            for (int i = 0; i < size; i++) {
                String mid = (String)movies.get(i);
				StringBuilder stripes = new StringBuilder();
                for (int j = 0; j < size; j++) {
	                String mid_2 = (String)movies.get(j);
	                if (!mid_2.equals(mid)) {
	                    //wordpair.setNeighbor(mid_2.toString());
	                    //String pair = mid + "," + mid_2;
	                    if (j==size-1)
	                    	stripes.append(mid_2);
	               		else 
	               			stripes.append(mid_2+",");
	                    //System.out.println(pair);        
	                }
                }
                output.collect(new Text(mid.toString()), new Text(stripes.toString()));
                //output(key, value): A\tB,C,D...
            }
            movies.clear();
        }
    }
    public static class movieidPartition implements Partitioner<Text, IntWritable>
    {
    	@Override
    	public int getPartition(Text key, IntWritable value, int numReduceTasks)
    	{
    		String[] tokens = key.toString().split(":");
    		String movieid = tokens[0];
    		int mid = Integer.parseInt(movieid);
    		return mid%256;
    	}
    	@Override
    	public void configure(JobConf arg0){

    	}
    }
     public  static class StripesOccurrenceMap
      extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
    {
    	private IntWritable one = new IntWritable(1);
    	//private MapWritable occurrenceMap = new MapWritable();
    	private Text word = new Text();
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
                throws IOException 
        {
    			String[] oneline = value.toString().split("\t");
    			if (oneline.length == 2) {
	    			String current = oneline[0];
	    			String others = oneline[1];
	    			//String[] others = oneline[1].split(",");
	    			output.collect(new Text(current), new Text(others));
	    		}
        }      
    }
    public static class StripesOccurrenceReduce extends MapReduceBase implements Reducer<Text,Text,Text,Text> 
    {
    	//private MapWritable occurrenceMap = new MapWritable();
    	private Text word = new Text();
    	//private static final Logger LOGFILE = Logger.getLogger(StripesOccurrenceReduce.class.getName());
    	//private static FileHandler filetxt;
    	//private static SimpleFormatter formattertxt;
    	@Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) 
               throws IOException 
        {
        	//occurrenceMap.clear();	
        	HashMap<String, Integer> stripes = new HashMap<String, Integer>();

        	word.set(key.toString());
        	StringBuilder results = new StringBuilder();
    		while (values.hasNext()) {
    			Text temp = values.next();
    			String[] movieids = temp.toString().split(",");
    			for (int i = 0; i < movieids.length; i++) {
    				String neighbor = movieids[i];
    				if (stripes.containsKey(neighbor)) {
    					Integer count = stripes.get(neighbor);
    					count = count + 1;
    					//stripes.remove(neighbor);
    					stripes.put(neighbor, count);
    					//LOGFILE.info(word.toString()+","+neighbor+"\t"+count.toString());
    				}
    				else {
    					stripes.put(neighbor, 1);
    				}
    			}
    		}
    		ValueComparator bvc = new ValueComparator(stripes);
    		TreeMap<String, Integer> sortedmap = new TreeMap<String, Integer>(bvc);
    		sortedmap.putAll(stripes);
    		Set <Map.Entry<String, Integer>> all = sortedmap.entrySet();
    		Iterator <Map.Entry<String, Integer>> maps = all.iterator();
    		while(maps.hasNext()){
    			Map.Entry<String, Integer> entry = maps.next();
    			String nbr = entry.getKey();
    			Integer count = entry.getValue();
    			results.append(nbr+":"+count.toString()+",");
    		}
    		//String newrs = results.toString();

    		int size = results.length();
    		results.deleteCharAt(size-1);
    		output.collect(word, new Text(results.toString()));
    		stripes.clear();
    		all.clear();

        }
    }
    public static class ValueComparator implements Comparator<String>{
    	Map<String, Integer> base;
    	public ValueComparator(Map<String, Integer> base){
    		this.base = base;
    	}
    	public int compare(String a, String b){
    		if(base.get(a) >= base.get(b)){
    			return -1;
    		}
    		else return 1;
    	}
    }
    
    
    public  static class ShuffleStripesMap
      extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> 
    {
    	//private IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) 
                throws IOException 
        {
        	String[] tokens = value.toString().split("\t");
        	String movieid = tokens[0];
        	String[] neighbors = tokens[1].split(",");
        	for (int i = 0; i < neighbors.length;i++){
	        	String[] nbrs_count = neighbors[i].split(":");//get the maximum count from the sorted hashmap

				String nbrs = nbrs_count[0];
	        	int frequency = Integer.parseInt(nbrs_count[1]);
	        	//output.collect(new Text(frequency), new Text(pairs));
				output.collect(new IntWritable(frequency), new Text(movieid+","+nbrs));
        	}
        }      
    }

    public static void main(String[] args) throws Exception 
    {
        JobControl jobsctrl = new JobControl("jobs");

        JobConf conf1 = new JobConf(StripesOccurrence.class);
        JobConf conf2 = new JobConf(StripesOccurrence.class);
        JobConf conf3 = new JobConf(StripesOccurrence.class);
        conf1.setJobName("PreStripes");
        conf2.setJobName("StripesOccurrence");
		conf3.setJobName("topfrequent");

        conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(Text.class);
        conf2.setOutputKeyClass(Text.class);//change the type for output (sum, pair) to get top frequent pairs
        conf2.setOutputValueClass(Text.class);
        conf3.setOutputKeyClass(IntWritable.class);//change the type for output (sum, pair) to get top frequent pairs
        conf3.setOutputValueClass(Text.class);

        conf1.setMapperClass(PreStripesMap.class);
        conf1.setReducerClass(PreStripesReduce.class);

        conf2.setMapperClass(StripesOccurrenceMap.class);
        conf2.setReducerClass(StripesOccurrenceReduce.class);

        conf3.setMapperClass(ShuffleStripesMap.class);
        conf3.setNumReduceTasks(1);

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
        //conf2.setPartitionerClass(movieidPartition.class);
       // jobsctrl.addJob(job1);
        //jobsctrl.addJob(job2);
        //jobsctrl.addJob(job3);
        job2.addDependingJob(job1);
        job3.addDependingJob(job2);
        //jobsctrl.run();

        JobClient.runJob(conf1);
        JobClient.runJob(conf2);
        JobClient.runJob(conf3);
        //conf.waitForCompletion(true);
        System.out.println("Job finished!!!");
    } 
}