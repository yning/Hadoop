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



public class InvertedIndexReduce extends Reducer<Text,Text,Text,Text>
{
    public static class ValueComparator implements Comparator<Integer>
    {
        Map<Integer, Double> base;
        public ValueComparator(Map<Integer, Double> base){
            this.base = base;
        }
        public int compare(Integer a, Integer b){
            if(base.get(a) >= base.get(b)){
                return -1;
            }
            else return 1;
        }
    }
	private Text output_value = new Text();
    private Text output_key = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
                        throws IOException, InterruptedException
    {
		String keystr = key.toString();
		int allfreq = 0,j;
		HashMap<Integer, Integer> cooccur = new HashMap<Integer, Integer>();
        HashMap<Integer, Double> logprob = new HashMap<Integer, Double>();
		HashMap<Integer, Integer> wcounts = new HashMap<Integer, Integer>();
    	if (!keystr.contains("#")){
			for(Text value : values) {
    			if (value.toString().contains(",")){
    				String[] temp = value.toString().split(",");
	            	if(temp.length == 3) {
	            		int label = Integer.parseInt(temp[0]);
	            		int cooccurence = Integer.parseInt(temp[1]);
	            		int wordscount = Integer.parseInt(temp[2]);
	            		cooccur.put(label, 
	            			cooccur.containsKey(label)?cooccur.get(label)+cooccurence:cooccurence);
	            		if (!wcounts.containsKey(label))
                            wcounts.put(label,wordscount);
	            	}
    			}
        	}
    	}
        for (Integer label: cooccur.keySet()){
            int freq = cooccur.get(label);
            int allcount = wcounts.get(label);
            float condprob = (float) (freq+1) / (float) allcount;
            Double log_cprob = Math.log(condprob);
            logprob.put(label, log_cprob);
        }
    	ValueComparator bvc = new ValueComparator(logprob);
		TreeMap<Integer, Double> sortedmap = new TreeMap<Integer, Double>(bvc);
		sortedmap.putAll(logprob);
        Set <Map.Entry<Integer, Double>> all = sortedmap.entrySet();
        Iterator <Map.Entry<Integer, Double>> maps = all.iterator();
        StringBuilder invertedindex = new StringBuilder();
    	while (maps.hasNext()) {
            Map.Entry<Integer, Double> entry = maps.next();
            int label = entry.getKey();
            double logcp = entry.getValue();
           	invertedindex.append(label + ":" + Double.toString(logcp) + ",");
        }
       
        int size = invertedindex.length();
		invertedindex.deleteCharAt(size-1);
    	output_key.set(keystr);
        output_value.set(invertedindex.toString());
        context.write(output_key,output_value);
    }
}