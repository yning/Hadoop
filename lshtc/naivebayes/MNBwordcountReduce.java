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
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.filecache.DistributedCache;

 
 //compute conditional probabilities of each feature given each label
 public class MNBwordcountReduce extends Reducer<Text,Text,Text,Text>
{
    private Text output_value = new Text();
    private Text output_key = new Text();
   
  
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
                        throws IOException, InterruptedException
    {
        int i, j;
    	int alllabelcount = 0;
    	String label = key.toString();
    	if (label.equals("all")){
    		for(Text value : values) {
    			alllabelcount += Integer.parseInt(value.toString());
    		}
    		output_key.set("#");
            output_value.set(Integer.toString(alllabelcount));
            context.write(output_key,output_value);
    	}
    	else {
    		int labelfreq = 0;
        	HashMap<Integer, Integer> wordlist = new HashMap<Integer, Integer> ();
        	int allcount = 0;
            for(Text value : values) {
            	String[] temp = value.toString().split(",");
            	if(temp.length >= 2){
            		String word = temp[0];

            		int count = Integer.parseInt(temp[1]);
            		if (word.equals("#")) allcount += count;
            		else if (word.contains("c")) {
            			labelfreq += count;
            			//alllabelcount += Integer.parseInt(temp[2]);
            		}
            		else {
            			wordlist.put(Integer.parseInt(word), wordlist.containsKey(word)? wordlist.get(word)+count:count);
            		}
            	}
            }
             //output count of words that didn't appear in this label
            /*for (j = 0; j < dict.size();j++){
                int wordid = dict.get(j);
                //if(!wordlist.containsKey(wordid)){
                    output_key.set(Integer.toString(wordid));
                    output_value.set(label+",1,"+Integer.toString(allcount));
                    context.write(output_key,output_value);
                //}
            }*/
            output_key.set("#");
            output_value.set(label+","+Integer.toString(labelfreq));
            context.write(output_key,output_value);

            Set <Map.Entry<Integer, Integer>> all = wordlist.entrySet();
	        Iterator <Map.Entry<Integer, Integer>> maps = all.iterator();
        	while (maps.hasNext()) {
                Map.Entry<Integer, Integer> entry = maps.next();
                int w = entry.getKey();
                int count = entry.getValue();
                //float condprob = (float)(count + 1)/ (float)allcount;
                //float log_cp = (float)Math.log(condprob);
               	output_key.set(Integer.toString(w));
	            output_value.set(label+","+Integer.toString(count)+","+Integer.toString(allcount));
	            context.write(output_key,output_value);
            }
    	}	
    }
}