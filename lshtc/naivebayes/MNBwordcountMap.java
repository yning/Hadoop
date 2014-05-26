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

import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;

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

public class MNBwordcountMap extends Mapper<LongWritable,Text,Text,Text>
{
    private Text output_value = new Text();
    private Text output_key = new Text();

    
    @Override
    public void map(LongWritable key, Text value, Context context) 
                    throws IOException, InterruptedException
    {
        int i, j;
        List<Integer> labels = new ArrayList<Integer>();
        HashMap<Integer, Integer> word_count = new HashMap<Integer, Integer>();
        if(!value.toString().contains("Data")){
            String[] tokens = value.toString().split("(\\s)|(,\\s)");
            for (i = 0; i < tokens.length; i++){      
                if (!tokens[i].contains(":")) {
                	String newtoken = tokens[i];
                    labels.add(Integer.parseInt(newtoken));
                }
                else {
                    String[] temp = tokens[i].split(":");
                    if(temp.length == 2)
                        word_count.put(Integer.parseInt(temp[0]),Integer.parseInt(temp[1]));
                }
            }
            Set <Map.Entry<Integer, Integer>> all = word_count.entrySet();
            Iterator <Map.Entry<Integer, Integer>> maps = all.iterator();
            
            for (i = 0; i < labels.size();i++){
            	int allcount = 0;
            	int label = labels.get(i);
            	while (maps.hasNext()) {
                    Map.Entry<Integer, Integer> entry = maps.next();
                    int w = entry.getKey();
                    int count = entry.getValue();
                   	allcount += count;
                   	output_key.set(Integer.toString(label));
		            output_value.set(Integer.toString(w)+","+Integer.toString(count));
		            context.write(output_key,output_value);
                }
                
               
                output_key.set(Integer.toString(label));
	            output_value.set("#,"+Integer.toString(allcount));
	            context.write(output_key,output_value);

	            //for label prior probability
	            output_key.set(Integer.toString(label));
	            output_value.set("c,1");
	            context.write(output_key,output_value);        
            }
            output_key.set("all");
            //output_value.set(Integer.toString(labels.size()));
            output_value.set("1");
            context.write(output_key,output_value);   
    	}
	}
}