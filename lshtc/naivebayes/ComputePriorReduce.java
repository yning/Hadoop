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
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class ComputePriorReduce extends Reducer<Text,Text,Text,Text>
{
	private Text output_value = new Text();
    private Text output_key = new Text();
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
                        throws IOException, InterruptedException
    {
		String keystr = key.toString();
		int allfreq = 0;
		HashMap<String, Integer> labellist = new HashMap<String, Integer>();
    	if (keystr.contains("#")){
    		for(Text value : values) {
    			if (value.toString().contains(",")){
    				String[] temp = value.toString().split(",");
	            	if(temp.length ==2){
	            		String label = temp[0];
	            		int lfreq = Integer.parseInt(temp[1]);
	            		labellist.put(label,lfreq);
	            	}
    			}
            	else 
            		allfreq = Integer.parseInt(value.toString());
        	}
        	if (allfreq!=0) {
            	Set <Map.Entry<String, Integer>> all = labellist.entrySet();
		        Iterator <Map.Entry<String, Integer>> maps = all.iterator();
	        	while (maps.hasNext()) {
	                Map.Entry<String, Integer> entry = maps.next();
	                String label = entry.getKey();
	                int lfreq = entry.getValue();
	                float prior = (float)(lfreq + 1) / (float) allfreq;
                    float log_prior = (float) Math.log(prior);
	               	output_key.set(label);
		            output_value.set(Float.toString(log_prior));
		            context.write(output_key,output_value);
	            }
			}

    	}
    }
}