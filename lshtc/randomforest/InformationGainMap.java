/* ************************ *
*         Yue Ning          *
*     Partner: Xin Guan     *
*    CS 757  Spring 2014    *
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

public class InformationGainMap extends Mapper<LongWritable,Text,Text,Text>
{
    private Text output_value = new Text();
    private Text output_key = new Text();
    private HashMap<Integer, Integer> word_count = new HashMap<Integer, Integer>();
    @Override
    public void map(LongWritable key, Text value, Context context) 
                    throws IOException, InterruptedException
    {
       	int i,j;
       	//List<String> labels = new ArrayList<String>();
       	String[] tokens = value.toString().split("\t");
        String[] lbs = tokens[0].split(",");
        String wc = tokens[1];
        for (j = 0; j < lbs.length; j++){
        	int index = ((int)key.get()%2000);
        	String doclbl = String.format("%s,%s", key.get(),lbs[j]);

            String ostr = doclbl+"::" + wc;
            //ostr = ostr.substring(0, ostr.length()-1);
            output_key.set(Integer.toString(index));
            output_value.set(ostr);
            context.write(output_key, output_value);
        }
	}
}