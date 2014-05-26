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


//input from the first mapred job, get the inverted index for each feature
public class InvertedIndexMap extends Mapper<LongWritable,Text,Text,Text>
{
	private Text output_value = new Text();
	private Text output_key = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) 
		        throws IOException, InterruptedException
	{
		String[] arr = value.toString().split("\t");
		if (arr.length == 2 && !arr[0].equals("#")){
			output_key.set(arr[0]);
			output_value.set(arr[1]);
			context.write(output_key,output_value);
		}
	
	}
}
