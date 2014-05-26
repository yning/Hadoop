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
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

/**
 * Handle test data set: 
 * key = wordid
 * value = docid_i,,labelset_i
 */
public class TestDataMapper extends Mapper<LongWritable,Text,Text,Text>
{
    private Text output_value = new Text();
    private Text output_key = new Text();
    private StringBuilder labels;
    private HashMap<Integer, Integer> word_count;

    @Override
    public void map(LongWritable key, Text value, Context context) 
                    throws IOException, InterruptedException
    {
    	int i,j;
        labels = new StringBuilder();
        word_count = new  HashMap<Integer, Integer>();
        int numOfwords = 0;
        if(!value.toString().contains("Data")){
            String[] tokens = value.toString().split("(\\s)|(,\\s)");
            for (i = 0; i < tokens.length; i++){      
                if (!tokens[i].contains(":")) {
                    String newtoken = tokens[i];
                    labels.append(newtoken+",");
                }
                else {
                    String[] temp = tokens[i].split(":");
                    if(temp.length == 2){
                        numOfwords += Integer.parseInt(temp[1]);
                        word_count.put(Integer.parseInt(temp[0]), Integer.parseInt(temp[1]));
                    }
                }
            }
            String labellist = labels.toString();
            labellist = labellist.substring(0, labellist.length() - 1);
            for (Integer wordid: word_count.keySet()){
                int count = word_count.get(wordid);
                output_key.set(Integer.toString(wordid));
                //for (j = 0; j < count; j++){
                String outputstr = String.format("%s++%s++%s", key.get(), labellist, count);
                output_value.set(outputstr);
                context.write(output_key, output_value);
                //}
            }
        }
    }
}