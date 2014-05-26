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
//import DecisionTree.Node;

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



public class TestRandomForestReduce extends Reducer<Text,Text,Text,Text>
{
    private Text output_value = new Text();
    private Text output_key = new Text();
	
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
                        throws IOException, InterruptedException
    {
        HashMap<Integer, Integer> labelcount = new  HashMap<Integer, Integer>();
        String[] docid_reallabels = key.toString().split(":");
        int docid = Integer.parseInt(docid_reallabels[0]);
        String reallabels = docid_reallabels[1];
        int sumoflabels = 0;
    	List<Integer> predicted = new ArrayList<Integer>();
        for(Text value : values) {
            //String[] varr = value.toString().split("::");
            String[] valuestr = value.toString().split(":");
            int rfid = Integer.parseInt(valuestr[0]);
            int label = Integer.parseInt(valuestr[1]);
            sumoflabels++;
            labelcount.put(label, labelcount.containsKey(label)?labelcount.get(label)+1:1);
        }

        Set <Map.Entry<Integer, Integer>> labels = labelcount.entrySet();
        Iterator <Map.Entry<Integer, Integer>> allmaps = labels.iterator();
        while (allmaps.hasNext()) {
            Map.Entry<Integer, Integer> entry = allmaps.next();
            int lbl = entry.getKey();
            int count = entry.getValue();
            if (count > (int)(0.4*sumoflabels))
            	predicted.add(lbl);
        }
        StringBuilder plabels = new StringBuilder();
        for (Integer lbl : predicted) {
            plabels.append(Integer.toString(lbl)+",");
        }
        String plabelstr = plabels.toString();
        plabelstr = plabelstr.substring(0, plabelstr.length()-1);
        output_key.set(Integer.toString(docid));
        output_value.set(reallabels+":"+plabelstr);
        context.write(output_key, output_value);
    }
}