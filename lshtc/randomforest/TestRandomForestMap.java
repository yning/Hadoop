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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestRandomForestMap extends Mapper<Text,DecisionTree,Text,Text>
{
    private Text output_value = new Text();
    private Text output_key = new Text();
    private HashMap<Integer, String> doc_words = new HashMap<Integer, String>();
    private HashMap<Integer, String> doc_labels = new HashMap<Integer, String>();
    @Override
    protected void setup(Context context) throws IOException,InterruptedException {
        //doc_words = new HashMap<Integer, String>();
        //doc_labels = new HashMap<Integer, String>();
        Path [] cacheFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());

        for (int i = 0; i < cacheFile.length;i++) {
            String filename = cacheFile[i].toString();
            String line = "";
            BufferedReader br =  new BufferedReader(new FileReader(cacheFile[i].toString()));
            int counter = 0;
            while((line = br.readLine())!= null) {
                String [] elems = line.split("(\\t)|(\\s)");
                if (elems.length == 2) {
                	doc_labels.put(counter, elems[0]);
	                doc_words.put(counter, elems[1]);
	                counter++;
                }       
            }
            br.close();
        }
    }
    @Override
    public void map(Text key, DecisionTree dtree, Context context) 
                    throws IOException, InterruptedException
    {

    	//dtree.readFields();
        //DecisionTree mytree = dtree.get();
        //String outd = dtree.toString();
        //System.out.println("Tree id :" + key.toString());
        //System.out.println("Tree :" + outd);
        //DecisionTree mytree = dtree;
        Set<Map.Entry<Integer, String>> all = doc_words.entrySet();
        Iterator <Map.Entry<Integer, String>> maps = all.iterator();
        int max_count = 0;
        int commonlabel = 0;
        while (maps.hasNext()) {
            Map.Entry<Integer, String> unit = maps.next();
            int docid = unit.getKey();
            String words = unit.getValue();
            int predictedLbl= dtree.predict(words);
            output_key.set(Integer.toString(docid)+":"+doc_labels.get(docid));
	        output_value.set(key.toString()+":"+predictedLbl);
	        context.write(output_key, output_value);
        }
	}
}