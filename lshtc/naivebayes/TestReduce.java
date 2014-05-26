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
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

// join the training model and test data per word
public class TestReduce extends Reducer<Text,Text,Text,Text>
{
	private Text output_value = new Text();
    private Text output_key = new Text();
   

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
                        throws IOException, InterruptedException
    {
        String modelLine = null;
        ArrayList<String> documents = new ArrayList<String>();
        for (Text value : values) {
            String line = value.toString();
            if (line.contains(":")) {
                // Line is from the model.
                modelLine = line;
            } else {
                // Contains the document ID with list of labels.
                documents.add(line);
            }
        }
        
        if (documents.size() > 0) {
            // The only words in the training set we care about are those
            // which appear in the testing set as well. If they don't appear
            // in the testing set...sorry brah.
            if (modelLine == null) {
                modelLine = "";
            }
            StringBuilder output = new StringBuilder();
            output.append(String.format("%s::", modelLine));
            for (String doc : documents) {
                output.append(String.format("%s::", doc));
            }
            String out = output.toString();
            context.write(key, new Text(out.substring(0, out.length() - 2)));
        }
        documents.clear();
    }
/*
 * Outputs: 
 * key = wordid
 * value = label_i:condprob_i,...,label_j:condprob_j::docid_i,,labelset_i::...::docidj,,labelset_j
 * label_i:condprob_i,...,label_j:condprob_j THIS PART IS FROM TRAINING MODEL
 * docid_i,,labelset_i::...::docidj,,labelset_j THIS PART IS FROM TEST DOCUMENT
 */
}