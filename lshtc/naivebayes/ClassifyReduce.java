/* ************************ *
*         Yue Ning          *
*     Partner: Xin Guan     *
*          CS 757           *
*      Course Project       *
* ************************* */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.Random;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.DataInput;   
import java.io.DataOutput; 
import java.nio.channels.FileChannel;
import java.lang.*;
import java.lang.Math;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Sums the log probabilities for the document.
 */
public class ClassifyReduce extends
        Reducer<Text, Text, Text, Text> {
  	public static class ValueComparator implements Comparator<Integer>
    {
        Map<Integer, Float> base;
        public ValueComparator(Map<Integer, Float> base){
            this.base = base;
        }
        public int compare(Integer a, Integer b){
            if(base.get(a) >= base.get(b)){
                return -1;
            }
            else return 1;
        }
    }

    private HashMap<Integer, Float> priors;
    @Override
    protected void setup(Context context) throws IOException,InterruptedException {
        priors = new HashMap<Integer, Float>();
        Path [] cacheFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        for (int i = 0; i < cacheFile.length;i++){
            String filename = cacheFile[i].toString();
            if (!filename.contains("labelset")){
                String line = "";
                BufferedReader br =  new BufferedReader(new FileReader(cacheFile[i].toString()));
                while((line = br.readLine())!= null) {
                    String [] elems = line.split("\t");
                    int label = Integer.parseInt(elems[0]);
                    float prior = Float.parseFloat(elems[1]);
                    priors.put(label, prior);
                }
                br.close();
            }
        } 
    }
    public static int factorial(int n) {
        int fact = 1; // this  will be the result
        for (int i = 1; i <= n; i++) {
            fact *= i;
        }
        return fact;
    }
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
            throws InterruptedException, IOException 
    {
        // Lots of metadata.
        int i,j;
        HashMap<Integer, Float> probabilities = new HashMap<Integer, Float>();
        String trueLabels = "";
        int docsize = 0;
        List<Integer> features = new ArrayList<Integer>();
        HashMap<Integer, Integer> labelwc = new HashMap<Integer, Integer>();
        for (Text value : values) {
            // Each value is a list of label probabilities for a single word.
            String [] elements = value.toString().split("::");
            String [] labelProbs = elements[0].split(",");
            for (String labelProb : labelProbs) {
                String [] pieces = labelProb.split(":");
                int label = Integer.parseInt(pieces[0]);
                float prob = Float.parseFloat(pieces[1]);
                int currentcount = Integer.parseInt(elements[2]);//one word count in a document
                features.add(currentcount);
                labelwc.put(label, labelwc.containsKey(label) ? labelwc.get(label) + currentcount : currentcount);
                probabilities.put(label, new Float(
                        probabilities.containsKey(label) ? probabilities.get(label)+ prob : prob));
            }
            trueLabels = elements[1];
            docsize += Integer.parseInt(elements[2]);
            
        }
       
        for (Integer label : labelwc.keySet()){
            int numOfWords = labelwc.get(label);
            if (numOfWords < docsize){
                probabilities.put(label, 
                    probabilities.get(label)- ((float)Math.log(1.0/100.0)*(float)(docsize - numOfWords)));
            }
            //probabilities.put(label, probabilities.get(label)+ log_factor);
        }
        // Find the most likely labels

        for (int label : probabilities.keySet()) {
            if (priors.containsKey(label)){
                float prior = priors.get(label);
                float totalProb = probabilities.get(label) + prior;
                probabilities.put(label, totalProb);
            }  
        }

        StringBuilder truestr = new StringBuilder(trueLabels);
        StringBuilder predictedstr = new StringBuilder();
		ValueComparator bvc = new ValueComparator(probabilities);
		TreeMap<Integer, Float> sortedmap = new TreeMap<Integer, Float>(bvc);
		sortedmap.putAll(probabilities);
        Set <Map.Entry<Integer, Float>> all = sortedmap.entrySet();
        Iterator <Map.Entry<Integer, Float>> maps = all.iterator();
        //int truelabelsize = trueLabels.split(",").length;
        i = 0;
        float threshold = -20.0f;
        while (maps.hasNext()) {
            if (i == 0) {
                Map.Entry<Integer, Float> entry = maps.next();
                int label = entry.getKey();
                float condprob = entry.getValue();
                predictedstr.append(Integer.toString(label)+",");
                threshold = condprob - 20.0f; //+(float)Math.log(0.6);
                i++;
            }
        	else {
        		Map.Entry<Integer, Float> entry = maps.next();
	            int label = entry.getKey();
	            float condprob = entry.getValue();
                if (condprob > threshold && i < 20) {
                    predictedstr.append(Integer.toString(label)+",");
                    i++;
                } 
                else break;
        	}
           	
        }
       
        if(truestr.length() > 0 && predictedstr.length() > 0){
            Text trueout = new Text(truestr.toString());//.substring(0, truestr.length()-1)
            Text predictedout = new Text(predictedstr.toString().substring(0, predictedstr.length()-1));
            context.write(trueout, predictedout);
            //probabilities.clear();
            //priors.clear();
            //trueLabels.clear();
            //predictedlables.clear();
        } 
    }
}
