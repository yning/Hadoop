/* ************************ *
*         Yue Ning          *
*     Partner: Xin Guan     *
*          CS 757           *
*      Course Project       *
* ************************* */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.DataInput;   
import java.io.DataOutput; 
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Generates a probability list for one document give by current word and 
 * conditional probabilities under all labels.
 * Inputs: 
 * key = wordid
 * value = label_i:condprob_i,...,label_j:condprob_j::docid_i:labelset_i, docidj:labelset_j
 */
public class ClassifyMap extends Mapper<Text, Text, Text, Text> 
{
    @Override
    public void map(Text key, Text value, Context context) 
            throws InterruptedException, IOException {
        String [] elements = value.toString().split("(\\:{2})");
        String model = elements[0];
        
        // Create a HashMap for the labels and conditional probabilities.
        HashMap<Integer, Float> modelProbs = null; //<label, conditional prob>
        HashMap<Long, Integer> docCount = new HashMap<Long, Integer>();
       // HashMap<Long, Integer> docSize = new HashMap<Long, Integer>();
        if (model.length() > 0) {
            String [] labelProbs = model.split(",");
            modelProbs = new HashMap<Integer, Float>();
            for (String labelprob : labelProbs) {
                String [] elems = labelprob.split(":");
                modelProbs.put(Integer.parseInt(elems[0]), Float.parseFloat(elems[1]));
            }
        
        
	        // Get the true lables for each documents
	        // HashMap<Long, Integer> multipliers = new HashMap<Long, Integer>();
	        HashMap<Long, String> trueLabels = new HashMap<Long, String>();
	        for (int i = 1; i < elements.length; ++i) {
	            String [] elems = elements[i].split("(\\+{2})");
	            Long docId = Long.parseLong(elems[0]);
	            int count =  Integer.parseInt(elems[2]);
	            //int docsize = Integer.parseInt(elems[2]);
	            //docSize.put(docId, docsize);
	            docCount.put(docId, count);
	            if (!trueLabels.containsKey(docId)) {
	                String outval = elems[1];
	                trueLabels.put(docId, outval);
	            }
	        }

	        
	        // get conditional probabilites of this word giver all lables
	        for (Long docId : trueLabels.keySet()) {
	            StringBuilder probs = new StringBuilder();
	            int thiscount = docCount.get(docId);
	            //for (Integer label : candidatelabels) {
	            int i = 0, j = 0;
	            for (Integer label : modelProbs.keySet()) {
	            	if (i>30) break;
	            	//if (modelProbs.containsKey(label)) {
	                float wordgivenlabel = (float)thiscount * modelProbs.get(label);
	                probs.append(String.format("%s:%s,", label, wordgivenlabel));
	            	//}
	            	i++;
	            }
	            // Output the document ID, followed by the value containing:
	            // probs: list of "<label:probability>," statements for the word
	            // trueLabels: list of "<truelabel>:" statements for each true label of the document
	            if (probs.length() > 0) {
	            	String output = probs.toString();
		            output = output.substring(0, output.length() - 1);
		            context.write(new Text(Long.toString(docId)), 
		                    new Text(String.format("%s::%s::%s", output, trueLabels.get(docId), thiscount)));
		        }
	        }
	        //trueLabels.clear();
	        //modelProbs.clear();
	    }
    }
}
