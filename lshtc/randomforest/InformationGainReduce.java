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



public class InformationGainReduce extends Reducer<Text,Text,Text,DecisionTree>
{
	private Text output_value = new Text();
    private Text output_key = new Text();
    private List<String> expls;
    private DecisionTree treeroot;
    private HashMap<Integer, Integer> dictionary;
    
    @Override
    protected void setup(Context context) throws IOException,InterruptedException {
        //minorlabels = new HashMap<Integer, Integer>();
        dictionary = new HashMap<Integer, Integer>();
        Path [] cacheFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());

        for (int i = 0; i < cacheFile.length;i++){
            String filename = cacheFile[i].toString();
            if (filename.contains("newdict")) {
                String line = "";
                BufferedReader br =  new BufferedReader(new FileReader(cacheFile[i].toString()));
                int counter = 0;
                while((line = br.readLine())!= null) {
                    String [] elems = line.split(",");
                    for (int j = 0; j < elems.length; j++){
                    	int word = Integer.parseInt(elems[j]);
	                    dictionary.put(word, counter%2000);
	                    counter++;
                    }
                }
                br.close();
            }
        }
    }
    //find most common label in examples
    public int findMostCommonLabel(List<String> examples) {
    	HashMap<Integer, Integer> labelcount = new HashMap<Integer, Integer>();
    	for (int i = 0; i < examples.size(); i++) {
    		String[] oneinstance = examples.get(i).split("::");
            String[] doc_lbl = oneinstance[0].split(",");
            int label = Integer.parseInt(doc_lbl[1]);
            labelcount.put(label, labelcount.containsKey(label)?labelcount.get(label)+1:1);
        }
        Set <Map.Entry<Integer, Integer>> all = labelcount.entrySet();
        Iterator <Map.Entry<Integer, Integer>> maps = all.iterator();
        int max_count = 0;
        int commonlabel = 0;
        while (maps.hasNext()) {
            Map.Entry<Integer, Integer> unit = maps.next();
            int label = unit.getKey();
            int count = unit.getValue();
            if (count > max_count) commonlabel = label;
        }
        return commonlabel;
    }

    //compute Remainder for a branch of a feature;
    public ArrayList<Double> computeGain(int featureid, List<String> examples) {
    		//HashMap<Integer, Integer> hlabels = new HashMap<Integer, Integer>();
            HashMap<Integer, Integer> mlabels = new HashMap<Integer, Integer>();
            HashMap<Integer, Integer> lowlabels = new HashMap<Integer, Integer>();
            //HashMap<Integer, Integer> examples = new HashMap<Integer, Integer>();
            int allcount = 0, lcount = 0, mcount = 0;//, hcount = 0;
           
            //Set <Map.Entry<String, String>> allexpls = examples.entrySet();
            //Iterator <Map.Entry<String, String>> explsmaps = allexpls.iterator();
            //while(explsmaps.hasNext()){
            for (int i = 0; i < examples.size(); i++) {
                //Map.Entry<String, String> entry = explsmaps.next();
                String[] oneinstance = examples.get(i).split("::");
                String[] doc_lbl = oneinstance[0].split(",");
                String wclist = oneinstance[1];
                if (wclist.contains(Integer.toString(featureid))) {
                    int doc = Integer.parseInt(doc_lbl[0]);
                    int label = Integer.parseInt(doc_lbl[1]);
                    mcount++;
                    mlabels.put(label, mlabels.containsKey(label)?mlabels.get(label)+1:1);
                }
                else {
                    int doc = Integer.parseInt(doc_lbl[0]);
                    int label = Integer.parseInt(doc_lbl[1]);
                    lcount++;
                    lowlabels.put(label, lowlabels.containsKey(label)?lowlabels.get(label)+1:1);
                }
            }
            allcount += lcount;
            allcount += mcount; 
            //double gain = 0.0;
            double sumB = 0.0;
            double prob1, prob2;
            int label1 = -1, label2 = -1;
            Set <Map.Entry<Integer, Integer>> allset = lowlabels.entrySet();
            for(Map.Entry<Integer, Integer> unit: allset){
                int label = unit.getKey();
                int nofexamples = unit.getValue();
                prob1 = (double)nofexamples / (double)lcount;
                if (prob1 > 0.5) label1 = label;
                sumB += -(prob1 * Math.log(prob1));
            }
            /*Iterator <Map.Entry<Integer, Integer>> maps = all.iterator();
            
            while (maps.hasNext()) {
                Map.Entry<Integer, Integer> unit = maps.next();
                int label = unit.getKey();
                int nofexamples = unit.getValue();
                prob1 = (double)nofexamples / (double)lcount;
                if (prob1 > 0.6) label1 = label;
                sumB += -(prob1 * Math.log(prob1));
                //maps.remove();
            }*/
            double branchprob1 = (double)lcount / (double)allcount;
            sumB = sumB *  branchprob1;

            allset.clear();

            allset = mlabels.entrySet();
            for(Map.Entry<Integer, Integer> unit: allset){
                int label = unit.getKey();
                int nofexamples = unit.getValue();
                prob2 = (double)nofexamples / (double)mcount;
                if (prob2 > 0.5) label2 = label;
                sumB += -(prob2 * Math.log(prob2));
            }
            
            double branchprob2 = (double)mcount / (double)allcount;
            sumB = sumB * branchprob2;

            ArrayList<Double> result = new ArrayList<Double>();
            result.add(sumB);
            result.add((double)label1);
            result.add((double)label2);
            return result;
    }
    //get the node with minimum remainder in examples
    public ArrayList<Double> getMinNode(List<Integer> wlist, List<String> expls) {

		HashMap<Integer, Double> wordsgain = new HashMap<Integer, Double>();
        HashMap<Integer, String> wordsdecisions = new HashMap<Integer, String>();

        for (int i = 0; i < wlist.size(); i++){
            int currentword = wlist.get(i);
            ArrayList<Double> gain_lbls = computeGain(currentword, expls);
            wordsgain.put(currentword, gain_lbls.get(0));
            wordsdecisions.put(currentword, 
                Integer.toString(gain_lbls.get(1).intValue())+"," +Integer.toString(gain_lbls.get(2).intValue()));
        }
        Set <Map.Entry<Integer, Double>> allset = wordsgain.entrySet();
        Iterator <Map.Entry<Integer, Double>> allmaps = allset.iterator();
        //StringBuilder sortedgain = new StringBuilder();
        int min_word = 0;
        double min_gain = 9000.0; 
        while (allmaps.hasNext()) {
            Map.Entry<Integer, Double> entry = allmaps.next();
            int w = entry.getKey();
            double g = entry.getValue();
            if (g < min_gain) {
            	min_word = w;
            }
        }
       
        ArrayList<Double> result = new ArrayList<Double>();
        if(min_word!=0){
        	String[] lbls = wordsdecisions.get(min_word).split(",");
	        int lb1 = Integer.parseInt(lbls[0]);
	        int lb2 = Integer.parseInt(lbls[1]);
            result.add((double)min_word);
            result.add(wordsgain.get(min_word));
            result.add((double)lb1);
            result.add((double)lb2);
        }
        else {
            result.add(-1.0);
            result.add(-1.0);
            result.add(-1.0);
            result.add(-1.0);
        }
    	return result;
    }

    public List<String>  getLeftExpls(int treeid, List<String> examples){
    	List<String>  leftexpls = new ArrayList<String> ();
    	for (int i = 0; i < examples.size(); i++){
    		String[] instance = examples.get(i).split("::");
        	//Map.Entry<String, String> entry = allmaps.next();
        	String doclb = instance[0];
        	String wordlist = instance[1];

        	if (!wordlist.toString().contains(Integer.toString(treeid))) {
        		leftexpls.add(doclb+"::"+  wordlist);
        	}
        }
        //allexpls.clear();
        return leftexpls;
    }

    public List<String> getRightExpls(int treeid,  List<String> examples){
    	 List<String> rightexpls = new ArrayList<String>();
    	for (int i = 0; i < examples.size(); i++) {
        	String[] instance = examples.get(i).split("::");
        	String doclb = instance[0];
        	String wordlist = instance[1];

        	if (wordlist.toString().contains(Integer.toString(treeid))) {
        		rightexpls.add(doclb+"::"+ wordlist); //right: appear
        	}
        }
       // allexpls.clear();
        return rightexpls;
    }
    //recursively build a decision tree for given examples
   /* public void buildDecisionTree(int depth, List<Integer> wlist, List<String> examples)
    {
    	depth ++;
    	if (depth > 5) return;
    	if (gain < 1.0 || examples.size() == 1 ) {//gain > 10.0 ||gain < 0.0003 ||
    		//Set <Map.Entry<String, String>> allexpls = examples.entrySet();
	        //Iterator <Map.Entry<String, String>> allmaps = allexpls.iterator();
	       // while (allmaps.hasNext()) {
    		String[] instance = examples.get(0).split("::");

	        	//Map.Entry<String, String> entry = allmaps.next();
	        	String doclb = instance[0];
	        	String wordlist = instance[1];

	        	String[] doc_lb = doclb.split(",");
	        	if(wordlist.contains(Integer.toString(tree.getID()))) {
	        		tree.setRight(new DecisionTree.Node(-1));
	        		tree.getRight().setLabel(Integer.parseInt(doc_lb[1]));
	        	}
	        	else {
	        		tree.setLeft(new DecisionTree.Node(-1));
	        		tree.getLeft().setLabel(Integer.parseInt(doc_lb[1]));
	        	}
	    		//tree.setLabel(Integer.parseInt(doc_lb[1]));
	    		
	    	//}
	    	return;
    	}


        wlist.remove(new Integer(tree.getID()));
        if (wlist.size()==1) {
			ArrayList<Double> result = getMinNode(wlist, examples);
			int min_node = result.get(0).intValue(); 
	        double g = result.get(1);
			int lb1 = result.get(2).intValue();
	        int lb2 = result.get(3).intValue();
 			DecisionTree.Node leaf = new DecisionTree.Node(min_node);
	        DecisionTree.Node left = new DecisionTree.Node(-1);
	        left.setLabel(lb1);
            DecisionTree.Node right = new DecisionTree.Node(-1);
            right.setLabel(lb2);
            leaf.setLeft(left);
            leaf.setRight(right);
            return;
        }
        HashMap<String, String> leftexpls = getLeftExpls(tree.getID(), examples);
    	HashMap<String, String> rightexpls  = getRightExpls(tree.getID(), examples);
		ArrayList<Double> leftresult = getMinNode(wlist, leftexpls);
		ArrayList<Double> rightresult = getMinNode(wlist, rightexpls);

        int min_left = leftresult.get(0).intValue(); 
        double left_g = leftresult.get(1);
		int left_lb1 = leftresult.get(2).intValue();
        int left_lb2 = leftresult.get(3).intValue();

        int min_right = rightresult.get(0).intValue();
        double right_g = rightresult.get(1);
        int right_lb1 = rightresult.get(2).intValue();
        int right_lb2 = rightresult.get(3).intValue();

        if (min_left==-1 || min_right == -1) return;
        else {
            DecisionTree.Node left = new DecisionTree.Node(min_left);
            DecisionTree.Node right = new DecisionTree.Node(min_right);
            tree.setLeft(left);
            tree.setRight(right);
 			if (left_lb1!=-1 && left_lb2 != -1) {
 				left.setLeft(new DecisionTree.Node(-1));
            	left.getLeft().setLabel(left_lb1);
            	left.setRight(new DecisionTree.Node(-1));
            	left.getRight().setLabel(left_lb1);
            	return;
 			}

			if (right_lb1!=-1 && right_lb2 != -1) {
 				right.setLeft(new DecisionTree.Node(-1));
            	right.getLeft().setLabel(right_lb1);
            	right.setRight(new DecisionTree.Node(-1));
            	right.getRight().setLabel(right_lb2);
            	return;
 			}
            if (left_lb1!=-1 && left_lb2 == -1) {
            	left.setLeft(new DecisionTree.Node(-1));
            	left.getLeft().setLabel(left_lb1);

                wlist.remove(new Integer(min_left));
            	HashMap<String, String> r_leftexpls = getRightExpls(min_left, leftexpls);
            	ArrayList<Double> r_leftresult = getMinNode(wlist, r_leftexpls);
            	int r_min_left = r_leftresult.get(0).intValue(); 
        		double r_left_g = r_leftresult.get(1);
        		DecisionTree.Node r_left = new DecisionTree.Node(r_min_left);
        		left.setRight(r_left);
        		//wlist.add(min_left);
             	buildDecisionTree(r_left, r_left_g, wlist, r_leftexpls);
            }
            if (left_lb1 == -1 && left_lb2 != -1 ) {
            	left.setRight(new DecisionTree.Node(-1));
            	left.getRight().setLabel(left_lb1);

                wlist.remove(new Integer(min_left));
            	HashMap<String, String> l_leftexpls = getLeftExpls(min_left, leftexpls);
            	ArrayList<Double> l_leftresult = getMinNode(wlist, l_leftexpls);
            	int l_min_left = l_leftresult.get(0).intValue(); 
        		double l_left_g = l_leftresult.get(1);
        		DecisionTree.Node l_left = new DecisionTree.Node(l_min_left);
        		left.setLeft(l_left);
        		//wlist.add(min_left);
             	buildDecisionTree(l_left, l_left_g,  wlist, l_leftexpls);
            }
            if (left_lb1 == -1 && left_lb2 == -1) {
             	HashMap<String, String> l_leftexpls = getLeftExpls(min_left, leftexpls);
             	HashMap<String, String> r_leftexpls = getRightExpls(min_left, leftexpls);
                wlist.remove(new Integer(min_left));
            	ArrayList<Double> l_leftresult = getMinNode(wlist, l_leftexpls);
            	ArrayList<Double> r_leftresult = getMinNode(wlist, r_leftexpls);
            	int l_min_left = l_leftresult.get(0).intValue(); 
        		double l_left_g = l_leftresult.get(1);
        		int r_min_left = r_leftresult.get(0).intValue(); 
        		double r_left_g = r_leftresult.get(1);
        		DecisionTree.Node l_left = new DecisionTree.Node(l_min_left);
        		DecisionTree.Node r_left = new DecisionTree.Node(r_min_left);
        		//wlist.add(min_left);
        		left.setLeft(l_left);
        		left.setRight(r_left);
             	buildDecisionTree(l_left, l_left_g,  wlist, l_leftexpls);
             	buildDecisionTree(r_left, r_left_g,  wlist, r_leftexpls);
            }
            if (right_lb1!=-1 && right_lb2 == -1) {
            	right.setLeft(new DecisionTree.Node(-1));
            	right.getLeft().setLabel(right_lb1);

            	HashMap<String, String> r_rightexpls = getRightExpls(min_right, rightexpls);
                wlist.remove(new Integer(min_right));
            	ArrayList<Double> r_rightresult = getMinNode(wlist, r_rightexpls);
            	int r_min_right = r_rightresult.get(0).intValue(); 
        		double r_right_g = r_rightresult.get(1);
        		DecisionTree.Node r_right = new DecisionTree.Node(r_min_right);
        		right.setRight(r_right);
        		//wlist.add(min_right);
             	buildDecisionTree(r_right, r_right_g,  wlist, r_rightexpls);
            }
            if (right_lb2!=-1 && right_lb1 == -1) {
            	right.setRight(new DecisionTree.Node(-1));
            	right.getRight().setLabel(right_lb2);

            	HashMap<String, String> l_rightexpls = getLeftExpls(min_right, rightexpls);
                wlist.remove(new Integer(min_right));
            	ArrayList<Double> l_rightresult = getMinNode(wlist, l_rightexpls);
            	int l_min_right = l_rightresult.get(0).intValue(); 
        		double l_right_g = l_rightresult.get(1);
        		DecisionTree.Node l_right = new DecisionTree.Node(l_min_right);
        		right.setLeft(l_right);
        		//wlist.add(min_right);
             	buildDecisionTree(l_right, l_right_g, wlist,  l_rightexpls);
            }
            if (right_lb1 == -1 && right_lb2 == -1) {
            	HashMap<String, String> l_rightexpls = getLeftExpls(min_right, rightexpls);
            	HashMap<String, String> r_rightexpls = getRightExpls(min_right, rightexpls);
                wlist.remove(new Integer(min_right));
            	ArrayList<Double> l_rightresult = getMinNode(wlist, l_rightexpls);
            	ArrayList<Double> r_rightresult = getMinNode(wlist, r_rightexpls);
            	int l_min_right = l_rightresult.get(0).intValue(); 
        		double l_right_g = l_rightresult.get(1);
            	int r_min_right = r_rightresult.get(0).intValue(); 
        		double r_right_g = r_rightresult.get(1);
        		DecisionTree.Node l_right = new DecisionTree.Node(l_min_right);
        		DecisionTree.Node r_right = new DecisionTree.Node(r_min_right);
        		right.setLeft(l_right);
        		right.setRight(r_right);
        		//wlist.add(min_right);
             	buildDecisionTree(l_right, l_right_g, wlist,  l_rightexpls);
             	buildDecisionTree(r_right, r_right_g, wlist,  r_rightexpls);
            }
        }  
    }*/

    public void buildDecisionTree_recursive(DecisionTree tree, List<Integer> wlist, List<String> examples, int iteration)
    {
        DecisionTree.Node root = findNext(examples,wlist,0,iteration);
        tree.setRoot(root);
        return;
    }

    public DecisionTree.Node findNext(List<String> examples, List<Integer> wlist, int depth, int iteration){

        //fin min
    	//System.out.println("example size:" + examples.size());
    	//System.out.println("feature size:" + wlist.size());
        List<Double> result = getMinNode(wlist, examples);
        int wordid = result.get(0).intValue();
        //System.out.println("current node:"+wordid);
        double gain = result.get(1);
        int lb_left = result.get(2).intValue();
        int lb_right = result.get(3).intValue();
        DecisionTree.Node node = new DecisionTree.Node(wordid);
        //split 
        List<String> examples_left = getLeftExpls(wordid,examples);
        List<String> examples_right = getRightExpls(wordid,examples);

        //remove wlist
        List<Integer> wlist_copy = new ArrayList<Integer>();
        for (Integer i: wlist) {
            if(i.intValue()!=wordid){
                wlist_copy.add(i);
            }
        }


        if (depth > iteration || wlist_copy.size() == 0 ) {
            //find most common label in left
            node.setLeftLabel(findMostCommonLabel(examples_left));
            //find most common label in right
            node.setRightLabel(findMostCommonLabel(examples_right));

            return node;
        }

        if (examples_left.size() < 10 || lb_left != -1) {
            //find most common label in left
            if (lb_left!=-1)
                node.setLeftLabel(lb_left);
            else
                node.setLeftLabel(findMostCommonLabel(examples_left));
        }
        else {
            node.setLeft(findNext(examples_left,wlist_copy,depth+1,iteration));
        }

        if (examples_right.size() < 10 || lb_right!=-1) {
            //find
            if (lb_right!=-1)
                node.setRightLabel(lb_right);
            node.setRightLabel(findMostCommonLabel(examples_right));
        }
        else {
            node.setRight(findNext(examples_right,wlist_copy,depth+1,iteration));
        }

        return node;
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
                        throws IOException, InterruptedException
    {
    	expls = new ArrayList<String> ();
        for(Text value : values) {
            //String[] varr = value.toString().split("::");

            expls.add(value.toString());
        }
        List<Integer> wordlist = new ArrayList<Integer>();
        Set <Map.Entry<Integer, Integer>> allwords = dictionary.entrySet();
        Iterator <Map.Entry<Integer, Integer>> allmaps = allwords.iterator();
        while (allmaps.hasNext()) {
            Map.Entry<Integer, Integer> entry = allmaps.next();
            int word = entry.getKey();
            int idx = entry.getValue();
            if (idx == Integer.parseInt(key.toString()))
            	wordlist.add(word);
        }

        //ArrayList<Double> rootresult = getMinNode(wordlist, expls);//wordsinfo);
        //int min_root = rootresult.get(0).intValue();
        //double root_gain = rootresult.get(1);
        /*DecisionTree.Node rt = buildDecisionTree(0, wordlist, expls);
        buildDecisionTree_recursive()
        treeroot = new DecisionTree(rt.getID());
        treeroot.setRoot(rt);
        */
        treeroot = new DecisionTree(Integer.parseInt(key.toString()));
        buildDecisionTree_recursive(treeroot,wordlist,expls,5);
        //System.out.println(treeroot.toString());
        context.write(key, treeroot);
    }
}