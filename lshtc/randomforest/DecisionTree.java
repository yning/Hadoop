import java.io.DataInput;   
import java.io.DataOutput;   
import java.io.IOException;  
import java.io.EOFException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;  
import java.util.ArrayList;   
import java.util.Collection;      
import java.util.List;
import java.util.Map;
import java.util.HashMap; 
import java.util.Random;
import java.lang.Math;
import java.util.Set;
import java.util.HashSet;
  
import org.apache.hadoop.conf.Configuration;   
import org.apache.hadoop.fs.FileSystem;   
import org.apache.hadoop.fs.Path;   
import org.apache.hadoop.io.IOUtils;   
import org.apache.hadoop.io.LongWritable;   
import org.apache.hadoop.io.SequenceFile;   
import org.apache.hadoop.io.WritableComparable; 
import org.apache.hadoop.io.Writable;  
import org.apache.hadoop.util.ReflectionUtils;  
import org.apache.hadoop.io.Text;


public class DecisionTree implements WritableComparable {
	//override write, readFields, compareTo, hashCode


  public static class Node {
    private int wordid;
    private Node left;
    private Node right;
    private double gain;
    private int leftlabelid = 0;
    private int rightlabelid = 0;


    public Node(int wordid) {   
      super();   
      this.wordid = wordid;
    }    

    public Node() {   
      super();   
    }

    public int getID(){
      return this.wordid;
    }

    public void setID(int wordid){
      this.wordid = wordid;
    }



    public Node getLeft(){
      return this.left;
    }

    public void setLeft(Node left){
      this.left = left;
    } 

    public Node getRight(){
      return this.right;
    }

    public void setRight(Node right){
      this.right = right;
    }

    public int getLeftLabel(){
      return this.leftlabelid;
    }

    public void setLeftLabel(int label){
      this.leftlabelid = label;
    }

    public int getRightLabel(){
      return this.rightlabelid;
    }

    public void setRightLabel(int label){
      this.rightlabelid = label;
    }

    public int getLabel(Set<Integer> words){

      if(words.contains(new Integer(this.wordid))){
        if(this.right!=null){
          return this.right.getLabel(words);
        }
        else{
        //System.out.println("right label:"+this.rightlabelid);
          return this.getRightLabel();
        }
      }
      else{
        if(this.left!=null){
          return this.left.getLabel(words);
        }
        else{
        	 //System.out.println("left label:"+this.leftlabelid);
          return this.getLeftLabel();
        }
      }
    }

    public String toString(){
      //if(this.wordid!=null){
        StringBuilder str = new StringBuilder(this.wordid+"\t");
        if(this.left!=null){
          str.append(this.left.toString()+"\t");
          /*if (this.left.getID()== -1 )
            str.append("#label:" + this.left.getLabel());*/
        }
        else{
          //str.append("#"+(this.leftlabelid?-1:this.leftlabelid)+"\t");
          str.append("#"+this.leftlabelid);
        }

        if(this.right!=null){
          str.append(this.right.toString()+"\t");
          /*if (this.right.getID()== -1)
            str.append("#label:" + this.right.getLabel());*/
        }
        else{
          //str.append("#"+(this.leftlabelid?-1:this.leftlabelid)+"\t");
          str.append("#"+this.rightlabelid);
        }
        return str.toString(); 
      //}
      //return "no wordid";
    }

    public void write2Seq(DataOutput out) throws IOException {
      out.writeInt(this.wordid);
      if(this.left!=null){
        this.left.write2Seq(out);
      }
      else{
        //out.writeInt((this.leftlabelid?-1:-this.leftlabelid));
        out.writeInt(-this.leftlabelid);
      }

      if(this.right!=null){
        this.right.write2Seq(out);
      }
      else{
        //out.writeInt((this.rightlabelid?-1:-this.rightlabelid));
        out.writeInt(-this.rightlabelid);
      }

    }

    public void readFromSeq(DataInput in) throws IOException {

      int leftid = in.readInt();
      if(leftid<=0){
        this.leftlabelid = -leftid;
      }
      else{
        this.left = new Node(leftid);
        this.left.readFromSeq(in);
      }

      int rightid = in.readInt();
      if(rightid<=0){
        this.rightlabelid = -rightid;
      }
      else{
        this.right = new Node(rightid);
        this.right.readFromSeq(in);
      }
      return;
    }
  }

  private int id;
  private Node root;
  
  public DecisionTree(int id){
    this.id = id;
    this.root =null;
  }
  public DecisionTree() {   
    super();   
    this.id = 0;
    this.root = null;
  }    

  public DecisionTree get(){
  	return this;
  }
  public void setId(int id){
    this.id = id;
  }

  public int getId(){
    return this.id;
  }

  public void setRoot(Node root){
    this.root = root;
  } 

  public Node getRoot(){
    return this.root;
  }

  @Override  
  public void write(DataOutput out) throws IOException {   
    this.root.write2Seq(out);
  }   

  @Override  
  public void readFields(DataInput in) throws IOException {
    
    try{
      int rootid = in.readInt();
      this.root = new Node(rootid);
      this.root.readFromSeq(in);
    }
    catch (IOException e){
      this.root = null;
      throw e;
    }
    
  }   

  @Override  
  public String toString() {
    if(this.root!=null){
      return this.root.toString();
    }
    return "no nodes";
  }   

  //key equal
    
  @Override  
  public boolean equals(Object obj) {   
    if(obj instanceof DecisionTree){   
      DecisionTree tree=(DecisionTree)obj;   
      return tree.getId()==this.id;   
    }   
    return false;  
  }   
    
  @Override  
  public int compareTo(Object obj) 
  {   
    int result=-1;   
    if(obj instanceof DecisionTree){   
     DecisionTree tree=(DecisionTree)obj;   
     if(this.id>tree.getId()){   
       result=1;   
     }
    }   
    return result;
  }   
     
  @Override  
  public int hashCode() 
  {
    return this.id;
  }

 /* public List<Integer> TestDocument(Node root, String words){
      List<Integer> predictedlabels = new ArrayList<Integer>();
      //String[] wordlist = words.split(",");
      DecisionTree.Node rt = this.getRoot();
      int root = rt.getID();
      if (words.contains(Integer.toString(root))){
        goRight();
      }
      else goLeft();
  }        */

  public int predict(String words) {
    String[] wordlist = words.split(",");
    Set<Integer> wordSet = new HashSet<Integer>();
    //save it to set
    for(int i=0; i < wordlist.length; i++) {
	      String[] temp = wordlist[i].split(":");
	      wordSet.add(Integer.parseInt(temp[0]));
    }
    return this.root.getLabel(wordSet);
  }
} 