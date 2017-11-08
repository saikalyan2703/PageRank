//Name: Sai kalyan Yeturu
//Id: 800956002

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class PageRank extends Configured implements Tool {

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new PageRank(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	   
	 //Creating job is to run the link graph for the given input
      Job job  = Job .getInstance(getConf(), " pagerank ");
      job.setJarByClass( this .getClass());
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1].concat("/LinkGraphOutput")));
      job.setMapperClass( LinkGraphMap .class);
      job.setReducerClass( LinkGraphReduce .class);
      job.setOutputKeyClass( IntWritable .class);
      job.setOutputValueClass( Text .class);
      job.waitForCompletion( true);
      
     //Creating job to iterate 10 times to calculate PageRank for the created LinkGraph output 
     Job job2;
     for(int i=1; i<=10; i++){
    	 Configuration pageRankConfig = getConf();
    	 job2 = Job .getInstance(pageRankConfig, " pagerank ");
    	 job2.setJarByClass( this .getClass());
    	 
    	 if(i==1){
    		 FileInputFormat.addInputPaths(job2,  args[1].concat("/LinkGraphOutput"));
    	 }
    	 
    	 else{
    		 FileInputFormat.addInputPaths(job2,  args[1].concat("/PageRankOutput").concat(Integer.toString(i-1)));
    	 }
         
         FileOutputFormat.setOutputPath(job2,  new Path(args[ 1].concat("/PageRankOutput").concat(Integer.toString(i))));
         job2.setMapperClass( PageRankMap .class);
         job2.setReducerClass( PageRankReduce .class);
         job2.setOutputKeyClass( Text .class);
         job2.setOutputValueClass( Text .class);
         job2.waitForCompletion( true);
     }
     
     //Creating job to clean and sort the page rank output
     Configuration cleanAndSortConfig = getConf();
     Job job3  = Job .getInstance(cleanAndSortConfig, " pagerank ");
     job3.setJarByClass( this .getClass());
     job3.setNumReduceTasks(1);
     FileInputFormat.addInputPaths(job3,  args[1].concat("/PageRankOutput10"));
     FileOutputFormat.setOutputPath(job3,  new Path(args[ 1].concat("/FinalOutput")));
     job3.setMapperClass( CleanAndSortMap .class);
     job3.setReducerClass( CleanAndSortReduce .class);
     job3.setOutputKeyClass( DoubleWritable .class);
     job3.setOutputValueClass( Text .class);
     job3.waitForCompletion( true);
     
     //Creating configuration object to delete intermediate files
     Configuration config = getConf();
     FileSystem hdfs = FileSystem.get(config);
     Path LinkGraphPath = new Path(args[1].concat("/LinkGraphOutput"));
     
     if(hdfs.exists(LinkGraphPath)){
    	 hdfs.delete(LinkGraphPath, true);
     }
     
     for(int i=1; i<=10; i++){
    	 Path PageRankPath = new Path(args[1].concat("/PageRankOutput").concat(Integer.toString(i)));
    	 if(hdfs.exists(PageRankPath)){
    		 hdfs.delete(PageRankPath, true);
    	 }
     }
     
     return 1;
     
   }
   
   public static class LinkGraphMap extends Mapper<LongWritable ,  Text , IntWritable, Text > {
      private final static IntWritable one  = new IntWritable( 1);

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
	 
         //Patterns to retrieve text in between title and URL tags
         String titlePattern = "(<title>)([\\s\\S]*?)(</title>)";
         String urlPattern = "(\\[\\[)([\\s\\S]*?)(\\]\\])";
         
         String linkText = "";
         Pattern r1 = Pattern.compile(urlPattern);

         // Now create matcher object.
         java.util.regex.Matcher m1 = r1.matcher(line);
         while (m1.find( )) {
        	 linkText = linkText.concat(m1.group(2)).concat("&&");  
         }
         
         // Create a Pattern object
         Pattern r = Pattern.compile(titlePattern);

         // Now create matcher object.
         java.util.regex.Matcher m = r.matcher(line);
         //Writing the intermediate output key as one and value as the title data and URL data
         while (m.find( )) {
        	 if(linkText.length()>0){
        		 context.write(one, new Text(m.group(2).concat("->").concat(linkText.substring(0,linkText.length()-2)))); 
        	 }
        	 else{
        		 context.write(one, new Text(m.group(2).concat("->").concat(linkText)));
        	 }
         }       
      }
   }
   
   public static class LinkGraphReduce extends Reducer< IntWritable , Text,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( IntWritable count,  Iterable<Text > words,  Context context)
         throws IOException,  InterruptedException {
    	  	
    	  ArrayList<String> list = new ArrayList<String>();
    	  HashMap<String,String> pointTo = new HashMap<String,String>();
    	  Double total_pages = 0.0;
    	  
    	  //Iterate the value iterator to find total number of pages in the input file
    	    for(Text word: words){
    	    	String[] keyWords = word.toString().split("->");
    	    	list.add(keyWords[0]);
    	    	if(keyWords.length>1){
    	    		pointTo.put(keyWords[0], keyWords[1]);
    	    	}
    	    	
    	    	total_pages += 1;
    	    }
    	    
    	    //Finding initial page rank as 1/N
    	    Double page_rank = (double) (1/total_pages);
    	    
    	    //Writing the output key as the title name with output links and value as the page rank value
    	    for(int i=0; i<list.size(); i++){
    	    	if(pointTo.containsKey(list.get(i))){
    	    		context.write(new Text(list.get(i).concat("->").concat(pointTo.get(list.get(i)))), new DoubleWritable(page_rank));
    	    	}
    	    	else{
    	    		context.write(new Text(list.get(i)), new DoubleWritable(page_rank));
    	    	}
    	    }
      }
   }
   
   public static class PageRankMap extends Mapper<LongWritable ,  Text , Text, Text > {

	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {
	    	  
	    	  String line  = lineText.toString();
	    	  String[] splits = line.split("\\t");
	    	  String[] nodeLinks = splits[0].split("->");
	    	  
	    	  if(nodeLinks.length>1){
	    		  //Writing intermediate key as the link name and the value as out links
	    		  context.write(new Text(nodeLinks[0]), new Text("Links->".concat(nodeLinks[1]))); 
	    		  String[] links = nodeLinks[1].split("&&");
		    	  int total_links = links.length;
		    	  
		    	  for(String link: links){
		    		  try{
		    			  //Writing the intermediate key as the link and value as the division value of page rank and number of out links
	    				  context.write(new Text(link), new Text(Double.toString(Double.parseDouble(splits[1])/total_links)));  
	    			  } catch(Throwable e ){
	    				  continue;
	    			  }
		    	  }
	    	  }
	    	  //Writing the intermediate key as the link name and value as some random flag to detect the waste pages
	    	  context.write(new Text(nodeLinks[0]),new Text("aTextNode"));
	      }
	   }
	   
	   public static class PageRankReduce extends Reducer< Text , Text,  Text ,  Text > {
	      @Override 
	      public void reduce( Text nodeName,  Iterable<Text > words,  Context context)
	         throws IOException,  InterruptedException {
	    	  
	    	String node = nodeName.toString();  
	    	String outLinks = "";  
	    	Double pageRank = 0.0;
	    	Boolean flag = false;
	    	  
	    	for(Text word: words){
	    		String wordName = word.toString();
	    		
	    		//Condition to store the out links of a particular link
	    		if(wordName.startsWith("Links->")){
	    			outLinks = wordName.substring(7, wordName.length());
	    		}
	    		
	    		//Condition to detect whether the key is waste or not
	    		else if(wordName.equals("aTextNode")){
	    			flag = true;
	    		}
	    		
	    		//Condition to find the page rank values
	    		else{
	    			pageRank += Double.parseDouble(wordName);
	    		}
	    	}
	    	
	    	//Finding the final page rank value using the page rank formulae with damping factor as 0.85
	    	pageRank = 0.15 + 0.85*(pageRank);
	    	
	    	if(flag){
	    		//Writing final output key as the link name and output links and value as the page rank value
	    		if(outLinks==""){
		    		context.write(new Text(node), new Text("" + pageRank));
		    	}
		    	
		    	else{
		    		context.write(new Text(node.concat("->").concat(outLinks)), new Text("" + pageRank));
		    	}
	    	}
	      }
	   }
	   
	   public static class CleanAndSortMap extends Mapper<LongWritable ,  Text , DoubleWritable, Text > {

		      public void map( LongWritable offset,  Text lineText,  Context context)
		        throws  IOException,  InterruptedException {

		         String line  = lineText.toString();
		         String[] wordNames = line.split("\\t");
		         
		         //Setting intermediate key as the negative page rank value and the value as the link name and out links
		         context.write(new DoubleWritable(Double.parseDouble(wordNames[1])*(-1)), new Text(wordNames[0].split("->")[0]));
		      }
		   }
		   
		   public static class CleanAndSortReduce extends Reducer< DoubleWritable , Text,  Text ,  DoubleWritable > {
		      @Override 
		      public void reduce( DoubleWritable count,  Iterable<Text > words,  Context context)
		         throws IOException,  InterruptedException {
		    	  
		    	 for(Text word: words){
		    		 String data = word.toString();
		    		 Double value = count.get();
		    		 
		    		 //Setting final output key as the link name ,out links and value as the page rank value
		    		 context.write(new Text(data), new DoubleWritable(-value));
		    	 }
		    	  
		      }
		   }
}
