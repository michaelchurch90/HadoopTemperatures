//I just modified a wordcount program and made it find the average temperature. I still haven't converted it to use decimal places though
//but i think this is a good start
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.FileSystem;	
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.jobcontrol.*;

	public class Neighbor {
	
	   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	     private Text code = new Text();
	    
	     public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

	      //This is the code for the original wordcount
	      // String line = value.toString();
	      // StringTokenizer tokenizer = new StringTokenizer(line);
	      // while (tokenizer.hasMoreTokens()) {
	       //  word.set(tokenizer.nextToken());
	       //  output.collect(word, one);

		  String tokens[] = value.toString().split(" ");
	          code.set(tokens[0]);
		   
		  output.collect(code, new Text(tokens[1]));
		  
		   

	       
	     }
	   }

	   public static class Map2 extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text> {
		   public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
			   String tokens[] = value.toString().split("\\s+");
			   int zip = Integer.parseInt(tokens[0]);
			   float mean = Float.parseFloat(tokens[1]);
				
		           int zipMin2=(zip-2)%1000;
			   int zipMin1=(zip-1)%1000;
			   int zipPlus1=(zip+1)%1000;
			   int zipPlus2=(zip+2)%1000;

			   String outkey1= zip+":"+zipPlus1+":"+zipPlus2;
			   String outval1= mean+":-1000:-1000";

			   String outkey2=zipMin1+":"+zip+":"+zipPlus1;
			   String outval2="-1000:"+mean+":-1000";

			   String outkey3=zipMin2+":"+zipMin1+":"+zip;
			   String outval3="-1000:-1000:"+mean;

			   output.collect(new Text(outkey1),new Text(outval1));
			   output.collect(new Text(outkey2),new Text(outval2));
			   output.collect(new Text(outkey3),new Text(outval3));
		   }
	   }
	
	   public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	     public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	       float sum = 0;
	       int count=0;
		float curVal=0;
		String curString="";
	       while (values.hasNext()) {
		       curString=values.next().toString();

         	curVal =Float.parseFloat(curString);

                sum += curVal;
		 count++;
	       }
	       float average = sum/count;
	       String outString=""+average;
		       output.collect(key, new Text(outString));
		
	     }
	   }
	   
	   public static class Reduce2 extends MapReduceBase implements Reducer<Text,Text,Text,Text> {
		   public void reduce(Text key, Iterator<Text>values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
			   String tokens[] = key.toString().split(":");
			   String lowKey=tokens[0];
			   String medKey=tokens[1];
			   String highKey=tokens[2];

			   float lowVal=-1000;
			   float medVal=-1000;
			   float highVal=-1000;
			   


			   //String input="";;
			   while(values.hasNext())
			   {
				   String[] input=values.next().toString().split(":");
				   if(lowVal<=Float.parseFloat(input[0]))
					   lowVal=Float.parseFloat(input[0]);
			 	   if(medVal<=Float.parseFloat(input[1]))
					   medVal=Float.parseFloat(input[1]);
				   if(highVal<=Float.parseFloat(input[2]))
					   highVal=Float.parseFloat(input[2]);

				
				   
			   }
			   if(medVal>highVal && medVal>lowVal)
			   	output.collect(new Text(medKey),new Text(String.valueOf(medVal)));
		   }
	   }
	
	   public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(Neighbor.class);
		
		conf.setJobName("Neighbor");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path("/tmp123/"));
//		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		FileSystem fs =  FileSystem.get(conf);
		fs.delete(new Path("/tmp123"),true);

		JobClient.runJob(conf);



		JobConf conf2 = new JobConf(Neighbor.class);
		conf2.setJobName("Neighbor");


		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(Text.class);
		conf2.setMapperClass(Map2.class);
		conf2.setReducerClass(Reduce2.class);

		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf2, new Path("/tmp123/"));
		FileOutputFormat.setOutputPath(conf2, new Path(args[1]));

		JobClient.runJob(conf2);
		fs.delete(new Path("/tmp"),true);

		fs.close();

		
	   }
	}
