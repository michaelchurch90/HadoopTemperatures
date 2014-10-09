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

	public class StdDev {
	
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
			   float temp = Float.parseFloat(tokens[1]);
			   float mean = Float.parseFloat(tokens[2]);

			   float answer = (temp-mean)*(temp-mean);
			   output.collect(new Text(tokens[0]),new Text(String.valueOf(answer)));
		   }
	   }
	
	   public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	     public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	       float sum = 0;
	       int count=0;
		float curVal=0;
		ArrayList<String>newValues = new ArrayList<String>();
		String curString="";
	       while (values.hasNext()) {
		       curString=values.next().toString();

         	curVal =Float.parseFloat(curString);

         	//curVal =55;
                sum += curVal;
		newValues.add(curString);
		 count++;
	       }
	       float average = sum/count;
	       String outString="";
		for(String val: newValues)
		{		
	      		 outString=val+" "+average;
		       output.collect(key, new Text(outString));
		}
	     }
	   }
	   
	   public static class Reduce2 extends MapReduceBase implements Reducer<Text,Text,Text,Text> {
		   public void reduce(Text key, Iterator<Text>values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
			   float sum=0;
			   int count=0;
			   while(values.hasNext())
			   {
				   sum+= Float.parseFloat(values.next().toString());
				   count++;
			   }
			   double answer = Math.sqrt(sum/count);
			   output.collect(key,new Text(String.valueOf(answer)));
		   }
	   }
	
	   public static void main(String[] args) throws Exception {
		/*
	     JobConf conf = new JobConf(StdDev.class);
	     conf.setJobName("StdDev");
	

	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(FloatWritable.class);
	
	     conf.setMapperClass(Map.class);
	     conf.setCombinerClass(Reduce.class);
	     conf.setReducerClass(Reduce.class);
	
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	     JobClient.runJob(conf);
		*/

		JobConf conf = new JobConf(StdDev.class);
		conf.setJobName("StdDev");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		//conf.setCombinerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path("/tmp/"));
//		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		FileSystem fs =  FileSystem.get(conf);
		fs.delete(new Path("/tmp"),true);

		JobClient.runJob(conf);

		JobConf conf2 = new JobConf(StdDev.class);
		conf2.setJobName("StdDev");


		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(Text.class);
		conf2.setMapperClass(Map2.class);
		conf2.setReducerClass(Reduce2.class);

		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf2, new Path("/tmp/"));
		FileOutputFormat.setOutputPath(conf2, new Path(args[1]));

		JobClient.runJob(conf2);

/*

		Job job2 = new Job(conf);
		FileInputFormat.addInputPath(job2, new Path("/tmp/part*"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		ControlledJob controlJob2= new ControlledJob(conf);
		controlJob.setJob(job2);

		JobControl jobControll = new JobControl("JobControl");
		jobControl.addJob(controlJob1);
		jobControl.addJob(controlJob2);
		controlJob2.addDependingJob(controlJob1);

		Thread jobRunner = new Thread(new JobRunner(jobControl));
		jobRunner.start();

		while(!jobControl.allFinished()){
			Thread.sleep(500);
		}

		jobControl.stop();
*/

		fs.delete(new Path("/tmp"),true);
		fs.close();

		
	   }
	}
