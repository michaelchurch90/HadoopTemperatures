//I just modified a wordcount program and made it find the average temperature. I still haven't converted it to use decimal places though
//but i think this is a good start
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
	
	public class AverageTemp {
	
	   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {
	     private  IntWritable temp = new IntWritable();
	     private Text code = new Text();
	    
	     public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {

	      //This is the code for the original wordcount
	      // String line = value.toString();
	      // StringTokenizer tokenizer = new StringTokenizer(line);
	      // while (tokenizer.hasMoreTokens()) {
	       //  word.set(tokenizer.nextToken());
	       //  output.collect(word, one);

		  String tokens[] = value.toString().split(" ");
	          code.set(tokens[0]);
		   
		  output.collect(code, new FloatWritable(Float.parseFloat(tokens[1])));
		  
		   

	       
	     }
	   }
	
	   public static class Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {
	     public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
	       float sum = 0;
	       int count=0;
	       while (values.hasNext()) {
	         sum += values.next().get();
		 count++;
	       }
	       float average = sum/count;
	       output.collect(key, new FloatWritable(average));
	     }
	   }
	
	   public static void main(String[] args) throws Exception {
	     JobConf conf = new JobConf(AverageTemp.class);
	     conf.setJobName("AverageTemp");
	

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
	   }
	}
