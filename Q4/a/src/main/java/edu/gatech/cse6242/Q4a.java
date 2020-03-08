package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Q4a {
	static class Mapper1 extends Mapper<Object, Text, IntWritable, IntWritable>{@Override
  		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String data = value.toString();
			if(data.length() > 1){
				String[] all_data = data.split("\t");
				IntWritable source = new IntWritable(Integer.parseInt(all_data[0]));
				IntWritable target = new IntWritable(Integer.parseInt(all_data[1]));
				IntWritable out_degree = new IntWritable(1);
				IntWritable in_degree = new IntWritable(-1);
				context.write(source, out_degree);
				context.write(target, in_degree);}}}
				
	static class Mapper2 extends Mapper<Object, Text, IntWritable, IntWritable>{@Override
  		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
  			String data = value.toString();
            if(data.length() > 1){
				String[] all_data = data.split("\t");
				IntWritable diff = new IntWritable(Integer.parseInt(all_data[1]));
				IntWritable count = new IntWritable(1);
				context.write(diff, count);}}}
	
	static class totalReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{@Override
  		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int diff = 0;
			for(IntWritable value: values)
				diff += value.get();
			context.write(key, new IntWritable(diff));}}
					
	public static void main(String[] args) throws Exception {

		/* TODO: Update variable below with your gtid */
		final String gtid = "jzhu411";

		Configuration conf = new Configuration();
		
		/* TODO: Needs to be implemented */
		
		String med_path = args[1] + "_med";
		Job job1 = Job.getInstance(conf, "Q4a");
		job1.setJarByClass(Q4a.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(totalReducer.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(med_path));
		job1.waitForCompletion(true);
	
		Configuration config2 = new Configuration();
		Job job2 = Job.getInstance(config2, "Q4a");
		job2.setJarByClass(Q4a.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(totalReducer.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(IntWritable.class);
	
		FileInputFormat.addInputPath(job2, new Path(med_path));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
