package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Q4b {
	static class Mapper1 extends Mapper<Object, Text, IntWritable, DoubleWritable>{@Override
  		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String data = value.toString();
			if(data.length() > 1){
				String[] all_data = data.split("\t");
				IntWritable source = new IntWritable(Integer.parseInt(all_data[2]));
				DoubleWritable target = new DoubleWritable(Double.parseDouble(all_data[3]));
				context.write(source, target); }}}
	
	static class Reducer1 extends Reducer<IntWritable,DoubleWritable,IntWritable,Text>{
		private Text result = new Text();
		@Override public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			int count = 0;
			double total_fare = 0;
			for(DoubleWritable value: values) {
				count++;
				total_fare += value.get();}
			double avg_fare = total_fare/count;
			String s1 = String.format("%.2f",avg_fare);
			result.set(s1);
			context.write(key, result);}}
					
	public static void main(String[] args) throws Exception {

		/* TODO: Update variable below with your gtid */
		final String gtid = "jzhu411";

		Configuration conf = new Configuration();
		
		/* TODO: Needs to be implemented */
		
		Job job1 = Job.getInstance(conf, "Q4b");
		job1.setJarByClass(Q4b.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}
