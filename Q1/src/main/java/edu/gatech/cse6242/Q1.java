package edu.gatech.cse6242;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1 {
	/* TODO: Update variable below with your gtid */
	final String gtid = "jzhu411";
	
	public static class TokenizerMapper
	extends Mapper<Object, Text, IntWritable, Text>{
		private final static IntWritable word = new IntWritable();
        private Text weight = new Text();
		public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException
		{String[] trip_info = value.toString().split(",");
            if(trip_info.length == 4) {
                word.set(Integer.parseInt(trip_info[0]));
				double distance = Double.parseDouble(trip_info[2]);
				double fare = Double.parseDouble(trip_info[3]);
				if (distance > 0 && fare > 0) {
				    String trip_weight = trip_info[3];
				    weight.set(trip_weight);
				    context.write(word, weight);}}}}

	public static class IntSumReducer
    extends Reducer<IntWritable,Text,IntWritable,Text> {
		private Text result = new Text();
		public void reduce(IntWritable key, Iterable<Text> values, Context context
						) throws IOException, InterruptedException {
		int count_trip = 0;	    
		double sum_fare = 0;
        for (Text val : values) {
			String[] temp = val.toString().split(",");			
			double fare = Double.parseDouble(temp[0]);
			count_trip ++;
			sum_fare = sum_fare + fare;}
		String s1 = String.format("%,d", count_trip);
		String s2 = String.format("%,.2f", sum_fare);
		result.set(s1 +","+ s2);
		context.write(key, result);}}

	public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q1");
		
		/* TODO: Needs to be implemented */
		
        job.setJarByClass(Q1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
