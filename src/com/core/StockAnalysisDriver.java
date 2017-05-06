package com.core;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StockAnalysisDriver extends Configured implements Tool{

	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new Configuration() ,new StockAnalysisDriver(), args);
			System.out.println(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		if(args.length != 4){
			System.out.println("Three parameters are required: <inputFile> <outputFile>");
			return -1;
		}
		long start = new Date().getTime();	
		
		//Step1: Setting Job Configuration
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		job.setJobName("Stock Analysis Phase 1");
		job.setJarByClass(StockAnalysisDriver.class);
		//Step2: Adding input & output directory path
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//Step3: Setting Reducer Output Key Value Types
		//This is required as default is LongWritable and Text
		//And we are passing Text and Text
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//Step4: Setting Mapper and Reducer Class
		job.setMapperClass(MapperPh1.class);
		job.setReducerClass(ReducePh1.class);
		//Step5: Run job and add wait condition
		job.waitForCompletion(true);
		
		//------------- Second Phase Job Configurations ----
		//Step1 - Set Job Config
		Job job2 = Job.getInstance();
		job2.setJobName("Stock Analysis Phase 2");
		job2.setJarByClass(StockAnalysisDriver.class);
		//Step2 - Set Input & Output directory path
		FileInputFormat.setInputPaths(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		//Step3 - Setting Map Output Key value types
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		//Step4 - Setting Mapper and Reducer Class
		job2.setMapperClass(MapPhase2.class);
		job2.setReducerClass(ReducePhase2.class);
		//Step5 - Run the job
		boolean success2 = job2.waitForCompletion(true);
		if (success2 == true) {
			long end = new Date().getTime();
			System.out.println("\nJob took " + (end-start)/1000 + "seconds\n");
		}
		return success2?0:1;
	}

}
