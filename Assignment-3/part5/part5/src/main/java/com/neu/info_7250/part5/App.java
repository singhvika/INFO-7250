package com.neu.info_7250.part5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Please provide input and output file as arguments");
		}
		try {
			System.out.println("Hello World!");
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "visitCount");
			job.setJarByClass(App.class);
			job.setJobName("Find IP Visit Count");
			FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    job.setInputFormatClass(TextInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(IntWritable.class);
	        job.setMapperClass(VisitCountMapper.class);
	        job.setReducerClass(VisitCountReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        
	        System.out.println(job.waitForCompletion(true) ? 0 : 1);
		    
			
			
			

		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
