/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.assignment4.part5.UsingNLineInputFormat;

/**
 *
 * @author Vinyas Kaushik
 * 
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class Driver {
    
    public static void main(String[] args)  throws Exception {

//      Configuration conf = new Configuration();


		Job job = Job.getInstance();
		job.setJarByClass(IpCountMapper.class);
		job.setInputFormatClass(NLineInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

        NLineInputFormat.setNumLinesPerSplit(job, 1000);
		//Mapper
		job.setMapperClass(IpCountMapper.class);

		//Reducer
		job.setReducerClass(IpCountReducer.class);

		job.setCombinerClass(IpCountReducer.class);
		//Number of Reducers
        job.setNumReduceTasks(2);

        //Specify Key value

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Specify Input Path
        FileInputFormat.addInputPath(job, new Path(args[0]));

        //Specify Output Path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
    
}
