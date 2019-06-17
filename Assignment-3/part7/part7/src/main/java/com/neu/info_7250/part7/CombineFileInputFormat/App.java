package com.neu.info_7250.part7.CombineFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class App {

	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance();
		job.setJarByClass(App.class);
		job.setInputFormatClass(MyInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Mapper
		job.setMapperClass(wordCountMapper.class);

		// Reducer
		job.setReducerClass(wordCountReducer.class);

		job.setCombinerClass(wordCountReducer.class);
		// Number of Reducers
		job.setNumReduceTasks(1);

		// Specify Key value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Specify Input Path
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Specify Output Path
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

//		System.out.println("Hello Hadoop");
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
