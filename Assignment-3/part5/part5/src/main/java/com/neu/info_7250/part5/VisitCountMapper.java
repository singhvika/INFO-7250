package com.neu.info_7250.part5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class VisitCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public static IntWritable one = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, 
            Context context) {
		String line = value.toString();
		String[] split = line.split("\\s+");
		
		Text ip = new Text(split[0]); 
		try {
			context.write(ip, one);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
