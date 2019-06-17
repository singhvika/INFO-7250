package com.neu.info_7250.part5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class VisitCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	
	@Override
	 protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
		int sum = 0;
		
		for (IntWritable count : values) {
			sum = sum + count.get();
		}
		IntWritable totalCount = new IntWritable(sum);
		try {
			context.write(key, totalCount);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
