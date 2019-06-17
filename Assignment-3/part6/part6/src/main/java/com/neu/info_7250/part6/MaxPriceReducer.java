package com.neu.info_7250.part6;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MaxPriceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	
	@Override
	 protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) {
		double max = 0.0;
		
		for (DoubleWritable price : values) {
			max = Math.max(max, price.get());
		}
		DoubleWritable maxPrice = new DoubleWritable(max);
		try {
			context.write(key, maxPrice);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
