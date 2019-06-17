package com.neu.info_7250.part6;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MaxPriceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) {
		String line = value.toString();
		String[] split = line.split(",");
		Text stockSymbol = null;
		try {
			stockSymbol = new Text(split[1]);
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
		
		
		DoubleWritable price = new DoubleWritable(0.0);
		try {
			price.set(Double.parseDouble(split[4]));
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		try {
			if (stockSymbol!=null && stockSymbol.equals("exchange") == false && stockSymbol.getLength()!=0) {
				System.out.println("STOCK: "+stockSymbol);
				System.out.println("PRICE: "+price);
				context.write(stockSymbol, price);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
