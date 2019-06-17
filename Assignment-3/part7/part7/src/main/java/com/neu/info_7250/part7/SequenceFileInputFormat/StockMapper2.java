package com.assignment4.part5.UsingSequenceFileInputFormat;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StockMapper2 extends Mapper<Text, Text, Text, FloatWritable> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] s = key.toString().split(",");
                    Text t = new Text(s[0]);
                    FloatWritable val = new FloatWritable(Float.parseFloat(s[1]));
                    context.write(t, val);

    }
}

