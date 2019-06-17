package com.neu.info_7250.part7.CombineFileInputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class wordCountMapper extends Mapper<WordOffsetWritable, Text, Text, IntWritable> {


    @Override
    protected void map(WordOffsetWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] field = value.toString().split(" ");
        for(String s:field){
            Text t = new Text(s);
            IntWritable count = new IntWritable(1);
            context.write(t,count);
        }
    }
}
