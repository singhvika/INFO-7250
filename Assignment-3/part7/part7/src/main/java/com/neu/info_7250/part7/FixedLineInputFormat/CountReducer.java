/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.assignment4.part5.UsingFixedLengthInputFormat;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @author Vinyas Kaushik
 */
public class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable count = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum =0;
         for(IntWritable val : values){
             sum += val.get();
         }
         count.set(sum);
         context.write(key,count);
    }
}
