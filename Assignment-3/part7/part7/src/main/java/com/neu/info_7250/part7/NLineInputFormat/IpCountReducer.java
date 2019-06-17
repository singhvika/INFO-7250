/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.assignment4.part5.UsingNLineInputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @author Vinyas Kaushik
 */
public class IpCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable total = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for(IntWritable val: values){

            sum += val.get();
        }
        total.set(sum);
        context.write(key,total);
    }
}
