/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.assignment4.part5.UsingNLineInputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Vinyas Kaushik
 */
public class IpCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {
    
        @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           String[] fields = value.toString().split(" ");
           for(int i=0;i<1;i++){
               Text t = new Text(fields[i]);
               IntWritable count = new IntWritable(1);
               context.write(t,count);
           }

    }
    
}
