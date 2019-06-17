/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.assignment4.part5.UsingFixedLengthInputFormat;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Vinyas Kaushik
 */
public class CountMapper extends Mapper<LongWritable, BytesWritable, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {

        String s = new String(value.copyBytes());
        System.out.println(s);

        String[] fields = s.split(" ");
            for(int i=0;i<fields.length;i++) {
                if(i==4) {
                    Text t = new Text(fields[i]);
                    IntWritable count = new IntWritable(1);
                    context.write(t,count);
                }
            }
    }
}
