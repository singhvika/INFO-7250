/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.assignment4.part5.UsingKeyValueInputFormat;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class StockMapper extends Mapper<Text, Text, Text, FloatWritable> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        FloatWritable val = new FloatWritable(Float.valueOf(s));
    context.write(key,val);
    }
}
