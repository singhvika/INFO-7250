/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.assignment4.part5.UsingSequenceFileInputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

/**
 *
 * @author Vinyas Kaushik
 */
public class StockMapper extends Mapper<Text, Text, Text, Text>  {
    
        @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
//           String[] fields = value.toString().split(" ");
//           for(int i=0;i<1;i++){
//               Text t = new Text(fields[i]);
//               IntWritable count = new IntWritable(1);
//               context.write(t,count);
//           }

            context.write(key,value);

    }
    
}
