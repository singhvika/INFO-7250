/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.assignment4.part5.UsingKeyValueInputFormat;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @author Vinyas Kaushik
 */
public class StockReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    private FloatWritable max_stock_price_high = new FloatWritable();
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float max =0;
         for(FloatWritable val : values){

             if(val.get() > max){
                max = val.get();
             }
         }
         max_stock_price_high.set(max);
         context.write(key,max_stock_price_high);
    }
}
