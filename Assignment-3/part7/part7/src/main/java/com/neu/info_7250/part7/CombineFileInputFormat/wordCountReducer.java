package com.neu.info_7250.part7.CombineFileInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class wordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {

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
