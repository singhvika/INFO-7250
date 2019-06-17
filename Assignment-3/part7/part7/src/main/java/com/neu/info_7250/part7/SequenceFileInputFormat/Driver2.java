package com.assignment4.part5.UsingSequenceFileInputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver2 {

    public static void main(String[] args) throws Exception {


        //Map Reduce to find MAX stock_price_high

        try {

            //Creating a Job  Instance

            Job job = Job.getInstance();
            job.setJarByClass(Driver2.class);

            //Mapper Class, Reducer Class, Combiner Class
            job.setMapperClass(StockMapper2.class);
            job.setReducerClass(StockReducer.class);
            job.setCombinerClass(StockReducer.class);



            //Input and Output Format
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);

            //Setting the Input and Output Path
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setNumReduceTasks(1);



            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
}
