/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.info_7250.part7.FixedLineInputFormat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class Driver {
    
    public static void main(String[] args) throws Exception {


        //Map Reduce to find MAX stock_price_high

        try {

            //Creating a Job  Instance
            Configuration conf = new Configuration();
            conf.setInt("FixedLengthInputFormat.FIXED_RECORD_LENGTH",72);
            FixedLengthInputFormat.setRecordLength(conf,72);
            Job job = Job.getInstance(conf);
            job.setJarByClass(Driver.class);

            //Mapper Class, Reducer Class, Combiner Class
            job.setMapperClass(CountMapper.class);
            job.setReducerClass(CountReducer.class);
            job.setCombinerClass(CountReducer.class);

            //Input and Output Format
            job.setInputFormatClass(FixedLengthInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //Setting the Input and Output Path
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            //Setting Number of Reducers
            job.setNumReduceTasks(1);



            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
}
