/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.assignment4.part5.UsingSequenceFileInputFormat;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 *
 * @author Vinyas Kaushik
 */
public class Driver {
    
    public static void main(String[] args) throws Exception {


        //Map Reduce to find MAX stock_price_high

        try {

            //Creating a Job  Instance

            Job job = Job.getInstance();
            job.setJarByClass(Driver.class);

            //Mapper Class, Reducer Class, Combiner Class
            job.setMapperClass(StockMapper.class);



            //Input and Output Format
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //Setting the Input and Output Path
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setNumReduceTasks(0);



            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
}
