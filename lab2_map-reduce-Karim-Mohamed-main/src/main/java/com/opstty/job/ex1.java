package com.opstty.job_1;

import com.opstty.mapper.ex_1;
import com.opstty.reducer.ex__1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ex1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();


        if (otherArgs.length < 2) {
            System.exit(2);
        }

        Job job_1 = Job.getInstance(conf, "ex1");
        job_1.setJarByClass(ex1.class);
        job_1.setMapperClass(ex_1.class);
        job_1.setCombinerClass(ex__1.class);
        job_1.setReducerClass(ex__1.class);
        job_1.setOutputKeyClass(IntWritable.class);
        job_1.setOutputValueClass(IntWritable.class);

        for (int j = 0; j < otherArgs.length - 1; ++j) {
            FileInputFormat.addInputPath(job_1, new Path(otherArgs[j]));
        }

        FileOutputFormat.setOutputPath(job_1,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job_1.waitForCompletion(true) ? 0 : 1);
    }
}