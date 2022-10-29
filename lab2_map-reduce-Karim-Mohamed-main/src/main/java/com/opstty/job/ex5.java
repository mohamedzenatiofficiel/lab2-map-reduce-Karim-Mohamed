package com.opstty.job;

import com.opstty.mapper.ex_5;
import com.opstty.reducer.ex__5;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ex5 {
    public static void main(String[] args) throws Exception {
        Configuration CONFIG = new Configuration();
        String[] otherArgs = new GenericOptionsParser(CONFIG, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("error");
            System.exit(2);
        }

        Job job = Job.getInstance(CONFIG, "treesSortedByHeight");
        job.setJarByClass(ex5.class);
        job.setMapperClass(ex_5.class);
        job.setReducerClass(ex__5.class);
        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}