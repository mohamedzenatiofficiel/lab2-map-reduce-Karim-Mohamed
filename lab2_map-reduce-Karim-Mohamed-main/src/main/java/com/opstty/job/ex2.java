package com.opstty.job;

import com.opstty.mapper.ex_2;
import com.opstty.reducer.ex__2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ex2 {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.exit(2);
        }


        Job job_2 = Job.getInstance(config, "treeSpecies");
        job_2.setJarByClass(ex2.class);
        job_2.setMapperClass(ex_2.class);
        job_2.setCombinerClass(ex__2.class);
        job_2.setReducerClass(ex__2.class);
        job_2.setOutputKeyClass(Text.class);
        job_2.setOutputValueClass(NullWritable.class);


        for (int k = 0; k < otherArgs.length - 1; ++k) {
            FileInputFormat.addInputPath(job_2, new Path(otherArgs[k]));
        }



        FileOutputFormat.setOutputPath(job_2,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job_2.waitForCompletion(true) ? 0 : 1);
    }
}