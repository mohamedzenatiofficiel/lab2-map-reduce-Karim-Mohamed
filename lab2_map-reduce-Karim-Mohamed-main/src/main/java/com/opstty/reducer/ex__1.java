package com.opstty.reducer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ex__1 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
    	int s = 0;
        for (IntWritable value : values) {
            s += value.get();
        }
    	context.write(key, new IntWritable(s));
    }
}
