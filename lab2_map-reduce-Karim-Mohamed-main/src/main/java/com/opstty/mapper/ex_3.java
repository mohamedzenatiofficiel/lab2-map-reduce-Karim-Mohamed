package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ex_3 extends Mapper<Object, Text, Text, IntWritable> {
	public int li = 0;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		if (li != 0) {
			context.write(new Text(value.toString().split(";")[3]), new IntWritable(1));
		}
		li++;
	}
}