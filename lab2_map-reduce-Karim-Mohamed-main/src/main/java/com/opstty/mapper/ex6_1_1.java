package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ex6_1_1 extends Mapper<Object, Text, IntWritable, IntWritable> {
	public int in_line = 0;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (in_line != 0) {

			try {
				Integer annee = Integer.parseInt(value.toString().split(";")[5]);
				context.write(new IntWritable(annee), new IntWritable(Integer.parseInt(value.toString().split(";")[1])));
			}
			catch (NumberFormatException ex) {
			}

		} in_line++;
	}
}