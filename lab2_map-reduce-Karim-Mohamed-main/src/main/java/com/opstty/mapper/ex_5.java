package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ex_5 extends Mapper<Object, Text, FloatWritable, Text> {
	public int in_line = 0;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (in_line != 0) {

			try {
				String[] l_tok = value.toString().split(";");

				Float h = Float.parseFloat(l_tok[6]);

				context.write(new FloatWritable(h),

			} catch (NumberFormatException ex) {
				}

		} in_line++;
	}
}