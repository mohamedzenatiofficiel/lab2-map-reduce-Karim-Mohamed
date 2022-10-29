package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class input_mapper extends Mapper<LongWritable, Text, NullWritable, MapWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(value.toString());
		
		int c = 0;
		int val_key[] = new int[2];
		while (matcher.find()) {
			if(c < 2)
				val_key[c] = Integer.parseInt(matcher.group(1));
			++c;
		}
		
		MapWritable map = new MapWritable();
		map.put(new IntWritable(val_key[0]), new IntWritable(val_key[1]));
		context.write(NullWritable.get(), map);
	}
}