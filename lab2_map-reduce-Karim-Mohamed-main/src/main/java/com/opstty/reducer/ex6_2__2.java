package com.opstty.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ex6_2__2 extends Reducer<NullWritable, MapWritable, IntWritable, IntWritable> {
	public void reduce(NullWritable key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {
		
		ArrayList<Integer[]> district_y = (ArrayList<Integer[]>) StreamSupport.stream(values.spliterator(), false)
				.map( mw ->  (new Integer[] { ((IntWritable) mw.keySet().toArray()[0]).get(), ((IntWritable) mw.get(mw.keySet().toArray()[0])).get() }))
				.collect(Collectors.toList());
		int year_mini = district_y.stream().map((arr) -> arr[1]).min(Integer::compare).get();
		
		district_y.stream().filter(arr -> arr[1] == year_mini).map(arr -> arr[0]).distinct().forEach((district) -> { try {
			context.write(new IntWritable(year_mini), new IntWritable(district));
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		} });
	}
}
