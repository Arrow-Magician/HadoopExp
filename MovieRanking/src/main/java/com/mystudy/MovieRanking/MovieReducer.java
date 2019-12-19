package com.mystudy.MovieRanking;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieReducer extends Reducer<MovieInfo, NullWritable, NullWritable, Text>{
	private int count = 0;
	
	@Override
	public void reduce(MovieInfo k3, Iterable<NullWritable> v3, Context context) throws IOException, InterruptedException {
		if (count < 5) {
			context.write(NullWritable.get(), new Text("\t"+k3.toString()));
			count++;
		}
	}

}
