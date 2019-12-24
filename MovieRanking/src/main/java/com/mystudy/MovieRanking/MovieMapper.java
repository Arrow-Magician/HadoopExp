package com.mystudy.MovieRanking;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.alibaba.fastjson.JSON;

public class MovieMapper extends Mapper<LongWritable, Text, MovieInfo, NullWritable>{
	@Override
	protected void map (LongWritable key1, Text value1, Context context) throws IOException, InterruptedException {
		String MyActor = "山口胜平";	
		String data = value1.toString();
		//分词
		String[] words = data.split(",");
		//转换json
		MovieInfo mov = JSON.parseObject(data, MovieInfo.class);
		
//		int count = 0;
//		if(count < 5) {
//			if(mov.getActor().contains(MyActor)) {
//				context.write(mov, NullWritable.get());
//			}
//			count++;
//		}		
	}
}
