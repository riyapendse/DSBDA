package pk1;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.util.GenericOptionsParser;

public class Movie {
	public static void main(String [] args) throws Exception{
		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		
		Job j = new Job(c, "new job");
		j.setJarByClass(Movie.class);
		j.setMapperClass(MyMapper.class);
		j.setReducerClass(MyReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(j,input);
		FileOutputFormat.setOutputPath(j,output);
		
		System.exit(j.waitForCompletion(true)?0:1);
		
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			String tmp = value.toString();
			String []lines = tmp.split("\n");
			for(String line: lines){
				String []eachline = line.split(",");
				float rating = Float.parseFloat(eachline[2]);
				con.write(new Text(eachline[1]), new FloatWritable(rating));
			}
		}
	}
	
	public static class  MyReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
		public void reduce(Text key, Iterable<FloatWritable> vals, Context con) throws IOException, InterruptedException{
			float tot = 0;
			int cnt =0;
			for(FloatWritable v1: vals){
				tot += v1.get();
				cnt += 1;
			}
			float var = tot/(float)cnt;
			con.write(key, new FloatWritable(var));
		}
	}
	


}