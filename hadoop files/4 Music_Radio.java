package pk1;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.util.GenericOptionsParser;


public class Music {
	
	public static void main(String [] args) throws Exception{
		Configuration c = new Configuration();
		String [] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		
		Job j = new Job(c, "new job");
		j.setJarByClass(Music.class);
		j.setMapperClass(MyMapper.class);
		j.setReducerClass(MyReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			String tmpval = value.toString();
			String [] lines = tmpval.split("\n");
			for(String line: lines){
				String []eachLine = line.split(",");
				String trackId = eachLine[1];
				if(Integer.parseInt(eachLine[3]) == 1){
					con.write(new Text(trackId), new IntWritable(1));
				}
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		int maxi = 0;
		Text trackId = new Text("No Song");
		public void reduce(Text key, Iterable<IntWritable>values, Context con)throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val: values){
				sum += val.get();
			}
			if(sum > maxi){
				maxi = sum;
				trackId = key;
			}
			con.write(key, new IntWritable(sum));
		}
		
		@Override
		protected void cleanup(Context con) throws IOException, InterruptedException{
			con.write(new Text("Max time played on radio song is: " + trackId), new IntWritable(0));
		}
	}
	


}
