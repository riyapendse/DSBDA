package musicradpkg;



import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class musicradcls {
	static HashMap<Text,IntWritable> radio = new HashMap<>();
	public static void main(String [] args) throws Exception{
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c,args).getRemainingArgs();
        Path input=new Path(files[0]);
        Path output=new Path(files[1]);
        Path output1 = new Path(files[2]);
        Job j=new Job(c,"getlistners");
        j.setJarByClass(musicradcls.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        Job j1=new Job(c,"getlistner1");
        j1.setJarByClass(musicradcls.class);
        j1.setMapperClass(MapForWordCount1.class);
        j1.setReducerClass(ReduceForWordCount1.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, output1);
        System.exit(j.waitForCompletion(true) && j1.waitForCompletion(true)?0:1);

	}
	 public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

	        public void map(LongWritable key, Text textfile, Context con) throws IOException, InterruptedException {
	        		
	        	String alltext = textfile.toString();
	        	String[] textlines = alltext.split("\n");
	        	for(String line: textlines){
	        		String[] lineele = line.split(",");
	        		String trackid = lineele[1];
	        		int radio = Integer.parseInt(lineele[3]);
	        		con.write(new Text(trackid), new IntWritable(radio));
	        	}
	        	
				
	        }
	    }
	    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
	    	
	    	
	        public void reduce(Text key, Iterable<IntWritable> args, Context con) throws IOException, InterruptedException {
	        	int rad = 0;
	        	for(IntWritable num: args){
	        		rad+=num.get();
	        	}
	        	con.write(new Text("Radio of key "+key.toString()), new IntWritable(rad));

	        }
	       

	    }
	    public static class MapForWordCount1 extends Mapper<LongWritable, Text, Text, IntWritable> {

	        public void map(LongWritable key, Text textfile, Context con) throws IOException, InterruptedException {
	        		
	        	String alltext = textfile.toString();
	        	String[] textlines = alltext.split("\n");
	        	for(String line: textlines){
	        		String[] lineele = line.split(",");
	        		String trackid = lineele[1];
	        		int radio = Integer.parseInt(lineele[4]);
	        		con.write(new Text(trackid), new IntWritable(radio));
	        	}
	        	
				
	        }
	    }
	    public static class ReduceForWordCount1 extends Reducer<Text, IntWritable, Text, IntWritable> {
	    	
	        public void reduce(Text key, Iterable<IntWritable> args, Context con) throws IOException, InterruptedException {
	        	int rad = 0;
	        	for(IntWritable num: args){
	        		rad+=num.get();
	        	}
	        	con.write(new Text("Skip of key "+key.toString()), new IntWritable(rad));

	        }
	       
	     

	    }
}

