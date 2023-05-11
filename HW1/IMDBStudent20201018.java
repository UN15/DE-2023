import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20201018
{
	public static class IMDBStudent20201018Mapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		public final static LongWritable one = new LongWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] splitLine = line.split("::"); 
			String third = splitLine[2];
			StringTokenizer t2 = new StringTokenizer(third, "|");
			while(t2.hasMoreTokens()){
				word.set(t2.nextToken());
				context.write(word, one);
			}
		
		}
	
	}

	public static class IMDBStudent20201018Reducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		private LongWritable sumWritable = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			long sum = 0;
			for(LongWritable value:values){
				sum+=value.get();
			}
			sumWritable.set(sum);
			context.write(key, sumWritable);
		
		}
	
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: IMDBStudent20201018  <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "IMDBStudent20201018");

		job.setJarByClass(IMDBStudent20201018.class);
		job.setMapperClass(IMDBStudent20201018Mapper.class);
		job.setCombinerClass(IMDBStudent20201018Reducer.class);
		job.setReducerClass(IMDBStudent20201018Reducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		job.waitForCompletion(true);
	
	}


}
