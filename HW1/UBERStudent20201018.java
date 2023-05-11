import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20201018
{
	public static class UBERStudent20201018Mapper extends Mapper<LongWritable, Text, Text, Text>{
		private Text t1 = new Text();
		private Text t2 = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			String[] splitLine = line.split(",");
			String day = "MON";
			try{
				day = getDay(splitLine[1]);
			}catch(Exception e){
				e.printStackTrace();
			}
			String k = splitLine[0] + "," + day;
			String v = splitLine[2] + "," + splitLine[3];
			t1.set(k);
			t2.set(v);
			context.write(t1, t2);
		}

		protected String getDay(String date1) throws ParseException {
		
			SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
	
		       	Date date2 = dateFormat.parse(date1);
	
			Calendar cal = Calendar.getInstance();
			cal.setTime(date2);
			int d = cal.get(Calendar.DAY_OF_WEEK);
			String day = "MON";
	
			switch(d){
				case 1:
					day = "SUN";
					break;
				case 2:
					day = "MON";
					break;
				case 3:
					day = "TUE";
					break;
				case 4:
					day = "WED";
					break;
				case 5:
					day = "THR";
					break;
				case 6:
					day = "FRI";
					break;
				case 7:
					day = "SAT";
					break;
			
			}
	
	
			return day;
	
		}


	}

	public static class UBERStudent20201018Reducer extends Reducer<Text, Text, Text, Text>{
		private Text tripsVehicles = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			long trips = 0;
			long vehicles = 0;
			
			for(Text v : values){
				String val = v.toString();
				StringTokenizer st = new StringTokenizer(val, ",");
				vehicles+=Long.parseLong(st.nextToken());
				trips+=Long.parseLong(st.nextToken());
			}
			String tv = Long.toString(trips) + "," + Long.toString(vehicles);
			tripsVehicles.set(tv);
			context.write(key, tripsVehicles);
		
		} 
	
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: UBERStudent20201018  <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "UBERStudent20201018");

		job.setJarByClass(UBERStudent20201018.class);
		job.setMapperClass(UBERStudent20201018Mapper.class);
		job.setReducerClass(UBERStudent20201018Reducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		job.waitForCompletion(true);
	}

}

