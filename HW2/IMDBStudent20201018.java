import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20201018
{
	
	public static class IMDBStudent20201018Mapper1 extends Mapper<Object, Text, IntWritable, Text> {
		boolean fileM = true;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] splitLine = line.split("::");
			
			IntWritable outputKey = new IntWritable();
			Text outputValue = new Text();
			int joinKey = 0;
			String o_value = "";
			if(fileM) {
				joinKey = Integer.parseInt(splitLine[0]);
				o_value = "M,"+ splitLine[1]+","+splitLine[2];
					
				outputKey.set(joinKey);
		                outputValue.set(o_value);
                	        context.write(outputKey, outputValue);
			}
			else {
				joinKey = Integer.parseInt(splitLine[1]);
				o_value = "R,"+ splitLine[2];
				
				outputKey.set(joinKey);
				outputValue.set(o_value);
				context.write(outputKey, outputValue);
			}
		}
		protected void setup(Context context) throws IOException, InterruptedException {
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();

			if (filename.indexOf( "movies.dat" ) != -1 ) 
				fileM = true;
			else 
				fileM = false;
		}
	}

	public static class IMDBStudent20201018Reducer1 extends Reducer<IntWritable,Text,Text, NullWritable> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text reduce_key = new Text();
			DoubleWritable reduce_result = new DoubleWritable();
			String m_name = "";
			String genre = "";
			double sumRate = 0;
			double count = 0;
			boolean checkFantasy = false;
			for (Text val : values) {
				StringTokenizer st = new StringTokenizer(val.toString(), ",");
				String file_type = st.nextToken();
				if(file_type.equals("M")) {
					m_name = st.nextToken().trim();
					genre = st.nextToken();
                                	StringTokenizer st2 = new StringTokenizer(genre, "|");
		
                	                while(st2.hasMoreTokens()){
                        	                if(st2.nextToken().equals("Fantasy")){
						       	checkFantasy = true;
                                        	}
                                	}

				}
				else {
					sumRate += Double.parseDouble(st.nextToken());
					count += 1;
				}
			}
			if(checkFantasy){
				double av_rate = sumRate/count;
				reduce_key.set(m_name+"|"+av_rate);
				reduce_result.set(av_rate);
				context.write(reduce_key, NullWritable.get());
			}
		}
	}
	
	public static class Movie{
		public String movie_name;
		public double av_rate;

		public Movie(String _movie_name, double _av_rate){
			this.movie_name = _movie_name;
			this.av_rate = _av_rate;
		}

		public String getString(){
			return movie_name +"|"+ av_rate;
		}
	
	}

	public static class MovieComparator implements Comparator<Movie> {
		public int compare(Movie x, Movie y){
			if(x.av_rate > y.av_rate) 
				return 1;
			if(x.av_rate < y.av_rate)
				return -1;
			return 0;
		}
		
	}
	public static void insertMovie(PriorityQueue q, String movie_name, double av_rate, int topK){
		Movie movie_head = (Movie)q.peek();
                if(q.size() < topK || movie_head.av_rate < av_rate){
			Movie movie = new Movie(movie_name, av_rate);
                        q.add(movie);
                        if(q.size() > topK) 
				q.remove();
                }

       }


	public static class IMDBStudent20201018Mapper2 extends Mapper<Object, Text, Text, NullWritable>{
		private PriorityQueue<Movie> queue;
		private Comparator<Movie> comp = new MovieComparator();
		private int topK;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			String movie_name = itr.nextToken().trim();
			double av_rate = Double.parseDouble(itr.nextToken().trim());
			insertMovie(queue, movie_name, av_rate, topK);

		}

		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Movie>(topK, comp);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException{
			while(queue.size() != 0){
				Movie movie = (Movie)queue.remove();
				context.write(new Text(movie.getString()), NullWritable.get());
			}
		}
	
	}

	public static class IMDBStudent20201018Reducer2 extends Reducer<Text, NullWritable, Text, NullWritable>{
		private PriorityQueue<Movie> queue;
		private Comparator<Movie> comp = new MovieComparator();
		private int topK;
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
			StringTokenizer itr2 = new StringTokenizer(key.toString(), "|");
			String movie_name = itr2.nextToken().trim();
			double av_rate = Double.parseDouble(itr2.nextToken().trim());
			insertMovie(queue, movie_name, av_rate, topK);
		
		}
		
		protected void setup(Context context) throws IOException, InterruptedException{
                        Configuration conf = context.getConfiguration();
                        topK = conf.getInt("topK", -1);
                        queue = new PriorityQueue<Movie>(topK, comp);
                }

                protected void cleanup(Context context) throws IOException, InterruptedException{
                        while(queue.size() != 0){
                                Movie movie = (Movie)queue.remove();
				String k = movie.getString();
				String key = k.replace("|"," ");
                                context.write(new Text(key), NullWritable.get());
                        }
                }
	
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Path path = new Path(otherArgs[2]);
		int topK = Integer.parseInt(path.toString());
		//int topK = 2;
		String first_phase_result = "/first_phase_result";
		if(otherArgs.length != 3){
			System.err.println("Usage: IMDBStudent20201018 <in> <out> <topK>");
			System.exit(2);
		}
		conf.setInt("topK", topK);	
		Job job1 = new Job(conf, "IMDBStudent20201018 1");
		job1.setJarByClass(IMDBStudent20201018.class);
		job1.setMapperClass(IMDBStudent20201018Mapper1.class);
		job1.setReducerClass(IMDBStudent20201018Reducer1.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		//FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job1, new Path(first_phase_result));
		//FileSystem.get(job1.getConfiguration()).delete(new Path(otherArgs[1]), true);
                FileSystem.get(job1.getConfiguration()).delete(new Path(first_phase_result), true);
		job1.waitForCompletion(true);
		
		Job job2 = new Job(conf, "IMDBStudent20201018 2");
                job2.setJarByClass(IMDBStudent20201018.class);
                job2.setMapperClass(IMDBStudent20201018Mapper2.class);
                job2.setReducerClass(IMDBStudent20201018Reducer2.class);
		job2.setNumReduceTasks(1);

                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(NullWritable.class);
                //FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
                FileInputFormat.addInputPath(job2, new Path(first_phase_result));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
                FileSystem.get(job2.getConfiguration()).delete(new Path(otherArgs[1]), true);
		
                System.exit(job2.waitForCompletion(true)? 0:1);
	}

}
