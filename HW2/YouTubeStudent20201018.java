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

public class YouTubeStudent20201018
{
	
	public static class YouTubeStudent20201018Mapper1 extends Mapper<Object, Text, Text, DoubleWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] splitLine = line.split("\\|");
			
			String category = splitLine[3];
			double av_rating = Double.parseDouble(splitLine[6]);
			Text outputKey = new Text();
			DoubleWritable outputValue = new DoubleWritable();
			
			outputKey.set(category);
			outputValue.set(av_rating);
			context.write(outputKey, outputValue);
			
		}
		
	}

	public static class YouTubeStudent20201018Reducer1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable averageWritable = new DoubleWritable();
		private Text category = new Text();
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			
			double sum = 0;
			double count = 0;
			for(DoubleWritable value:values){
				sum+=value.get();
				count+=1;
			}

			double average = sum/count;
			averageWritable.set(average);

			String k = key.toString();
			String replaceK = k.replace(" ", "");
			category.set(replaceK);
			context.write(category, averageWritable);			
			
		}
	}
	
	public static class Youtube{
                public String category;
                public double av_rate;

                public Youtube(String _category, double _av_rate){
                        this.category = _category;
                        this.av_rate = _av_rate;
                }

                public String getString(){
                        return category+" "+ av_rate;
                }

        }
	
	public static class YoutubeComparator implements Comparator<Youtube> {
                public int compare(Youtube x, Youtube y){
                        if(x.av_rate > y.av_rate)
                                return 1;
                        if(x.av_rate < y.av_rate)
                                return -1;
                        return 0;
                }

        }

	public static void insertYoutube(PriorityQueue q, String category, double av_rate, int topK){
		Youtube youtube_head = (Youtube)q.peek();
                if(q.size() < topK || youtube_head.av_rate < av_rate){
			Youtube youtube = new Youtube(category, av_rate);
                        q.add(youtube);
                        if(q.size() > topK) 
				q.remove();
                }

       }


	public static class YouTubeStudent20201018Mapper2 extends Mapper<Object, Text, Text, NullWritable>{
		private PriorityQueue<Youtube> queue;
		private Comparator<Youtube> comp = new YoutubeComparator();
		private int topK;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString());
			String category = itr.nextToken().trim();
			double av_rate = Double.parseDouble(itr.nextToken().trim());
			insertYoutube(queue, category, av_rate, topK);

		}

		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Youtube>(topK, comp);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException{
			while(queue.size() != 0){
				Youtube youtube = (Youtube)queue.remove();
				context.write(new Text(youtube.getString()), NullWritable.get());
			}
		}
	
	}

	public static class YouTubeStudent20201018Reducer2 extends Reducer<Text, NullWritable, Text, NullWritable>{
		private PriorityQueue<Youtube> queue;
		private Comparator<Youtube> comp = new YoutubeComparator();
		private int topK;
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
			StringTokenizer itr2 = new StringTokenizer(key.toString());
			String category = itr2.nextToken().trim();
			double av_rate = Double.parseDouble(itr2.nextToken().trim());
			insertYoutube(queue, category, av_rate, topK);
		
		}
		
		protected void setup(Context context) throws IOException, InterruptedException{
                        Configuration conf = context.getConfiguration();
                        topK = conf.getInt("topK", -1);
                        queue = new PriorityQueue<Youtube>(topK, comp);
                }

                protected void cleanup(Context context) throws IOException, InterruptedException{
                        while(queue.size() != 0){
                                Youtube youtube = (Youtube)queue.remove();
				String k = youtube.getString();
			//	String key = k.replace("|"," ");
                                context.write(new Text(k), NullWritable.get());
                        }
                }
	
	}


	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Path path = new Path(otherArgs[2]);
		int topK = Integer.parseInt(path.toString());
		String first_phase_result = "/first_phase_result";
		if(otherArgs.length != 3){
			System.err.println("Usage: YouTubeStudent20201018 <in> <out> <topK>");
			System.exit(2);
		}
		conf.setInt("topK", topK);	
		Job job1 = new Job(conf, "YouTubeStudent20201018 1");
		job1.setJarByClass(YouTubeStudent20201018.class);
		job1.setMapperClass(YouTubeStudent20201018Mapper1.class);
		job1.setReducerClass(YouTubeStudent20201018Reducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		//FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job1, new Path(first_phase_result));
		//FileSystem.get(job1.getConfiguration()).delete(new Path(otherArgs[1]), true);
                FileSystem.get(job1.getConfiguration()).delete(new Path(first_phase_result), true);
		job1.waitForCompletion(true);
		
		Job job2 = new Job(conf, "YouTubeStudent20201018 2");
                job2.setJarByClass(YouTubeStudent20201018.class);
                job2.setMapperClass(YouTubeStudent20201018Mapper2.class);
                job2.setReducerClass(YouTubeStudent20201018Reducer2.class);
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
