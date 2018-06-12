package mr;

import java.io.IOException;
import java.util.StringTokenizer;

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

public class Max {
	
	/**
	 * 
	 * The TokenizerMapper tokenize the input 
	 * and add one for each value
	 *
	 */
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	
	
	/**
	 * 
	 * The IntSumReducer receives from TokenizerMapper
	 * and sum all same keys
	 *
	 */
	public static class IntSumReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	/**
	 * 
	 * The MaxMapper gets from MaxMapper result resend it
	 * to the new reducer
	 *
	 */
	public static class MaxMapper
	extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private Text word = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split("\t");
			word.set(words[0]);
			IntWritable i = new IntWritable(Integer.parseInt(words[1]));
			context.write(word,i);
		}
	}

	/**
	 * 
	 * The MaxReducer performs the maximum over the result
	 * of MaxMapper 
	 *
	 */
	public static class MaxReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		static int max = Integer.MIN_VALUE;
		static Text best_key = new Text();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			
			for (IntWritable val : values) {
				sum += val.get();
			}
			if(sum > max) {
				max = sum;
				best_key.set(key);
			}
		}
		
		/**
		 * It writes the final result when all reducers done
		 */
		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(best_key, new IntWritable(max));
		}
	}
	
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path startInput = new Path(args[0]);
		Path tempInput = new Path(args[1]);
		Path finalOutput = new Path(args[1]+"/o");
		
		Job jobCount = Job.getInstance(conf, "Word Count");
		
		jobCount.setMapperClass(TokenizerMapper.class);
		jobCount.setCombinerClass(IntSumReducer.class);
		jobCount.setReducerClass(IntSumReducer.class);
		
		jobCount.setOutputKeyClass(Text.class);
		jobCount.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(jobCount, startInput);
		FileOutputFormat.setOutputPath(jobCount, tempInput);
		
		boolean success = jobCount.waitForCompletion(true);
		if (success) {
			Job jobMax = Job.getInstance(conf, "Word Max");
			
			jobMax.setMapperClass(MaxMapper.class);
			jobMax.setReducerClass(MaxReducer.class);
			
			jobMax.setOutputKeyClass(Text.class);
			jobMax.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(jobMax, tempInput);
			FileOutputFormat.setOutputPath(jobMax, finalOutput);
			success = jobMax.waitForCompletion(true);
		}

		if (success)
			System.exit(1);
		else
			System.exit(0);
	}
}
