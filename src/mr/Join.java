package mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Join {

	/**
	 * {@link LongWritable} the row id
	 */
	static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] row = value.toString().split(",");
			StringBuilder str = new StringBuilder();
			for (int i = 1; i < row.length; i++) {
				str.append(row[i]);
				str.append("\t");
			}

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			filename = filename.substring(0, filename.lastIndexOf("."));

			str.append(filename);
			context.write(new Text(row[0]), new Text(str.toString()));
		}
	}

	/**
	 * 
	 * @author Lorenzo
	 *
	 */
	static class JoinReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			
		}

		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			StringBuilder builder = new StringBuilder("");
			List<String> list = new ArrayList<>();
			for (Text text : value) {
				list.add(text.toString());
			}
			
			for (int i = 1; i <= 2; i++) {
				String file = "file" + i;
				for (String str : list) {
					String str_temp = new String(str.trim());
					if (str.contains(file)) {
						str_temp = str_temp.replace(file, " ");
						builder.append(str_temp);
						builder.append("\t");
					}
				}
			}
			context.write(key, new Text(builder.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MapReduce");

		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
