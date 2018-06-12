package mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinMax {

	public static class Pair implements WritableComparable<Pair> {

		private String key;
		private String value;

		public Pair() {
		}

		public Pair(String key, String value) {
			super();
			this.value = value;
			this.key = key;
		}

		@Override
		public void readFields(DataInput dataInput) throws IOException {
			key = WritableUtils.readString(dataInput);
			value = WritableUtils.readString(dataInput);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, key);
			WritableUtils.writeString(out, value);
		}

		@Override
		public int compareTo(Pair o) {
			int cmp = value.compareTo(o.value);
			if (cmp == 0) {
				cmp = key.compareTo(o.key);
			}
			return cmp;
		}

		@Override
		public String toString() {
			return String.format("[key: %s value: %s]", key, value);
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}

	}

	@SuppressWarnings("rawtypes")
	public static class PairComparator extends WritableComparator {

		protected PairComparator() {
			super(Pair.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Pair ac = (Pair) a;
			Pair bc = (Pair) b;
			return -ac.compareTo(bc);
		}
	}

	static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] row = value.toString().split(",");
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			StringBuilder str = new StringBuilder();
			for (int i = 1; i < row.length; i++) {
				if (filename.contains("cust")) {
					if (i != 0 && i <= 2) {
						str.append(row[i]);
						str.append("\t");
					}
				} else {
					if (i != 2) {
						str.append(row[i]);
						str.append("\t");
					}
				}
			}

			if (filename.contains("cust")) {
				str.append("file1");
				context.write(new Text(row[0]), new Text(str.toString()));
			} else {
				str.append("file2");
				context.write(new Text(row[2]), new Text(str.toString()));
			}
		}
	}

	static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

		}

		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			List<String> list = new ArrayList<>();
			for (Text text : value) {
				list.add(text.toString());
			}
			String file1 = "";
			List<String> file2 = new ArrayList<>();
			for (String str : list) {
				String str_temp = new String(str.trim());
				if (str.contains("file1")) {
					str_temp = str_temp.replace("file1", "");
					file1 = str_temp;
				} else if (str.contains("file2")) {
					str_temp = str_temp.replace("file2", "");
					file2.add(str_temp);
				}
			}
			for (String s : file2) {
				context.write(key, new Text(file1 + " " + s));
			}
		}
	}

	static class MaxMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] row = value.toString().split("\\s+");
			StringBuilder str = new StringBuilder("");
			for (int i = 1; i < row.length; i++) {
				str.append(row[i]);
				if (i < row.length - 1)
					str.append("\t");
			}
			context.write(new Text(row[0]), new Text(str.toString()));
		}
	}

	static class MaxReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

		}

		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			double importValue = Float.MIN_VALUE;
			String textValue = "";
			List<String> list = new ArrayList<>();
			for (Text t : value) {
				list.add(t.toString());
			}
			for (String str : list) {
				String[] val = str.split("\t");
				if (Float.parseFloat(val[3]) > importValue) {
					importValue = Float.parseFloat(val[3]);
					textValue = str;
				}
			}
			context.write(new Text(key), new Text(textValue));
		}
	}

	static class SortMapper extends Mapper<LongWritable, Text, Pair, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] row = value.toString().split("\t");
			StringBuilder str = new StringBuilder("");
			for (int i = 1; i < row.length; i++) {
				str.append(row[i]);

				if (i < row.length - 1)
					str.append("\t");
			}
			context.write(new Pair(row[0], row[4]), new Text(str.toString()));
		}
	}

	static class SortReduce extends Reducer<Pair, Text, Text, Text> {
		static int counter = 0;

		@Override
		protected void reduce(Pair key, Iterable<Text> values, Reducer<Pair, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			counter++;
			StringBuilder str = new StringBuilder("");
			for (Text t : values) {
				str.append(t);
			}
			if (counter <= 5)
				context.write(new Text(key.toString()), new Text(str.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Job1");

		job1.setMapperClass(JoinMapper.class);
		job1.setReducerClass(JoinReducer.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/initial"));

		boolean success = job1.waitForCompletion(true);
		if (success) {
			Job job2 = Job.getInstance(conf, "Job2");

			job2.setMapperClass(MaxMapper.class);
			job2.setReducerClass(MaxReducer.class);

			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job2, new Path(args[1] + "/initial"));
			FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/middle"));

			success = job2.waitForCompletion(true);
			if (success) {

				Job job3 = Job.getInstance(conf, "Job3");

				job3.setMapperClass(SortMapper.class);
				job3.setReducerClass(SortReduce.class);
				job3.setSortComparatorClass(PairComparator.class);

				job3.setMapOutputKeyClass(Pair.class);
				job3.setMapOutputValueClass(Text.class);

				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(Text.class);

				FileInputFormat.addInputPath(job3, new Path(args[1] + "/middle"));
				FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/final"));

				success = job3.waitForCompletion(true);
				if (!success) {
					System.exit(0);
				}
			} else {
				System.exit(0);
			}
		} else {
			System.exit(0);
		}

	}
}
