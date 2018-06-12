package mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class ProductMatrix {

	public static class Cell implements WritableComparable<Cell> {

		private String matr;
		private Integer row;
		private Integer col;

		public Cell() {
		}

		public Cell(String matr, Integer row, Integer col) {
			super();
			this.matr = matr;
			this.row = row;
			this.col = col;
		}

		@Override
		public void readFields(DataInput dataInput) throws IOException {
			matr = WritableUtils.readString(dataInput);
			row = WritableUtils.readVInt(dataInput);
			col = WritableUtils.readVInt(dataInput);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, matr);
			WritableUtils.writeVInt(out, row);
			WritableUtils.writeVInt(out, col);
		}

		@Override
		public int compareTo(Cell o) {
			int cmp = matr.compareTo(o.matr);
			if (cmp == 0) {
				cmp = row.compareTo(o.row);
				if (cmp == 0)
					cmp = col.compareTo(o.col);
			}
			return cmp;
		}

		@Override
		public String toString() {
			return matr + "\t" + row + "\t" + col;
		}

		public String getMatr() {
			return matr;
		}

		public Integer getRow() {
			return row;
		}

		public Integer getCol() {
			return col;
		}

	}

	@SuppressWarnings("rawtypes")
	public static class CellComparator extends WritableComparator {

		protected CellComparator() {
			super(Cell.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Cell ac = (Cell) a;
			Cell bc = (Cell) b;
			return ac.compareTo(bc);
		}
	}

	@SuppressWarnings("rawtypes")
	public static class CellGroupComparator extends WritableComparator {

		protected CellGroupComparator() {
			super(Cell.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Cell ac = (Cell) a;
			Cell bc = (Cell) b;
			if (ac.getCol() == bc.getCol() && ac.getRow() == bc.getRow())
				/** Row and Col are the same */
				return 0;
			return 1;
		}

	}

	public static class ProductMatrixMapper_1 extends Mapper<LongWritable, Text, Cell, IntWritable> {
		private Map<String, Integer> countRow = new HashMap<String, Integer>();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			filename = filename.substring(0, filename.lastIndexOf('.'));
			String[] tok = value.toString().split("\t");
			if (countRow.containsKey(filename))
				countRow.put(filename, countRow.get(filename) + 1);
			else
				countRow.put(filename, 0);
			for (int i = 0; i < tok.length; i++) {
				Cell c = new Cell(filename, countRow.get(filename), i);
				context.write(c, new IntWritable(Integer.parseInt(tok[i])));
			}
		}

	}

	public static class ProductMatrixMapper_2 extends Mapper<LongWritable, Text, Cell, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tok = value.toString().split("\t");
			Cell c = new Cell("new", Integer.parseInt(tok[1]), Integer.parseInt(tok[2]));
			context.write(c, new IntWritable(Integer.parseInt(tok[3])));
		}

	}

	public static class ProductMatrixMapper_3 extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tok = value.toString().split("\t");
			context.write(new LongWritable(Integer.parseInt(tok[0])), new IntWritable(Integer.parseInt(tok[1])));
		}
	}

	public static class ProductMatrixReducer_1 extends Reducer<Cell, IntWritable, Cell, IntWritable> {

		// switch row and column of second matrix data
		@Override
		protected void reduce(Cell key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			if (key.getMatr().equals("m2")) {
				Cell tmp = new Cell(key.getMatr(), key.getCol(), key.getRow());
				for (IntWritable intWritable : values) {
					// we are sure that there is only one value
					context.write(tmp, intWritable);
				}
			} else
				for (IntWritable intWritable : values) {
					// we are sure that there is only one value
					context.write(key, intWritable);
				}
		}
	}

	public static class ProductMatrixReducer_2 extends Reducer<Cell, IntWritable, LongWritable, Text> {

		// switch row and column of second matrix data
		@Override
		protected void reduce(Cell key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder str = new StringBuilder("");
			str.append("[");
			int product = 1;
			for (IntWritable writable : values)
				product *= writable.get();
			str.append("]");
			context.write(new LongWritable(key.getRow()), new Text(String.valueOf(product)));
		}
	}

	public static class ProductMatrixReducer_3 extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

		// switch row and column of second matrix data
		@Override
		protected void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable writable : values) {
				sum += writable.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Job1");

		job1.setMapperClass(ProductMatrixMapper_1.class);
		job1.setSortComparatorClass(CellComparator.class);
		job1.setReducerClass(ProductMatrixReducer_1.class);

		job1.setMapOutputKeyClass(Cell.class);
		job1.setMapOutputValueClass(IntWritable.class);

		job1.setOutputKeyClass(Cell.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/initial"));

		boolean success = job1.waitForCompletion(true);
		if (success) {
			Job job2 = Job.getInstance(conf, "Job2");

			job2.setMapperClass(ProductMatrixMapper_2.class);
			job2.setSortComparatorClass(CellComparator.class);
			job2.setReducerClass(ProductMatrixReducer_2.class);
			job2.setGroupingComparatorClass(CellGroupComparator.class);

			job2.setMapOutputKeyClass(Cell.class);
			job2.setMapOutputValueClass(IntWritable.class);

			job2.setOutputKeyClass(LongWritable.class);
			job2.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job2, new Path(args[1] + "/initial"));
			FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/middle"));

			success = job2.waitForCompletion(true);
			if (success) {
				Job job3 = Job.getInstance(conf, "Job3");

				job3.setMapperClass(ProductMatrixMapper_3.class);
				job3.setReducerClass(ProductMatrixReducer_3.class);

				job3.setMapOutputKeyClass(LongWritable.class);
				job3.setMapOutputValueClass(IntWritable.class);
				
				job3.setOutputKeyClass(LongWritable.class);
				job3.setOutputValueClass(IntWritable.class);

				FileInputFormat.addInputPath(job3, new Path(args[1] + "/middle"));
				FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/final"));

				success = job3.waitForCompletion(true);

			}

		}
		if (success)
			System.exit(1);
		else
			System.exit(0);
	}
}
