import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 */

/**
 * @author training
 * 
 */
public class AllTimeHighDriver {

	public static class AllTimeHighMapper extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context ctx) {
			String[] str = value.toString().split(",");
			Double val = Double.parseDouble(str[4]);
			try {
				ctx.write(new Text(str[1]), new DoubleWritable(val));
			} catch (IOException | InterruptedException e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public static class AllTimeHighReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		DoubleWritable result = new DoubleWritable();
		
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context ctx) {
			double max = 0;
			for (DoubleWritable value : values) {
				if (max < value.get()) {
					max = value.get();
				}
			}
			result.set(max);
			try {
				ctx.write(key, result);
			} catch (IOException | InterruptedException e) {
				System.out.println(e.getMessage());
			}
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "All Time High");
		job.setJarByClass(AllTimeHighDriver.class);
		job.setMapperClass(AllTimeHighMapper.class);
		job.setReducerClass(AllTimeHighReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
