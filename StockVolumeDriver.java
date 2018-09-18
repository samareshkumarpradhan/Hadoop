import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author training
 * 
 */
public class StockVolumeDriver {

	public static class StocKVolumeMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		@Override
		public void map(LongWritable key, Text value, Context ctx) {
			try {
				String[] str = value.toString().split(",");
				long vol = Long.parseLong(str[7]);
				ctx.write(new Text(str[1]), new LongWritable(vol));
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public static class StockVolumeReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context ctx) {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
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
		Job job = Job.getInstance(conf, "Stock Volume");
		job.setJarByClass(StockVolumeDriver.class);
		job.setMapperClass(StocKVolumeMapper.class);
		job.setReducerClass(StockVolumeReducer.class); 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
