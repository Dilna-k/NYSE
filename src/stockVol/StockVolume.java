package stockVol;

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

public class StockVolume {
	public static class MapClass extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		public void map(LongWritable key, Text value, Context context) {
			try {
				String line = value.toString();
				String[] lineParts = line.split(",");

				long vol = Long.parseLong(lineParts[7]);
				context.write(new Text(lineParts[1]), new LongWritable(vol));
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}
	//1) to find out the total volume for each stock
	//2) to find out the max volume for each stock,
	//3) to find the average daily volume for each stock
	//4) to find the least volume for each stock
		public static class ReduceClass extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		// private LongWritable result = new LongWritable();
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			long sum = 0;
			long max = 0;
			long current = 0;
			long count = 0;
			long min = 999999999;
			for (LongWritable val : values) {
				sum += val.get();
				current = val.get();
				if(current>max)
				{
					max =current;
				}
				count++;
				if(current < min)
				{
					min = current;
				}
			}
				long average = sum/count;
				System.out.println("");
				context.write(key, new LongWritable(sum));
				context.write(key, new LongWritable(max));
				context.write(key, new LongWritable(average));
				context.write(key, new LongWritable(min));
				
				
			// result.set(sum);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Volume count");
		job.setJarByClass(StockVolume.class);
		// job.setJarByClass(MapClass.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}