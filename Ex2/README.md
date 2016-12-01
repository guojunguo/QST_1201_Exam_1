
题目2：编写MapReduce，统计`/user/hadoop/mapred_dev/ip_time` 中去重后的IP数，越节省性能越好。（35分）

---package projecttest;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class QuChong {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		public void map (LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String [] a = line.split("\t");
			String ip = a[0];
			context.write(new Text(ip), new Text(""));
		}
	}
	
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		private static int ipcount = 0;	
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			ipcount++;
			context.write(key, new Text(""));
		}
		@Override
		public void cleanup(Context context) {
			// TODO Auto-generated method stub
			Counter counter = (Counter) context.getCounter("Custom Static", "Unique IP");
			System.out.println("This has "+ipcount);
			counter.increment(ipcount);
		}
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();  
		Job job = Job.getInstance(conf, "Ray" + QuChong.class);
		job.setJarByClass(QuChong.class);
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(2);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		return;
		
	}
}


运行完之后，描述程序里所做的优化点，每点+5分。
