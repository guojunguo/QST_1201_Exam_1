题目3：编写MapReduce，统计这两个文件

`/user/hadoop/mapred_dev_double/ip_time`

`/user/hadoop/mapred_dev_double/ip_time_2`

当中，重复出现的IP的数量(40分)
package projecttest;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GongTongIP {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private HashSet<String> hs = new HashSet<String>();
		public void map (LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String [] arr = value.toString().split("\t");
			String IP = arr[0];
			if(hs.contains(IP)){
				context.write(new Text(IP), new Text(""));
			}
		}

		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			Path p = new Path(conf.get("getIP"));
			InputStream is = fs.open(p);
			Scanner sc = new Scanner(is);
			while(sc.hasNext()){
				String ip = sc.nextLine();
				hs.add(ip);
			}
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
		conf.set("getIP", args[2]);
		Job job = Job.getInstance(conf, "Ray");
		job.setJarByClass(GongTongIP.class);
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

---
加分项：

1. 写出程序里面考虑到的优化点，每个优化点+5分。
2. 额外使用Hive实现，+5分。
3. 额外使用HBase实现，+5分。
