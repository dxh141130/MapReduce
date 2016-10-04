import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;


public class MutualFriendList {
	/**
	 * @version 1.7.0
	 * @author dxh141130
	 * @date 9/28/2016
	 */
	//Map Part
	public static class Map extends Mapper<LongWritable,Text,Text,Text>{
		/**
		 * @override map
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException{
			String[] pair = value.toString().trim().split("\\t");
			//check whether the user has no friend
            if(pair.length!=2)
                return;
			String userId = pair[0];
			String[] friendList = pair[1].trim().split(",");
			for(int i = 0; i < friendList.length;i++){
				String outputKey;
				if(Integer.parseInt(userId)<Integer.parseInt(friendList[i])){
					outputKey = userId+","+friendList[i];
				}
				else{
					outputKey = friendList[i]+","+userId;
				}
				context.write(new Text(outputKey),new Text(pair[1]));
			}
		}
	}

	//Reduce Part
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		/**
		 * @override reduce
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> it = values.iterator();
			List<String> saveFriend = new LinkedList<>();
			List<String> mutualFriend = new LinkedList<>();
			while (it.hasNext()) {
				Text list = it.next();
				String[] split = list.toString().trim().split(",");
				for (String s : split) {
					if (!saveFriend.contains(s)) {
						saveFriend.add(s);
					} else {
						mutualFriend.add(s);
					}
				}
			}
			//check whether the User A and B has no mutual friend
			if(mutualFriend.size()==0)
				return;
			StringBuilder sb= new StringBuilder();
			for(String str:mutualFriend)
				sb.append(str).append(",");
			sb.delete(sb.length()-1, sb.length());
			context.write(new Text(key), new Text(sb.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] parameter = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(parameter.length !=2){
			System.err.println("Usage: Parameter <in><out>");
			System.exit(2);
		}
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(parameter[1]),true);
		Job job = new Job(conf,"Parameter");
		
		job.setJarByClass(MutualFriendList.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
