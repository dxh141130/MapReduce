/**
 * Created by X99 on 9/28/2016.
 */
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
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

    public static class Map extends Mapper<LongWritable,Text,Text,Text>{
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{
            String[] pair = value.toString().trim().split("\\t");
            if(pair.length!=2)
                return;
            String userId = pair[0];

//            if(pair[0].equals("1")) {
//                System.out.println("-------------------------------------------------------");
//                System.out.println("Id: " + userId + "   --->FriendList: " + pair[1]);
//                System.out.println("-------------------------------------------------------");
//            }
            String[] friendList = pair[1].trim().split(",");
//
//            for(String s:friendList)
//            System.out.print(s+",");
//            System.out.println();
            for(int i = 0; i < friendList.length;i++){
                String outputKey;
                if(Integer.parseInt(userId)<Integer.parseInt(friendList[i])){
                    outputKey = userId+","+friendList[i];
                }
                else{
                    outputKey = friendList[i]+","+userId;
                }
//                if(pair[0].equals("1")&&friendList[i].equals("29826")||pair[0].equals("29826")&&friendList[i].equals("1")) {
//                    System.out.println("-------------------------------------------------------");
//                    System.out.println("Id: " + userId + "   --->pairSet: " + outputKey+"  , and context in reduce: "+pair[1]);
//                    System.out.println("-------------------------------------------------------");
//                }
                context.write(new Text(outputKey),new Text(pair[1]));
//                System.out.println(pair[1]);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> it = values.iterator();
//            if(key.toString().equals("1,29826")) {
//                System.out.println("-------------------------------------------------------");
//                System.out.println("ReduceId: " + key.toString() + "   --->pairSet: " + it.next() + "and "+it.next());
//                System.out.println("-------------------------------------------------------");
//            }

//            System.out.print("key: " + key+" ----->valueSet: ");
//            System.out.println()
//            while (it.hasNext()) {
//                System.out.print(it.next().toString()+" || ");
//            }
//            System.out.println();
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
