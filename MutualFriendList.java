
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;


/**
 * Created by liyaoting on 9/21/16.
 */


public class MutualFriendList {
    public static class MutualFriendMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split("\\t");
            String userID = line[0];
            // List<String> friendList = new ArrayList<>();

            if (line.length == 2) {
                String[] friends = line[1].split(",");
                for (int i = 0; i < friends.length; i++) {
                    String friend = friends[i];
                    String outputKey;
                    if (friend.compareTo(userID) < 0) {
                        outputKey = friend + "," + userID;
                        //context.write(new Text(outputKey), new Text(line[1]));
                    } else {
                        outputKey = userID +"," + friend;
                        //context.write(new Text(outputKey), new Text(line[1]));
                    }
                    //String outputKey = order(userID, friend);
                    context.write(new Text(outputKey), new Text(line[1].trim()));
                }

            }
        }

    }


    public static class MutualFriendReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] inputUser = key.toString().split(",");
            String input = key.toString();
            //String inputUserA = String.valueOf(key.toString().charAt(0));
            if (inputUser.length == 2) {
                String inputUserA = key.toString().split(",")[0].trim();
                String inputUserB = key.toString().split(",")[1].trim();

                // Map<String, Integer> friendRecord = new HashMap<String, Integer>();

                List<String> friendList = new ArrayList<String>();
                List<String> result = new ArrayList<String>();
                Iterator<Text> iter = values.iterator();    //could delete if you dont need

                for (Text val : values) {
                    String friend = val.toString();            //"friend" is list of user friend
                    String[] friendid = friend.split(",");  //list split by ","
//                    System.out.println("friend: "+ friend);
                    for(String st :friendid) {          //
                        if (!friendList.contains(st)) {
                            friendList.add(st);
                        } else {
                            result.add(st);
                        }
                    }
                }
                    if(result.size()==0){   //because if inputUserA and inputUserB are not mutual friend, size =0,
                        return;
                }
                StringBuilder resultbuilder= new StringBuilder();
                for(String str:result)
                    resultbuilder.append(str).append(",");
                String list = resultbuilder.toString();
                String resultMessage = list.substring(0,list.length()-1);
                context.write(new Text(key.toString()), new Text(resultMessage));
            }

        }
    }



    // driver
    public static void main(String[] args) throws Exception{
        //System.out.println("try");
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // ???

        if (otherArgs.length != 2) {
            System.out.println("Usage: MutualFriend<in><out>");
            System.exit(2);
        }

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(otherArgs[1]), true);

        Job job = new Job(conf, "mutualFriend");
        job.setJarByClass(MutualFriendList.class);
        job.setMapperClass(MutualFriendMapper.class);
        job.setReducerClass(MutualFriendReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 :1);

    }

}
