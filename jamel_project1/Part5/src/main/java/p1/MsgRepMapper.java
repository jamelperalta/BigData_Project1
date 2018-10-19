package p1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;

import java.io.IOException;

/**
 * Jamel Peralta Coss
 */
public class MsgRepMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            long msgIdReply = status.getId();
            long msgReplied = status.getInReplyToUserId();

            if(msgReplied != -1){
              String strMsgIdReply = Long.toString(msgIdReply);
              context.write(new LongWritable(msgReplied), new Text(strMsgIdReply));
            }
        }

        catch(TwitterException e) {

        }
    }
}
