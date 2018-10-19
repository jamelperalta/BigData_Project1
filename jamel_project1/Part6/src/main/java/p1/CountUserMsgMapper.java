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
public class CountUserMsgMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            long msgId = status.getId();
            User user = status.getUser();
            String screenName = user.getScreenName();
            String strMsgId = Long.toString(msgId);

            context.write(new Text(screenName), new Text(strMsgId));
        }

        catch(TwitterException e) {

        }
    }
}
