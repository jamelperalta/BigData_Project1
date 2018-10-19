package p1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

/**
 * Jamel Peralta Coss
 */
public class StopWordsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String rawTweet = value.toString();
      try {
          Status status = TwitterObjectFactory.createStatus(rawTweet);
          String tweet = status.getText().toUpperCase().replaceAll("[^A-Z ]", "");
          String[] stopWords = {"YOU", "HIS", "ABOUT", "WHAT", "NOT", "THIS", "A", "AN", "AND", "ARE", "AS", "AT", "BE", "BY", "FOR", "FROM", "HAS", "HE", "IN", "IS", "IT", "ITS", "OF", "ON", "THAT", "THE", "TO", "WAS", "WERE", "WILL", "WITH"};
          String[] temp;
          temp = tweet.split(" ");
          for (String aTemp : temp) {
              if (aTemp.length() > 2) {
                  boolean same = false;
                  String word = "";
                  for (String stopWord : stopWords) {
                      // Check if is equal to stopWord then dont include it
                      if (stopWord.equalsIgnoreCase(aTemp)) {
                          same = true;
                      }
                      word = aTemp;
                  }
                  if (!same) {
                      context.write(new Text(word), new IntWritable(1));
                  }
              }

          }
      }catch(TwitterException e){
      }
  }

}
