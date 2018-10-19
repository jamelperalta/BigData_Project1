package p1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Jamel Peralta Coss
 */
public class CountUserMsgReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;
        String msgs = " ";

        for (Text value : values){
            msgs = msgs.concat(value.toString()) + " ";
            count++;
        }

        String strCount = Integer.toString(count);
        msgs = msgs.concat(strCount);

        context.write(key, new Text(msgs));
    }
}
