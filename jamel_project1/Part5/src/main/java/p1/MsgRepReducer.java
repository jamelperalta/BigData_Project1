package p1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Jamel Peralta Coss
 */
public class MsgRepReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String ids = " ";
        for (Text value : values){
            ids = ids.concat(value.toString()) + " ";
        }

        context.write(key, new Text(ids));
    }
}
