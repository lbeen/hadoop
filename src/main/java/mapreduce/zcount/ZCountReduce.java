package mapreduce.zcount;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 李斌
 */
public class ZCountReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double count = 0;
        for (DoubleWritable value : values) {
            count += value.get();
        }
        context.write(key, new DoubleWritable(count));
    }
}
