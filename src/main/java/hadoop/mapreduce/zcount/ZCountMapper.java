package hadoop.mapreduce.zcount;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * @author 李斌
 */
public class ZCountMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value != null) {
            String[] words = StringUtils.split(value.toString());
            context.write(new Text(words[0]), new DoubleWritable(Double.valueOf(words[2])));
        }
    }
}
