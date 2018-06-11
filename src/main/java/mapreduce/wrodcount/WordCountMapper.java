package mapreduce.wrodcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * @author 李斌
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       if (value != null){
           String[] words = StringUtils.split(value.toString());
           for (String word : words) {
               context.write(new Text(word), new LongWritable(1));
           }
       }
    }
}
