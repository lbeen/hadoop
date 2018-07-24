package hbase.inputmr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 李斌
 */
public class InputMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split(",");
        String rowKey = "wr_pk_" + arr[0] + "_" + arr[1];
        Text outputValue = new Text(rowKey + "\t" + value.toString());
        context.write(new Text(rowKey), outputValue);
    }
}
