package mapreduce.flowcount.sort;

import mapreduce.flowcount.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 李斌
 */
public class FlowSortMappper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");

        flowBean.set(line[0], Long.parseLong(line[2]), Long.parseLong(line[3]));

        context.write(flowBean, NullWritable.get());
    }
}
