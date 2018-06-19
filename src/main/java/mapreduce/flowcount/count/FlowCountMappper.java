package mapreduce.flowcount.count;

import mapreduce.flowcount.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 李斌
 */
public class FlowCountMappper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        int len = line.length;

        flowBean.set(line[1], Long.parseLong(line[len - 3]), Long.parseLong(line[len - 2]));

        context.write(new Text(line[1]), flowBean);
    }
}
