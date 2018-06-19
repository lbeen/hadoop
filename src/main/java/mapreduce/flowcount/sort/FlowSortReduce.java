package mapreduce.flowcount.sort;

import mapreduce.flowcount.FlowBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 李斌
 */
public class FlowSortReduce extends Reducer<FlowBean, NullWritable, Text, FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(new Text(key.getPhone()), key);
    }
}
