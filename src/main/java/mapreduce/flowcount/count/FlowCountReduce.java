package mapreduce.flowcount.count;

import mapreduce.flowcount.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 李斌
 */
public class FlowCountReduce extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean flowBean = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlow = 0;
        long downFlow = 0;

        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
        }

        flowBean.set(key.toString(), upFlow, downFlow);

        context.write(key, flowBean);
    }
}
