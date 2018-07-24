package hbase.inputmr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author 李斌
 */
public class InputReducer extends TableReducer<Text, Text, NullWritable> {
    private byte[] family = Bytes.toBytes("base_info");

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] splits = value.toString().split(",");
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(family, Bytes.toBytes("sctd"), Bytes.toBytes(splits[0]));
            put.addColumn(family, Bytes.toBytes("tm"), Bytes.toBytes(splits[1]));
            put.addColumn(family, Bytes.toBytes("r"), Bytes.toBytes(splits[2]));
            put.addColumn(family, Bytes.toBytes("ts"), Bytes.toBytes(splits[4]));

            context.write(NullWritable.get(), put);
        }
    }

}
