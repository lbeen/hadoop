package hbase.inputmr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.BasicConfigurator;

/**
 * @author 李斌
 */
public class Main {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration configuration = new Configuration();
        configuration.set("hbase.rootdir", "hdfs://ns1/hbase");
        configuration.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
        configuration.set(TableOutputFormat.OUTPUT_TABLE, "WR_INFO");

        Job job = Job.getInstance(configuration);
        job.setJarByClass(Main.class);

        job.setMapperClass(InputMapper.class);
        job.setReducerClass(InputReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPaths(job, "hdfs://ns1/hivedata/wr/src.data");

        job.setOutputFormatClass(TableOutputFormat.class);

        job.waitForCompletion(true);
    }
}
