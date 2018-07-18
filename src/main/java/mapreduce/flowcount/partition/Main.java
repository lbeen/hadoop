package mapreduce.flowcount.partition;

import mapreduce.flowcount.FlowBean;
import mapreduce.flowcount.count.FlowCountMappper;
import mapreduce.flowcount.count.FlowCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * test mapreduce
 *
 * @author 李斌
 */
public class Main {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Main.class);

        job.setMapperClass(FlowCountMappper.class);

        job.setReducerClass(FlowCountReduce.class);

        //设置分组类和reducetask数量，
        //分组类小于reducetask数量，会生成空文件
        //分组类大于reducetask数量，会报错
        job.setPartitionerClass(AreaPartitioner.class);
        job.setNumReduceTasks(4);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, "/fc/http.data");
        FileOutputFormat.setOutputPath(job, new Path("/fc/result"));

        boolean res = job.waitForCompletion(true);

        System.out.println(res);
    }
}
