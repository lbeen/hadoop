package mapreduce.zcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

        job.setMapperClass(ZCountMapper.class);

//        job.setCombinerClass(ZCountReduce.class);

        job.setReducerClass(ZCountReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, "hdfs://ns1/zc/src.data");
        FileOutputFormat.setOutputPath(job, new Path("hdfs://ns1/zc/result"));

        boolean res = job.waitForCompletion(true);

        System.out.println(res);
    }
}
