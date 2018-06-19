package mapreduce.flowcount.count;

import mapreduce.flowcount.FlowBean;
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

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, "/tmp/fc/http.data");
        FileOutputFormat.setOutputPath(job, new Path("/tmp/fc/result"));

        boolean res = job.waitForCompletion(true);

        System.out.println(res);
    }
}
