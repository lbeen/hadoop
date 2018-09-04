package hadoop.mapreduce.wrodcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Random;

/**
 * test hadoop.mapreduce
 *
 * @author 李斌
 */
public class Main {

    @Test
    public void creatTestFile() throws Exception {
        OutputStream os = new FileOutputStream("F:/src.data");
        Random random = new Random();

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            for (int j = 0; j < 10; j++) {
                sb.append((char) (random.nextInt(26) + 65)).append(",");
            }
            sb.append("\n");
        }

        os.write(sb.toString().getBytes());
        os.flush();
        os.close();
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Main.class);

        job.setMapperClass(WordCountMapper.class);

        job.setCombinerClass(WordCountReduce.class);

        job.setReducerClass(WordCountReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, "hadoop.hdfs://ns1/wc/src.data");
        FileOutputFormat.setOutputPath(job, new Path("hadoop.hdfs://ns1/wc/result"));

        boolean res = job.waitForCompletion(true);

        System.out.println(res);
    }
}
