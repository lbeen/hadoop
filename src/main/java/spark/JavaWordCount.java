package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * java版sparkWordCount
 *
 * @author 李斌
 */
public class JavaWordCount {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("javaWordCount"));

        //读取文件并切割成单词
        JavaRDD<String> words = sc.textFile(args[0]).flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        //计算每个单词数量
        JavaPairRDD<String, Integer> counted = words.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((c1, c2) -> c1 + c2);

        //排序（javaApi只能按key排序，先把元组倒转，排序后在到转回来）
        JavaPairRDD<String, Integer> sorted = counted.mapToPair(Tuple2::swap).sortByKey().mapToPair(Tuple2::swap);

        //保存结果
        sorted.saveAsTextFile(args[1]);

        //停止spack任务
        sc.stop();
    }
}
