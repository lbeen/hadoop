package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * java版搜狗访问日志统计
 *
 * @author 李斌
 */
public class JavaSougouQa {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("javaWordCount").setMaster("hadoop1"));

        //读取符合要求的行
        JavaRDD<String[]> lines = sc.textFile(args[0]).map(text -> text.split("\t")).filter(line -> line.length == 6);

        //统计每个id访问数
        //val counted = lines.map(line => (line(1), 1)).reduceByKey(_+_).sortBy(_._2)
        JavaPairRDD<String, Integer> counted = lines.mapToPair(line -> new Tuple2<>(line[1], 1)).reduceByKey((c1, c2) -> c1 + c2);

        //访问数排序后去除访问数大于20的数据
        JavaPairRDD<String, Integer> sorted = counted.mapToPair(Tuple2::swap).sortByKey().filter(tuple -> tuple._1 > 20).mapToPair(Tuple2::swap);

        //保存结果
        sorted.saveAsTextFile(args[1]);

        //停止spack任务
        sc.stop();
    }
}
