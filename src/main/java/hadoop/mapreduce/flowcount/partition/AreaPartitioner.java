package hadoop.mapreduce.flowcount.partition;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author 李斌
 */
public class AreaPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE> {
    @Override
    public int getPartition(KEY key, VALUE value, int numPartitions) {
        String prefix = key.toString().substring(0, 3);
        switch (prefix) {
            case "136":
                return 1;
            case "139":
                return 2;
            case "137":
                return 3;
            default:
                return 0;

        }
    }
}
