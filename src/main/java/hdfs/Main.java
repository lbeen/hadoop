package hdfs;

import hdfs.util.HdfsClient;
import org.apache.hadoop.fs.FileStatus;
import org.junit.Test;


/**
 * @author 李斌
 */
public class Main {

    @Test
    public void listFile() throws Exception {
        FileStatus[] files = HdfsClient.listFile("/");
        for (FileStatus file : files) {
            System.out.println(file);
        }
    }

    @Test
    public void removeFile() throws Exception {
        HdfsClient.removeFile("/02.avi");
    }

    @Test
    public void downloadFile() throws Exception {
        HdfsClient.downLoad("/01.avi", "F:/01.avi");
    }

    @Test
    public void uploadFile() throws Exception {
        HdfsClient.updload("F:/test.data", "/wordcount/test.data");
    }
}
