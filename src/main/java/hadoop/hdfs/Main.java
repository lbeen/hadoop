package hadoop.hdfs;

import hadoop.hdfs.util.HdfsClient;
import org.apache.hadoop.fs.FileStatus;
import org.junit.Test;


/**
 * test hadoop.hdfs
 *
 * @author 李斌
 */
public class Main {

    @Test
    public void listFile() throws Exception {
        FileStatus[] files = HdfsClient.listFile("/spark_test");
        for (FileStatus file : files) {
            System.out.println(file);
        }
    }

    @Test
    public void removeFile() throws Exception {
        HdfsClient.removeFile("/fc/http.data");
    }

    @Test
    public void downloadFile() throws Exception {
        HdfsClient.downLoad("/fc/http.data", "/tmp/fc/http.data");
    }

    @Test
    public void uploadFile() throws Exception {
        HdfsClient.updload("/tmp/http.data", "/fc/http.data");
    }
}
