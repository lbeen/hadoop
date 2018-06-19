package hdfs;

import hdfs.util.HdfsClient;
import org.apache.hadoop.fs.FileStatus;
import org.junit.Test;


/**
 * test hdfs
 *
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
        HdfsClient.removeFile("/wc");
    }

    @Test
    public void downloadFile() throws Exception {
        HdfsClient.downLoad("/wc/test.data", "/tmp/test.data");
    }

    @Test
    public void uploadFile() throws Exception {
        HdfsClient.updload("/tmp/test.data", "/wc/test.data");
    }
}
