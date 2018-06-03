package hdfs.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @author 李斌
 */
public class HdfsClient {
    private static FileSystem FS = null;

    public static FileStatus[] listFile(String hdfsPath) throws Exception {
        return getFileSystem().listStatus(new Path(hdfsPath));
    }

    public static void updload(String localPath, String hdfsPath) throws Exception {
        getFileSystem().copyFromLocalFile(new Path(localPath), new Path(hdfsPath));
    }

    public static void downLoad(String hdfsPath, String localPath) throws Exception {
        getFileSystem().copyToLocalFile(false, new Path(hdfsPath), new Path(localPath), true);
    }

    public static void removeFile(String hdfsPath) throws Exception {
        getFileSystem().delete(new Path(hdfsPath), false);
    }

    private static FileSystem getFileSystem() throws Exception {
        if (FS == null) {
            synchronized (HdfsClient.class) {
                if (FS == null) {
                    synchronized (HdfsClient.class) {
                        Configuration conf = new Configuration();
                        conf.set("fs.defaultFS", "hdfs://192.168.1.112:9000/");
                        conf.set("dfs.replication", "1");
                        FS = FileSystem.get(new URI("hdfs://192.168.1.112:9000/"), conf, "root");
                    }
                }
            }
        }
        return FS;
    }


    public static void main(String[] args) throws Exception {
        FileSystem fs = getFileSystem();
//        FSDataInputStream is = fs.open(new Path("/01.avi"));
//        FileOutputStream os = new FileOutputStream("F:/01.avi");
//        IOUtils.copy(is, os);
        fs.copyFromLocalFile(new Path("F:/02.avi"), new Path("/02.avi"));
    }
}
