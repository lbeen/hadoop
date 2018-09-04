package hadoop.hdfs.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * hdfsClient
 *
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
//                        conf.set("fs.defaultFS", "hadoop.hdfs://ns1/");
//                        conf.set("dfs.replication", "1");
                        FS = FileSystem.get(conf);
                    }
                }
            }
        }
        return FS;
    }
}
