package hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;

/**
 * @author 李斌
 */
public class HbaseDao {

    public static void main(String[] args) throws Exception {

        //hBaseAdmin对表进行管理的客户端
        Admin admin = HbaseUtil.getAdmin();

        TableName tableName = TableName.valueOf("mygirls");
        HTableDescriptor htd = new HTableDescriptor(tableName);

        //对列族的描述
        HColumnDescriptor base_info = new HColumnDescriptor("base_info");
        base_info.setMaxVersions(3);


        //在表描述器中添加列族
        htd.addFamily(base_info);

        admin.createTable(htd);

    }
}
