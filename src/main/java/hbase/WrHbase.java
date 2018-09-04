package hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 李斌
 */
public class WrHbase {

    @Test
    public void creatTable() throws Exception {
        Admin admin = HbaseUtil.getAdmin();
        TableName tableName = TableName.valueOf("WR_INFO");
        TableDescriptorBuilder tBuilder = TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptorBuilder cBuder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("base_info"));
        cBuder.setMaxVersions(3);
        tBuilder.setColumnFamily(cBuder.build());
        admin.createTable(tBuilder.build());
        admin.close();
    }

    @Test
    public void inputData() throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader("/home/hadoop/Documents/src.data"));
        Connection connection = HbaseUtil.getConnection();
        TableName tableName = TableName.valueOf("WR_INFO");
        Table table = connection.getTable(tableName);
        byte[] family = "base_info".getBytes();
        List<Put> puts = new ArrayList<>(10000);
        int count = 0;
        int total = 0;

        String line;
        while ((line = reader.readLine()) != null) {
            String[] arr = line.split(",");
            String row = "wr_pk_" + arr[0] + "_" + arr[1];
            Put p = new Put(Bytes.toBytes(row));
            p.addColumn(family, Bytes.toBytes("sctd"), Bytes.toBytes(arr[0]));
            p.addColumn(family, Bytes.toBytes("tm"), Bytes.toBytes(arr[1]));
            p.addColumn(family, Bytes.toBytes("r"), Bytes.toBytes(Double.valueOf(arr[2])));
            p.addColumn(family, Bytes.toBytes("ts"), Bytes.toBytes(arr[4]));

            puts.add(p);
            count++;

            if (count == 10000) {
                table.put(puts);
                puts.clear();
                total += count;
                count = 0;
                System.out.println(total);
            }
        }

        if (count > 0) {
            table.put(puts);
        }
        table.close();
    }

    @Test
    public void truncate() throws Exception {
        Admin admin = HbaseUtil.getAdmin();
        TableName tableName = TableName.valueOf("WR_INFO");
        admin.disableTable(tableName);
        admin.truncateTable(tableName, false);
        admin.close();
    }

}
