package hadoop.hbase;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HbaseDemo {
    @Test
    public void testDrop() throws Exception {
        //hBaseAdmin对表进行管理的客户端
        Admin admin = HbaseUtil.getAdmin();
        TableName tableName = TableName.valueOf("WR_INFO");
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        admin.close();
    }

    @Test
    public void testPut() throws Exception {
        Connection connection = HbaseUtil.getConnection();
        TableName tableName = TableName.valueOf("WR_INFO");
        Table table = connection.getTable(tableName);
        Put p = new Put(Bytes.toBytes("wr_pk_81800120_20081117071101"));
        p.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("stcd"), Bytes.toBytes("81800120"));
        p.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("r"), Bytes.toBytes(2));
        p.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("tm"), Bytes.toBytes("20081117071101"));
        table.put(p);
        table.close();
    }

    @Test
    public void testGet() throws Exception {
        Connection connection = HbaseUtil.getConnection();
        TableName tableName = TableName.valueOf("WR_INFO");
        Table table = connection.getTable(tableName);
        Get get = new Get(Bytes.toBytes("wr_pk_81800120_20081117071100"));
        get.readVersions(5);
        Result result = table.get(get);

        //可以从result中直接取出一个特定的value
//        byte[] value = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("r"));
//        System.out.println(Bytes.toString(value));

        //遍历出result中所有的键值对
        for (Cell cell : result.listCells()) {
            String family = new String(CellUtil.cloneFamily(cell));
            System.out.println(family);
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            System.out.println(qualifier);
            System.out.println(new String(CellUtil.cloneValue(cell)));

        }
        table.close();
    }

    /**
     * 多种过滤条件的使用方法
     */
    @Test
    public void testScan() throws Exception {
        Connection connection = HbaseUtil.getConnection();
        TableName tableName = TableName.valueOf("WR_INFO");
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();

//        scan.withStartRow(Bytes.toBytes("wr_pk_81800060_20131022091000"));
//        scan.withStopRow(Bytes.toBytes("wr_pk_81800060_20131205191200"));

        Filter filter;

        //前缀过滤器----针对行键
//        filter = new PrefixFilter(Bytes.toBytes("wr_pk"));

//        //行过滤器
//        ByteArrayComparable rowComparator = new BinaryComparator(Bytes.toBytes("person_rk_bj_zhang_000001"));
//        RowFilter rf = new RowFilter(CompareOperator.LESS_OR_EQUAL, rowComparator);
//
//        /*
//         * 假设rowkey格式为：创建日期_发布日期_ID_TITLE
//         * 目标：查找  发布日期  为  2014-12-21  的数据
//         */
//        rf = new RowFilter(CompareOperator.EQUAL, new SubstringComparator("_2014-12-21_"));
//
//
//        //单值过滤器 1 完整匹配字节数组
//        filter = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("tm"), CompareOperator.EQUAL, Bytes.toBytes("20120926090900"));

// 单值过滤器2 匹配正则表达式
//        ByteArrayComparable comparator = new RegexStringComparator(".81800060.");
//        filter = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("stcd"), CompareOperator.EQUAL, comparator);
//
//        //单值过滤器2 匹配是否包含子串,大小写不敏感
//        comparator = new SubstringComparator("wu");
//        new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("NAME"), CompareOperator.EQUAL, comparator);
//
//        //键值对元数据过滤-----family过滤----字节数组完整匹配
//        FamilyFilter ff = new FamilyFilter(
//                CompareOperator.EQUAL,
//                new BinaryComparator(Bytes.toBytes("base_info"))   //表中不存在inf列族，过滤结果为空
//        );
//        //键值对元数据过滤-----family过滤----字节数组前缀匹配
//        ff = new FamilyFilter(
//                CompareOperator.EQUAL,
//                new BinaryPrefixComparator(Bytes.toBytes("inf"))   //表中存在以inf打头的列族info，过滤结果为该列族所有行
//        );
//
//
//        //键值对元数据过滤-----qualifier过滤----字节数组完整匹配
//
//        filter = new QualifierFilter(
//                CompareOperator.EQUAL,
//                new BinaryComparator(Bytes.toBytes("na"))   //表中不存在na列，过滤结果为空
//        );
//        filter = new QualifierFilter(
//                CompareOperator.EQUAL,
//                new BinaryPrefixComparator(Bytes.toBytes("na"))   //表中存在以na打头的列name，过滤结果为所有行的该列数据
//        );
//
//        //基于列名(即Qualifier)前缀过滤数据的ColumnPrefixFilter
//        filter = new ColumnPrefixFilter("na".getBytes());
//
//        //基于列名(即Qualifier)多个前缀过滤数据的MultipleColumnPrefixFilter
//        byte[][] prefixes = new byte[][]{Bytes.toBytes("na"), Bytes.toBytes("me")};
//        filter = new MultipleColumnPrefixFilter(prefixes);
//
//        //为查询设置过滤条件
//        scan.setFilter(filter);
//
//
//        scan.addFamily(Bytes.toBytes("base_info"));
//        //一行
////		Result result = table.get(get);
        //多行的数据
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            for (Cell cell : r.listCells()) {
                System.out.println(new String(CellUtil.cloneQualifier(cell)) + ":" + new String(CellUtil.cloneValue(cell)));
            }
            System.out.println();
        }
        table.close();
    }


    @Test
    public void testDel() throws Exception {
        Connection connection = HbaseUtil.getConnection();
        TableName tableName = TableName.valueOf("WR_INFO");
        Table table = connection.getTable(tableName);
        Delete del = new Delete(Bytes.toBytes("wr_pk_81800120_20081117071100"));
        del.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("ts"));
        table.delete(del);
        table.close();
    }


    public static void main(String[] args) throws Exception {
        Admin admin = HbaseUtil.getAdmin();

        TableName tableName = TableName.valueOf("person_info");

        TableDescriptorBuilder tBuilder = TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptorBuilder cBuder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("base_info"));
        cBuder.setMaxVersions(5);
        tBuilder.setColumnFamily(cBuder.build());


        admin.createTable(tBuilder.build());

        admin.close();

    }


}
