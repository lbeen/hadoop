package util;

import hadoop.hbase.inputmr.WrWriteFile;
import org.nutz.dao.Dao;
import org.nutz.dao.entity.Record;
import org.nutz.dao.impl.NutDao;
import org.nutz.dao.impl.SimpleDataSource;
import org.nutz.dao.impl.sql.NutSql;
import org.nutz.dao.impl.sql.callback.QueryRecordCallback;
import org.nutz.dao.sql.Sql;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * @author 李斌
 */
public class DBUtils {
    public static Dao dao() throws Exception {
        DataSource dataSource = getDataSource();
        return new NutDao(dataSource);
    }

    public static Connection getConnection() throws Exception {
        DataSource dataSource = getDataSource();
        return dataSource.getConnection();

    }

    private static DataSource getDataSource() throws Exception {
        Properties properties = new Properties();
        properties.load(WrWriteFile.class.getClassLoader().getResourceAsStream("db.properties"));
        return SimpleDataSource.createDataSource(properties);
    }

    public static List<Record> getRecords(Dao dao, String sqlStr, Consumer<Sql> sqlConsumer) {
        NutSql sql = new NutSql(sqlStr);
        sqlConsumer.accept(sql);
        sql.setCallback(new QueryRecordCallback());
        dao.execute(sql);
        @SuppressWarnings("unchecked")
        List<Record> records = (List<Record>) sql.getResult();
        return records;
    }
}
