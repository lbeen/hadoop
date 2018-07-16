package nutz;

import org.nutz.dao.Dao;
import org.nutz.dao.entity.Record;
import org.nutz.dao.impl.NutDao;
import org.nutz.dao.impl.SimpleDataSource;
import org.nutz.dao.impl.sql.NutSql;
import org.nutz.dao.impl.sql.callback.QueryRecordCallback;
import org.nutz.dao.sql.Sql;

import javax.sql.DataSource;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * @author 李斌
 */
class DBUtils {
    static Dao dao() throws Exception {
        Properties properties = new Properties();
        properties.load(Main.class.getClassLoader().getResourceAsStream("db.properties"));
        DataSource dataSource = SimpleDataSource.createDataSource(properties);
        return new NutDao(dataSource);
    }

    static List<Record> getRecords(Dao dao, String sqlStr, Consumer<Sql> sqlConsumer) {
        NutSql sql = new NutSql(sqlStr);
        sqlConsumer.accept(sql);
        sql.setCallback(new QueryRecordCallback());
        dao.execute(sql);
        @SuppressWarnings("unchecked")
        List<Record> records = (List<Record>) sql.getResult();
        return records;
    }
}
