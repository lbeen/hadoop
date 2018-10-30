package hadoop.hbase.inputmr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nutz.dao.Dao;
import org.nutz.dao.entity.Record;
import util.DBUtils;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * @author 李斌
 */
public class WrWriteFile {
    private static final Log LOG = LogFactory.getLog(WrWriteFile.class);

    public static void main(String[] args) throws Exception {
        Dao dao = DBUtils.dao();
        OutputStream os = new FileOutputStream("/tmp/src.data");
        writeData(dao, os, "2007");
        writeData(dao, os, "2008");
        writeData(dao, os, "2009");
        writeData(dao, os, "2010");
        writeData(dao, os, "2011");
        writeData(dao, os, "2012");
        writeData(dao, os, "2013");
        writeData(dao, os, "2014");
        writeData(dao, os, "2015");
        writeData(dao, os, "2016");
        os.close();
    }


    private static void writeData(Dao dao, OutputStream os, String year) throws Exception {
        LOG.debug("year=" + year);
        writeData(dao, os, year + "0101", year + "0630");
        writeData(dao, os, year + "0701", year + "1231");
    }

    private static void writeData(Dao dao, OutputStream os, String startTime, String endTime) throws Exception {
        LOG.debug("startTime=" + startTime + ",endTime=" + endTime);
        String sqlStr = "select stcd,to_char(tm,'yyyymmddhh24mmss') tm, z, spe_reg_data,to_char(ts,'yyyymmddhh24mmss') ts from wr_st_z_r where tm>to_date(@startTime,'yyyymmdd') and tm<=to_date(@endTime,'yyyymmdd')";
        List<Record> records = DBUtils.getRecords(dao, sqlStr, sql -> {
            sql.params().set("startTime", startTime);
            sql.params().set("endTime", endTime);
        });
        int i = 0;
        StringBuilder sb = new StringBuilder();
        for (Record record : records) {
            for (Map.Entry<String, Object> entry : record.entrySet()) {
                Object value = entry.getValue();
                if (value != null) {
                    sb.append(value).append(",");
                }
            }
            sb.append("\n");
            if (++i > 100000) {
                os.write(sb.toString().getBytes());
                os.flush();
                sb.setLength(0);
                i = 0;
            }
        }
        if (i > 0) {
            os.write(sb.toString().getBytes());
            os.flush();
        }
    }
}
