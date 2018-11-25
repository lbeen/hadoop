package hadoop.hive;

//import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * @author 李斌
 */
public class GetHost {//extends UDF {

    public Text evaluate(final Text url) {
        try {
            String host = new URL(url.toString()).getHost();
            return new Text(host);
        } catch (MalformedURLException e) {
            return null;
        }
    }
}
