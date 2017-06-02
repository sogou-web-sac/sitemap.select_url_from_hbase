/**
 * Created by zhufangze on 2017/5/27.
 */
import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class SelectUrlFromHbaseMapper
        extends TableMapper<Text, Text> {

    Text k = new Text();
    Text v = new Text();

    long CUR_TS            = System.currentTimeMillis() / 1000;
    long TS_IN_ONE_DAY    = 86400;
    int MAX_URL_LEN        = 1024;
    String SEPARATOR       = "#_#!";

    public void map(ImmutableBytesWritable row, Result value, Context context)
            throws IOException, InterruptedException {

        context.getCounter("user_mapper", "TOTAL").increment(1L);

        try {
            String url          = new String( value.getValue(Bytes.toBytes("i"), Bytes.toBytes("url")) );
            String root_sitemap = new String( value.getValue(Bytes.toBytes("i"), Bytes.toBytes("top_smap")) );
            String sitemap_type  = new String(value.getValue(Bytes.toBytes("i"), Bytes.toBytes("smap_type")) );
            Long select_ts       = Long.parseLong(new String(value.getValue(Bytes.toBytes("i"), Bytes.toBytes("select_ts"))) );
            Long fetch_ts        = Long.parseLong(new String(value.getValue(Bytes.toBytes("i"), Bytes.toBytes("fetch_ts"))) );

            String DEMAND_TYPE      = context.getConfiguration().get("user.param.sitemap.type", "unknown");
            int MAX_SELECT_INTERVAL = context.getConfiguration().getInt("user.param.max_select_interval", 2);
            int MAX_FETCH_INTERVAL  = context.getConfiguration().getInt("user.param.max_fetch_interval", 14);

            if (!sitemap_type.equals(DEMAND_TYPE)) {
                context.getCounter("user_mapper", "WRONG_SITEMAP_TYPE").increment(1L);
                return;
            }

            if (CUR_TS - select_ts <= MAX_SELECT_INTERVAL*TS_IN_ONE_DAY) {
                context.getCounter("user_mapper", "NOT_READY").increment(1L);
                return;
            }

            if (CUR_TS - fetch_ts <= MAX_FETCH_INTERVAL*TS_IN_ONE_DAY) {
                context.getCounter("user_mapper", "NOT_READY").increment(1L);
                return;
            }

            if (url.length() >= MAX_URL_LEN || root_sitemap.length() >= MAX_URL_LEN) {
                context.getCounter("user_mapper", "URL_TOO_LONG").increment(1L);
                return;
            }

            k.set(root_sitemap);
            v.set(url + SEPARATOR + root_sitemap);
            context.write(k, v);
            context.getCounter("user_mapper", "OUTPUT").increment(1L);
        } catch(Exception e) {
            context.getCounter("user_mapper", "EXCEPTION").increment(1L);
        }
    }

}
