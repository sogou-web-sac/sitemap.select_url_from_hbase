/**
 * Created by zhufangze on 2017/5/27
 * @param: user.param.sitemap.type
 * @param: user.param.n_sample
 * @param: user.param.max_select_interval (default: 2 days )
 * @param: user.param.max_fetch_interval (default: 14 days )
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SelectUrlFromHbase extends Configured implements Tool {

    // to print config param
    public static void print_conf(Configuration conf) {
        try {
            conf.writeXml(System.out);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int run(String[] arg0) throws Exception {

        Configuration conf = getConf();
        Job job = new Job(conf, conf.get("mapred.job.name"));
        String input_table = conf.get("hbase.table");


        job.setJarByClass(SelectUrlFromHbaseMapper.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("url"));
        scan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("top_smap"));
        scan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("smap_type"));
        scan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("select_ts"));
        scan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("fetch_ts"));

        TableMapReduceUtil.initTableMapperJob(
                input_table,
                scan,
                SelectUrlFromHbaseMapper.class,
                Text.class,
                Text.class,
                job);

        job.setReducerClass(SelectUrlFromHbaseReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(200); // this is need by hand code

        if (job.waitForCompletion(true) && job.isSuccessful()) {
            return 0;
        }
        return -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        print_conf(conf);

        int res = ToolRunner.run(conf, new SelectUrlFromHbase(), args);
        System.exit(res);
    }

}