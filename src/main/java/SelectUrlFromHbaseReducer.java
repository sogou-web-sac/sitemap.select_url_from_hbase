/**
 * Created by zhufangze on 2017/5/27.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class Pair {
    public String first;
    public String second;
    public Pair(String first, String second) {
        this.first = first;
        this.second = second;
    }
}

public class SelectUrlFromHbaseReducer
        extends Reducer<Text, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    String SEPARATOR = "#_#!";

    private List<Pair> sample_from_pool(Iterable<Text> values, Context context) {
        int N_SAMPLE = context.getConfiguration().getInt("user.param.n_sample", 1000);

        List<Pair> arr = new ArrayList<Pair>();

        Long seed = System.currentTimeMillis();
        Random rand = new Random();
        rand.setSeed(seed);

        int i = 1;
        for (Text value : values) {
            String[] parts = value.toString().split(SEPARATOR);
            if (parts.length != 2) {
                context.getCounter("user_reducer", "INVAILD").increment(1L);
                continue;
            }
            String url = parts[0].trim();
            String root_sitemap = parts[1].trim();
            if (i <= N_SAMPLE) {
                arr.add( new Pair(url, root_sitemap) );
            } else {
                int p = rand.nextInt(i);
                if (p < N_SAMPLE) {
                    int j = rand.nextInt(N_SAMPLE);
                    arr.set(j, new Pair(url, root_sitemap) );
                }
            }
            ++ i;
        }
        return arr;
    }

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        context.getCounter("user_reducer", "TOTAL").increment(1L);

        List<Pair> arr = sample_from_pool(values, context);

        for (Pair p : arr) {
            k.set(p.second);
            v.set(p.first);
            context.write(k, v);
            context.getCounter("user_reducer", "OUTPUT").increment(1L);
        }
    }
}

