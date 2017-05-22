package TopRecord;

/**
 * Created by Onerepublic on 5/2/17.
 */
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKReducer extends Reducer<DoubleWritable, Text, Text, Text> {
    private long topPages;
    private int count = 0;

    public void setup(Reducer<DoubleWritable, Text, Text, Text>.Context context) {
        this.topPages = context.getConfiguration().getLong("TOP_RECORDS", 0);
        System.out.println("totalRecords in reducer: " + this.topPages);
    }

    public void reduce(DoubleWritable pageRank, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ++count;
        if(count < topPages) {
            for(Text pageName: values) {
                context.write(pageName, new Text(String.valueOf(pageRank.get())));
            }
        }
    }
}

