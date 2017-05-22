package Prepare;

/**
 * Created by Onerepublic on 4/28/17.
 */
import Driver.Driver.PAGERANK_COUNTER;
import Element.Vertex;
import Element.VertexArrayWritable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class PrepReducer extends Reducer<Vertex, VertexArrayWritable, Vertex, VertexArrayWritable> {
    private Set<String> linkPages;
    private Counter dnCounter;
    private Counter trCounter;
    private Counter nnCounter;

    public void setup(Reducer<Vertex, VertexArrayWritable, Vertex, VertexArrayWritable>.Context context) {
        this.linkPages = new HashSet();
        this.dnCounter = context.getCounter(PAGERANK_COUNTER.DANGLING_NODES);
        this.trCounter = context.getCounter(PAGERANK_COUNTER.TOTAL_NODES);
        this.nnCounter = context.getCounter(PAGERANK_COUNTER.NORMAL_NODES);
    }

    public void reduce(Vertex key, Iterable<VertexArrayWritable> values, Reducer<Vertex, VertexArrayWritable, Vertex, VertexArrayWritable>.Context context) throws IOException, InterruptedException {
        this.linkPages.clear();
        this.trCounter.increment(1);

        for(VertexArrayWritable value: values){

            for(Writable w: value.get()) {
                Vertex linkPage = (Vertex)w;
                this.linkPages.add(linkPage.getPageName().toString());
            }
        }

        if(this.linkPages.size() > 0) {
            this.nnCounter.increment(1);
            String[] linkPageNames = (String[])this.linkPages.toArray(new String[this.linkPages.size()]);
            context.write(key, new VertexArrayWritable(linkPageNames));
        } else {
            this.dnCounter.increment(1);
            context.write(key, new VertexArrayWritable());
        }

    }
}
