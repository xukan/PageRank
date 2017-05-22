package PageRank;

/**
 * Created by Onerepublic on 4/25/17.
 */
import Element.Vertex;
import Element.VertexArrayWritable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class IterationMapper extends Mapper<Vertex, VertexArrayWritable, Vertex, VertexArrayWritable> {
    private Vertex vpage;
    private DoubleWritable change;
    private long dn;
    private long tr;
    private Configuration conf;
    private int iter;
    private int dcount;
    private int tcount;

    public IterationMapper() {
    }

    public void setup(Context context) {
        this.dcount = 0;
        this.tcount = 0;
        this.iter = context.getConfiguration().getInt("Iteration", -1);
        System.out.println("iter " + this.iter);
        this.tr = context.getConfiguration().getLong("TOTAL_NODES", 0);
        this.dn = context.getConfiguration().getLong("DANGLING_NODES", 0);
        this.vpage = new Vertex();
        this.change = new DoubleWritable();
    }

    protected void map(Vertex vpage, VertexArrayWritable value, Context context) throws IOException, InterruptedException {
        if(this.iter != -1 && this.tr != -1) {
            if(this.iter == 0) {
                DoubleWritable initail = new DoubleWritable(1.0 / (double)tr);
                vpage.setWeight(initail);
                context.write(vpage, value);
            } else {
                if(value.get().length > 0) {
                    this.change.set(vpage.getWeight().get() / (double)value.get().length);
                }

                if(value.get().length == 0) {
                    context.write(vpage, new VertexArrayWritable());
                    ++dcount;
                } else {
                    context.write(vpage, value);
                    for(Writable w: value.get()) {
                        Vertex linkPage = (Vertex)w;
                        linkPage.setWeight(this.change);
                        context.write(linkPage, new VertexArrayWritable(linkPage));
                    }
                }
            }
        }
    }

    public void cleanup(Context context) {
        System.out.println("Iteration " + this.iter + " Total Dangling Nodes " + this.dcount);
    }
}
