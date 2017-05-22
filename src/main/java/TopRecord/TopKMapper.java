package TopRecord;

/**
 * Created by Onerepublic on 5/2/17.
 */
import Element.Vertex;
import Element.VertexArrayWritable;
import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopKMapper extends Mapper<Vertex, VertexArrayWritable, DoubleWritable, Text> {
//    private class VertexComparator implements Comparator<Vertex> {
//        private VertexComparator() {
//        }
//
//        public int compare(Vertex x, Vertex y) {
//            return x.getWeight().get() == y.getWeight().get()?0:(x.getWeight().get() < y.getWeight().get()?-1:1);
//        }
//    }

    //private PriorityQueue<Vertex> pQueue;
    private TreeMap<Double, String> topMap;
    private int topr;
    int outpage;
    int countshen;

    protected void setup(Mapper<Vertex, VertexArrayWritable, DoubleWritable, Text>.Context context) {
        this.outpage = 0;
        this.countshen = 0;
        this.topr = (int)context.getConfiguration().getLong("TOP_RECORDS", 0L);
        System.out.println("TOP_RECORDS : " + this.topr);
        //this.pQueue = new PriorityQueue(this.topr, new TopKMapper.VertexComparator());
        this.topMap = new TreeMap();
    }

    public void map(Vertex vpage, VertexArrayWritable value, Context context) throws IOException, InterruptedException {
        topMap.put(Double.valueOf(vpage.getWeight().get()), vpage.getPageName().toString());
        if(topMap.size() > topr) {
            topMap.remove(topMap.firstKey());
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Double weight : topMap.descendingKeySet()) {
            context.write(new DoubleWritable(weight), new Text(topMap.get(weight)));
        }

    }
}

