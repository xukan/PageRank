package Prepare;

/**
 * Created by Onerepublic on 4/26/17.
 */
import Driver.Driver.PAGERANK_COUNTER;
import Element.Vertex;
import Element.VertexArrayWritable;
import Reader.Bz2WikiParser;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.XMLReader;

public class PrepMapper extends Mapper<LongWritable, Text, Vertex, VertexArrayWritable> {
    private Bz2WikiParser parser;
    private XMLReader xmlReader;
    private Set<String> linkPages;
    private Set<String> allLeftPages;
    private Set<String> allRightPages;
    private Set<String> leftDanglingNodes;
    private Vertex vpage;
    private BooleanWritable isDanglingNodes;
    private Counter leftCount;

    public PrepMapper() {
    }

    protected void setup(Mapper<LongWritable, Text, Vertex, VertexArrayWritable>.Context context) throws IOException, InterruptedException {
        this.vpage = new Vertex();
        this.parser = new Bz2WikiParser();
        this.linkPages = new HashSet();
        this.xmlReader = this.parser.getXMLReader();
        this.allLeftPages = new HashSet();
        this.allRightPages = new HashSet();
        this.leftDanglingNodes = new HashSet();
        this.leftCount = context.getCounter(PAGERANK_COUNTER.LEFT_NODES);
    }

    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Vertex, VertexArrayWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String page = this.parser.processLine(line, this.xmlReader);
        this.vpage.setPageName(page);
        this.allLeftPages.add(page);
        this.linkPages = this.parser.getLinkPageNames();
        if(this.linkPages.size() == 0) {
            context.write(this.vpage, new VertexArrayWritable());
            this.leftDanglingNodes.add(this.vpage.getPageName().toString());
        } else {
            this.allRightPages.addAll(this.linkPages);
            String[] lpages = (String[])this.linkPages.toArray(new String[this.linkPages.size()]);
            context.write(this.vpage, new VertexArrayWritable(lpages));
        }
    }

    public void cleanup(Mapper<LongWritable, Text, Vertex, VertexArrayWritable>.Context context) throws IOException, InterruptedException {
        HashSet allRightPagesCopy = new HashSet(this.allRightPages);
        this.allRightPages.removeAll(this.allLeftPages);

        for(String rightPage : allRightPages){
            vpage.setPageName(rightPage);
            context.write(vpage, new VertexArrayWritable());
        }

        this.allLeftPages.removeAll(allRightPagesCopy);
        this.leftCount.increment((long)this.allLeftPages.size());
    }
}
