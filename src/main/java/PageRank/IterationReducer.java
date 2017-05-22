package PageRank;

/**
 * Created by Onerepublic on 5/22/17.
 */
import Element.Vertex;
import Element.VertexArrayWritable;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class IterationReducer extends Reducer<Vertex, VertexArrayWritable, Vertex, VertexArrayWritable> {
    private long trCounter;
    private int iteration;
    private double sumOfDanglingNodes;
    private double oldSumOfDanglingNodes;
    private double totalPageRank;
    private double dampingFactor;
    private int ln;
    private long lnCounter;

    public void setup(Context context) throws IOException {
        this.ln = 0;
        System.out.println("Iterator Reducer");
        this.lnCounter = Long.valueOf(context.getConfiguration().get("LEFT_NODES")).longValue();
        this.trCounter = Long.valueOf(context.getConfiguration().get("TOTAL_NODES")).longValue();
        this.iteration = context.getConfiguration().getInt("Iteration", -1);
        if(this.iteration == -1) {
            System.out.println("Cannot get iteration number");
        } else {
            totalPageRank = 0.0;
            sumOfDanglingNodes = 0.0D;
            if(iteration == 0) {
                oldSumOfDanglingNodes = 0.0;
            } else {
                oldSumOfDanglingNodes = retrieveData(context, this.iteration);
                System.out.println("DNPR: " + oldSumOfDanglingNodes);
            }

            this.dampingFactor = Double.parseDouble(context.getConfiguration().get("DAMPING_FACTOR"));
        }
    }

    public double retrieveData(Context context, int iter) throws IOException {
        if(iter == 0) {
            return 0.0D;
        } else {
            int lastIter = iter - 1;
            String fileName = "/user/pagerank/cache/SumOfDanglingNodes_" + String.valueOf(lastIter) + ".txt";
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path inputPath = new Path(fileName);
            FSDataInputStream fis = fs.open(inputPath);
            if(!fs.exists(inputPath)) {
                System.out.println("No Record Number");
                return 0.0;
            } else {
                BufferedReader bfr = new BufferedReader(new InputStreamReader(fis));
                String data = "";
                String line = "";

                while((line = bfr.readLine()) != null) {
                    System.out.println(line);
                    String[] parts = line.split("=");
                    if(parts[0].equals("DNPR")) {
                        data = parts[1];
                    }
                }

                return Double.parseDouble(data);
            }
        }
    }

    public void storeData(Context context, int iter, double sumOfDn, double totalPR) throws IOException {
        String fileName = "/user/pagerank/cache/SumOfDanglingNodes_" + String.valueOf(iter) + ".txt";
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path outputPath = new Path(fileName);
        System.out.println(fileName);
        if(fs.exists(outputPath)) {
            System.err.println("output path exists");
            fs.delete(outputPath, true);
        }

        FSDataOutputStream fos = fs.create(outputPath);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos, "UTF-8"));
        bw.write("DNPR=" + sumOfDn);
        bw.newLine();
        bw.write("TOTALPR=" + totalPR);
        bw.close();
        fos.close();
    }

    public void reduce(Vertex vpage, Iterable<VertexArrayWritable> values, Context context) throws IOException, InterruptedException {
        double weight = 0.0D;
        VertexArrayWritable va = null;
        //Iterator pageRank = values.iterator();

        for(VertexArrayWritable val:values){
            if(this.iteration == 0) {
                if(val.get().length == 0) {
                    this.sumOfDanglingNodes += vpage.getWeight().get();
                }

                this.totalPageRank += vpage.getWeight().get();
                context.write(vpage, val);
                return;
            }

            Writable[] vertices = val.get();
            if(vertices.length == 1) {
                Vertex v = (Vertex)vertices[0];
                if(v.getPageName().toString().equals(vpage.getPageName().toString())) {
                    weight += v.getWeight().get();
                } else {
                    va = new VertexArrayWritable(val);
                }
            } else if(vertices.length > 1) {
                va = new VertexArrayWritable(val);
            } else {
                va = new VertexArrayWritable();
            }
        }
        if(va == null)
            System.out.println("Should not appear");

        if(weight == 0.0) {
            ++ln;
        }

        double pagerank = (1.0 - this.dampingFactor) / (double)this.trCounter + this.dampingFactor * (weight + this.oldSumOfDanglingNodes / (double)this.trCounter);
        vpage.setWeight(new DoubleWritable(pagerank));
        this.totalPageRank += pagerank;
        if(va.get().length == 0) {
            this.sumOfDanglingNodes += pagerank;
        }

        context.write(vpage, va);
    }

    public void cleanup(Context context) throws IOException {
        System.out.println(this.sumOfDanglingNodes + " " + this.totalPageRank);
        System.out.println("total_nodes: " + this.trCounter);
        System.out.println(this.ln + " " + this.lnCounter);
        this.storeData(context, this.iteration, this.sumOfDanglingNodes, this.totalPageRank);
    }
}
