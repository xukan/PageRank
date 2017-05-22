package Element;

/**
 * Created by Onerepublic on 4/22/17.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Vertex implements WritableComparable<Vertex> {
    private Text pageName;
    private DoubleWritable weight;

    public Vertex() {
        this.pageName = new Text();
        this.weight = new DoubleWritable(0.0D);
    }

    public Vertex(String pn) {
        this.pageName = new Text(pn);
        this.weight = new DoubleWritable(0.0D);
    }

    public Vertex(String pn, Double value, Boolean isdn) {
        this.pageName = new Text(pn);
        this.weight = new DoubleWritable(value.doubleValue());
    }

    public Vertex(Vertex v) {
        this.pageName = v.getPageName();
        this.weight = v.getWeight();
    }

    public Text getPageName() {
        return this.pageName;
    }

    public void setPageName(String pageName) {
        this.pageName.set(pageName);
    }

    public DoubleWritable getWeight() {
        return this.weight;
    }

    public void setWeight(DoubleWritable weight) {
        this.weight = weight;
    }

    public int hashCode() {
        return super.hashCode();
    }

    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        } else if(obj != null && this.getClass() == obj.getClass()) {
            Vertex other = (Vertex)obj;
            return this.getPageName().toString().equals(other.getPageName().toString());
        } else {
            return false;
        }
    }

    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public String toString() {
        return this.pageName.toString();
    }

    public int compareTo(Vertex o) {
        return this.pageName.compareTo(o.getPageName());
    }

    public void write(DataOutput out) throws IOException {
        this.pageName.write(out);
        this.weight.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.pageName.readFields(in);
        this.weight.readFields(in);
    }

    public void clear() {
        this.pageName = new Text();
        this.weight = new DoubleWritable(0.0D);
    }
}