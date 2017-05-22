package Element;

/**
 * Created by Onerepublic on 4/22/17.
 */
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class VertexArrayWritable extends ArrayWritable {
    public VertexArrayWritable() {
        super(Vertex.class);
        this.set(new Vertex[0]);
    }

    public VertexArrayWritable(Vertex[] values) {
        super(Vertex.class, values);
    }

    public VertexArrayWritable(String[] linkPages) {
        super(Text.class);
        Vertex[] vertices = new Vertex[linkPages.length];

        for(int i = 0; i < linkPages.length; ++i) {
            vertices[i] = new Vertex(linkPages[i]);
        }

        this.set(vertices);
    }

    public VertexArrayWritable(Vertex v) {
        super(Vertex.class);
        Vertex[] vertices = new Vertex[]{v};
        this.set(vertices);
    }

    public VertexArrayWritable(VertexArrayWritable va) {
        super(VertexArrayWritable.class);
        Writable[] array = va.get();
        Vertex[] vertices = new Vertex[array.length];
        int k = 0;

        for(Writable w: array) {
            vertices[k++] = (Vertex)w;
        }

        this.set(vertices);
    }
}

