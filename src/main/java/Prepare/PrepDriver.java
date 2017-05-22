package Prepare;

/**
 * Created by Onerepublic on 5/22/17.
 */
import Driver.Driver.PAGERANK_COUNTER;
import Element.Vertex;
import Element.VertexArrayWritable;
import Prepare.PrepMapper;
import Prepare.PrepReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class PrepDriver {
    public static boolean run(Configuration conf, String pathin, String pathout) throws Exception {
        Job job = Job.getInstance(conf, "reader");
        job.setJarByClass(PrepDriver.class);
        job.setMapperClass(PrepMapper.class);
        job.setReducerClass(PrepReducer.class);
        job.setMapOutputKeyClass(Vertex.class);
        job.setMapOutputValueClass(VertexArrayWritable.class);
        job.setOutputKeyClass(Vertex.class);
        job.setOutputValueClass(VertexArrayWritable.class);
        /*Sequence files are a basic file based data structure persisting the key/value pairs in a binary format
        and allowing you to interact more easily with basic hadoop
        data types (e.g IntWritable, LongWritable, etc…)*/
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        Path inputPath = new Path(pathin);
        Path outputPath = new Path(pathout);
        System.out.println(pathin + " " + pathout);
        FileSystem hdfs = FileSystem.get(conf);
        if(hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        Boolean success = job.waitForCompletion(true);

        Counters counters = job.getCounters();
        Counter dn = counters.findCounter(PAGERANK_COUNTER.DANGLING_NODES);
        System.out.println(dn.getDisplayName() + ":" + dn.getValue());
        Counter nn = counters.findCounter(PAGERANK_COUNTER.NORMAL_NODES);
        System.out.println(nn.getDisplayName() + ":" + nn.getValue());
        Counter tr = counters.findCounter(PAGERANK_COUNTER.TOTAL_NODES);
        System.out.println(tr.getDisplayName() + ":" + tr.getValue());
        Counter leftNodes = counters.findCounter(PAGERANK_COUNTER.LEFT_NODES);
        System.out.println(leftNodes.getDisplayName() + ":" + leftNodes.getValue());
        conf.setLong("TOTAL_NODES", tr.getValue());
        conf.setLong("NORMAL_NODES", nn.getValue());
        conf.setLong("DANGLING_NODES", dn.getValue());
        conf.setLong("LEFT_NODES", leftNodes.getValue());
        return success;
    }
}
