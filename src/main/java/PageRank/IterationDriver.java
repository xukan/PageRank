package PageRank;

/**
 * Created by Onerepublic on 4/25/17.
 */
import Element.Vertex;
import Element.VertexArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class IterationDriver {
    public IterationDriver() {
    }

    public static boolean run(Configuration conf, String inputDir, String outputDir) throws Exception {
        boolean success = false;
        int totaliter = conf.getInt("TotalIteration", -1);

        for(int iterationCount = 0; iterationCount <= totaliter; ++iterationCount) {
            if(iterationCount > 0) {
                inputDir = outputDir;
            }

            StringBuilder sb = new StringBuilder(outputDir);
            if(iterationCount == 0) {
                sb.append(iterationCount);
            } else {
                sb.deleteCharAt(sb.length() - 1);
                sb.append(iterationCount);
            }

            outputDir = sb.toString();
            Job job = Job.getInstance(conf, "iteration");
            job.getConfiguration().setInt("Iteration", iterationCount);
            int iter = job.getConfiguration().getInt("Iteration", -1);
            System.out.println(iter);
            System.out.println(outputDir);
            job.setJarByClass(IterationDriver.class);
            job.setMapperClass(IterationMapper.class);
            job.setReducerClass(IterationReducer.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setMapOutputKeyClass(Vertex.class);
            job.setMapOutputValueClass(VertexArrayWritable.class);
            job.setOutputKeyClass(Vertex.class);
            job.setOutputValueClass(VertexArrayWritable.class);
            Path inputPath = new Path(inputDir);
            Path outputPath = new Path(outputDir);
            success = false;
            FileSystem hdfs = FileSystem.get(conf);
            if(hdfs.exists(outputPath)) {
                hdfs.delete(outputPath, true);
            }

            FileInputFormat.addInputPath(job, inputPath);
            FileInputFormat.setInputDirRecursive(job, true);
            FileOutputFormat.setOutputPath(job, outputPath);
            success = job.waitForCompletion(true);
        }

        return success;
    }
}
