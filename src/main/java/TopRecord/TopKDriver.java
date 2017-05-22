package TopRecord;

/**
 * Created by Onerepublic on 5/2/17.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopKDriver {
    public static boolean run(Configuration conf, String inputDir, String outputDir) throws Exception {
        boolean success = false;
        int topr = conf.getInt("TOP_RECORDS", 100);
        System.out.println("TopRecoreds: " + topr);
        int totaliter = conf.getInt("TotalIteration", -1);
        inputDir = inputDir + totaliter;
        System.out.println("TopRecords");
        System.out.println(inputDir + " " + outputDir);
        Job job = Job.getInstance(conf, "TOPRECORDS");
        job.setJarByClass(TopKDriver.class);
        job.setMapperClass(TopKMapper.class);
        job.setReducerClass(TopKReducer.class);
        job.setSortComparatorClass(TopKComparator.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
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
        return success;
    }
}
