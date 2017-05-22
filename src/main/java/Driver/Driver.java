package Driver;

/**
 * Created by Onerepublic on 4/22/17.
 */
import PageRank.IterationDriver;
import Prepare.PrepDriver;
import TopRecord.TopKDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class Driver {

    public static enum PAGERANK_COUNTER {
        DANGLING_NODES,
        NORMAL_NODES,
        TOTAL_NODES,
        LEFT_NODES;
    }

    // args
    // /user/pagerank/input/ /user/pagerank/output /user/pagerank/iteroutput /user/pagerank/TopRecord

    public static void main(String[] args) throws Exception {
        boolean isJobDone = false;
        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/Cellar/hadoop/2.7.3/libexec/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/Cellar/hadoop/2.7.3/libexec/etc/hadoop/hdfs-site.xml"));
        conf.set("DAMPING_FACTOR", "0.85");
        conf.setInt("TotalIteration", 10);
        conf.setInt("TOP_RECORDS", 100);
        String prepInput = args[0];
        String prepOutput = args[1];
        isJobDone = PrepDriver.run(conf, prepInput, prepOutput);
        if(!isJobDone) {
            System.exit(1);
        }

        String IterDriverInput = args[1];
        String IterDriverOutput = args[2];
        System.out.println(IterDriverInput + " " + IterDriverOutput);
        isJobDone = IterationDriver.run(conf, IterDriverInput, IterDriverOutput);
        if(!isJobDone) {
            System.exit(1);
        }

        String TopKInput = args[2];
        String TopKOutput = args[3];
        isJobDone = TopKDriver.run(conf, TopKInput, TopKOutput);
        System.exit(isJobDone?0:1);
    }
}