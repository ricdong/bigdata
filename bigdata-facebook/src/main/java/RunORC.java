import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by gautam on 5/22/14.
 */
public class RunORC extends Configured implements Tool {


    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new RunORC(), args);
        System.exit(res);

    }

    public int run(String[] arg) throws Exception {
        arg = new String[] {"resource/input", "resource/output", "1"};

        Configuration conf=getConf();

        //Set ORC configuration parameters
        conf.set("orc.create.index", "true");
        conf.set("orc.compress", "SNAPPY"); // NONE, ZLIB, SNAPPY
        conf.set("orc.compress.size", "262144"); // Number of bytes in each compression chunk


        Job job = Job.getInstance(conf);
        job.setJarByClass(RunORC.class);
        job.setJobName("ORC Output");
        job.setMapOutputKeyClass(NullWritable.class);
        // job.setMapOutputValueClass(OrcNewOutputFormat.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(ORCMapper.class);

        job.setNumReduceTasks(Integer.parseInt(arg[2]));

        job.setOutputFormatClass(OrcNewOutputFormat.class);

        job.setReducerClass(ORCReducer.class);


        FileInputFormat.addInputPath(job, new Path(arg[0]));
        Path output=new Path(arg[1]);

        OrcNewOutputFormat.setCompressOutput(job,true);
        OrcNewOutputFormat.setOutputPath(job,output);

        return job.waitForCompletion(true) ? 0: 1;
    }


}