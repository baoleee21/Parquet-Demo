import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.ExampleParquetOutputFormat;

public class ParquetMapReduceJob {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Parquet MapReduce Example");

        job.setJarByClass(ParquetMapReduceJob.class);
        job.setInputFormatClass(ParquetInputFormat.class);
        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);

        job.setOutputFormatClass(ParquetOutputFormat.class);
        ParquetOutputFormat.setWriteSupportClass(job, ExampleParquetOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ParquetMapper.class);
        job.setReducerClass(ParquetReducer.class);

        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Group.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
