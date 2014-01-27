package com.altamiracorp.securegraph.accumulo.mr;

import com.altamiracorp.securegraph.accumulo.AccumuloConstants;
import org.apache.accumulo.core.client.mapreduce.AccumuloRowInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class ItemCountMR extends Configured implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(ItemCountMR.class);
    private static final IntWritable INT_WRITABLE_1 = new IntWritable(1);
    private static final Text DATA_COUNT = new Text("Data");
    private static final Text VERTEX_COUNT = new Text("Vertices");
    private static final Text EDGE_COUNT = new Text("Edges");

    public static class CountMapper extends Mapper<Text, PeekingIterator<Map.Entry<Key, Value>>, Text, IntWritable> {
        public void map(Text key, PeekingIterator<Map.Entry<Key, Value>> row, Context context) throws IOException, InterruptedException {
            String prefix = "" + key.toString().charAt(0);
            if (AccumuloConstants.DATA_ROW_KEY_PREFIX.equals(prefix)) {
                context.write(DATA_COUNT, INT_WRITABLE_1);
                return;
            }
            if (AccumuloConstants.VERTEX_ROW_KEY_PREFIX.equals(prefix)) {
                context.write(VERTEX_COUNT, INT_WRITABLE_1);
                return;
            }
            if (AccumuloConstants.EDGE_ROW_KEY_PREFIX.equals(prefix)) {
                context.write(EDGE_COUNT, INT_WRITABLE_1);
                return;
            }
            LOGGER.warn("Unrecognized row key prefix: " + prefix);
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException("Usage : " + ItemCountMR.class.getSimpleName() + " <instance name> <zookeepers> <user> <password> <table> <auths>");
        }
        String instanceName = args[0];
        String zookeepers = args[1];
        String user = args[2];
        String password = args[3];
        String table = args[4];
        String authorizations = args[5];

        String jobName = this.getClass().getSimpleName() + "_" + System.currentTimeMillis();
        String outputDirectory = "/tmp/" + jobName;

        Job job = Job.getInstance(getConf(), jobName);
        job.setJarByClass(this.getClass());

        job.setInputFormatClass(AccumuloRowInputFormat.class);
        String[] authorizationsArray = authorizations.split(",");
        if (authorizationsArray.length == 1 && authorizationsArray[0].length() == 0) {
            authorizationsArray = new String[0];
        }
        AccumuloRowInputFormat.setConnectorInfo(job, user, new PasswordToken(password.getBytes()));
        AccumuloRowInputFormat.setInputTableName(job, table);
        AccumuloRowInputFormat.setScanAuthorizations(job, new Authorizations(authorizationsArray));
        AccumuloRowInputFormat.setZooKeeperInstance(job, instanceName, zookeepers);

        job.setMapperClass(CountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(outputDirectory));

        job.waitForCompletion(true);

        if (job.isSuccessful()) {
            System.out.println("Results can be found in " + outputDirectory);
        }

        return job.isSuccessful() ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(CachedConfiguration.getInstance(), new ItemCountMR(), args);
        System.exit(res);
    }
}

