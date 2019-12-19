package com.briup.bigdata.project.gres.step8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 将推荐结果保存到MySQL数据库中
 */
public class SaveRecommendResultToDB extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SaveRecommendResultToDB(),args));
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();

        Path in = new Path("/gres/step7/ddfr/*");

        Job job =Job.getInstance(conf,"将数据存入数据库");
        job.setJarByClass(this.getClass());

        job.setMapperClass(SaveRecommendResultToDBMapper.class);
        job.setMapOutputKeyClass(UGEWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job,in);

        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(UGEWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(DBOutputFormat.class);
        // 数据库配置
        DBConfiguration.configureDB(
                job.getConfiguration(),
                "com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://bt1:3306/bd1905",
                "hadoop",
                "hadoop"
        );

        // 指定数据库表和字段名
        DBOutputFormat.setOutput(job,"gres",
                "id","uid","gid","exp");

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Mapper
     */
    public static class SaveRecommendResultToDBMapper extends Mapper<Text, Text,UGEWritable, NullWritable>{
        private UGEWritable k = new UGEWritable();
        private NullWritable v = NullWritable.get();
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("[\t]");
            this.k.setUid(key);
            this.k.setGid(strs[0]);
            this.k.setExp(Integer.parseInt(strs[1]));
            context.write(this.k,this.v);
        }
    }
}
