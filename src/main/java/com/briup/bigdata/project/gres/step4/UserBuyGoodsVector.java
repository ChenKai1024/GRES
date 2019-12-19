package com.briup.bigdata.project.gres.step4;

import com.briup.bigdata.project.gres.step3.GoodsCooccurrenceMatrix;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 计算用户的购买向量
 */
public class UserBuyGoodsVector extends Configured  implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new UserBuyGoodsVector(),args));
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path("/gres/rawdata/matrix.txt");
        Path out = new Path("/gres/step4/ubgv");
        Job job =  Job.getInstance(conf,"计算用户的购买向量");
        job.setJarByClass(this.getClass());

        job.setMapperClass(UserBuyGoodsVectorMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(UserBuyGoodsVectorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Mapper
     */
    public static class UserBuyGoodsVectorMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Text k = new Text();
        private Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("[\t]");
            this.k.set(strs[1]);
            this.v.set(strs[0] + ":" + strs[2]);
            context.write(this.k,this.v);
        }
    }

    /**
     * Reducer
     */
    public static class UserBuyGoodsVectorReducer extends Reducer<Text,Text,Text,Text>{
        private Text k = new Text();
        private Text v = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            values.forEach(value -> sb.append(value).append(","));
            sb.setLength(sb.length() - 1);
            this.k.set(key);
            this.v.set(sb.toString());
            context.write(this.k,this.v);
        }
    }
}
