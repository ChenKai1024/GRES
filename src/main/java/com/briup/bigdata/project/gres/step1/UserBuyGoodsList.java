package com.briup.bigdata.project.gres.step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 分析源数据，计算用户购买商品的列表
 */
public class UserBuyGoodsList extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new UserBuyGoodsList(),args));
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path("/gres/rawdata/matrix.txt");
        Path out = new Path("/gres/step1/ubgl");
        Job job =  Job.getInstance(conf,"计算用户购买商品的列表");
        job.setJarByClass(this.getClass());

        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job,in);

        job.setReducerClass(UserBuyGoodsListReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Reducer
     */
    public static class UserBuyGoodsListReducer extends Reducer<Text,Text,Text,Text>{
        private Text k = new Text();
        private Text v = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            values.forEach(value -> sb.append(value.toString().split("[\t]")[0]).append(","));
            sb.setLength(sb.length() - 1);
            this.k.set(key);
            this.v.set(sb.toString());
            context.write(this.k,this.v);
        }
    }
}
