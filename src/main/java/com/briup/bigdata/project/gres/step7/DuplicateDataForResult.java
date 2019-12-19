package com.briup.bigdata.project.gres.step7;

import com.briup.bigdata.project.gres.step5.MultiplyGoodsMatrixAndUserVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 数据去重，在推荐结果中去掉用户已购买的商品信息
 */
public class DuplicateDataForResult extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DuplicateDataForResult(),args));
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in1 = new Path("/gres/rawdata/matrix.txt");
        Path in2 = new Path("/gres/step6/msfm/*");
        Path out = new Path("/gres/step7/ddfr");
        Job job =  Job.getInstance(conf,"数据去重");
        job.setJarByClass(this.getClass());

        MultipleInputs.addInputPath(job,in1,
                TextInputFormat.class,DuplicateDataForResultFirstMapper.class);

        MultipleInputs.addInputPath(job,in2,
                TextInputFormat.class,DuplicateDataForResultSecondMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(DuplicateDataForResultReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 处理用户购买信息的Mapper
     */
    public static class DuplicateDataForResultFirstMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Text k = new Text();
        private Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("[\t]");
            this.k.set(strs[0] + "," + strs[1]);
            this.v.set(strs[2]);
            context.write(this.k,this.v);
        }
    }

    /**
     * 处理推荐信息的Mapper
     */
    public static class DuplicateDataForResultSecondMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Text k = new Text();
        private Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("[\t]");
            this.k.set(strs[0]);
            this.v.set(strs[1]);
            context.write(this.k,this.v);
        }
    }

    /**
     * 去重的Reducer
     */
    public static class DuplicateDataForResultReducer extends Reducer<Text,Text,Text,Text>{
        private Text k = new Text();
        private Text v = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            String buyTimes = null;
            for (Text value : values) {
                count ++;
                buyTimes = value.toString();
            }
            if (count == 1){
                String[] strs = key.toString().split("[,]");
                this.k.set(strs[0]);
                this.v.set(strs[1] + "\t" + buyTimes);
                context.write(this.k,this.v);
            }
        }
    }
}
