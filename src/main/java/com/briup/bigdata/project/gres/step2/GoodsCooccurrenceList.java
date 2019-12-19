package com.briup.bigdata.project.gres.step2;

import com.briup.bigdata.project.gres.step1.UserBuyGoodsList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 分析第一步的数据，计算商品的共现关系
 */
public class GoodsCooccurrenceList extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsCooccurrenceList(),args));
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path("/gres/step1/ubgl/*");
        Path out = new Path("/gres/step2/gcl");
        Job job =  Job.getInstance(conf,"计算商品的共现问题");
        job.setJarByClass(this.getClass());

        job.setMapperClass(GoodsCooccurrenceListMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job,in);

        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Mapper
     */
    public static class GoodsCooccurrenceListMapper extends Mapper<Text,Text,Text,Text>{
        private Text k = new Text();
        private Text v = new Text();
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("[,]");
            for (String good1 : strs) {
                for (String good2 : strs) {
                    this.k.set(good1);
                    this.v.set(good2);
                    context.write(this.k,this.v);
                }
            }
        }
    }


}
