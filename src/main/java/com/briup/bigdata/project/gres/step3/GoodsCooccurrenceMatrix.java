package com.briup.bigdata.project.gres.step3;

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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * 计算商品的共现次数(共现矩阵)
 */
public class GoodsCooccurrenceMatrix extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsCooccurrenceMatrix(),args));
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path("/gres/step2/gcl");
        Path out = new Path("/gres/step3/gcm");
        Job job =  Job.getInstance(conf,"计算商品的共现次数");
        job.setJarByClass(this.getClass());

        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job,in);

        job.setReducerClass(GoodsCooccurrenceMatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Reducer
     */
    public static class GoodsCooccurrenceMatrixReducer extends Reducer<Text,Text,Text,Text>{
        private Text k = new Text();
        private Text v = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String,Integer> map = new TreeMap<>();
            for (Text value : values) {
                String s = value.toString();
                map.put(s,map.containsKey(s) ? map.get(s) + 1 : 1);
            }
            StringBuilder sb = new StringBuilder();
            Set<Map.Entry<String, Integer>> entries = map.entrySet();
            for (Map.Entry<String, Integer> entry : entries) {
                sb.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
            }
            sb.setLength(sb.length() - 1);
            this.k.set(key);
            this.v.set(sb.toString());
            context.write(this.k,this.v);
        }
    }
}
