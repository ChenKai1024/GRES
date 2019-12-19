package com.briup.bigdata.project.gres.step5;

import com.briup.bigdata.project.gres.step4.UserBuyGoodsVector;
import org.apache.commons.collections.list.TreeList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class MultiplyGoodsMatrixAndUserVector extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MultiplyGoodsMatrixAndUserVector(),args));
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in1 = new Path("/gres/step3/gcm/*");
        Path in2 = new Path("/gres/step4/ubgv/*");
        Path out = new Path("/gres/step5/mgmauv");
        Job job =  Job.getInstance(conf,"计算临时推荐结果");
        job.setJarByClass(this.getClass());

        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job,in1);
        SequenceFileInputFormat.addInputPath(job,in2);

        job.setReducerClass(MultiplyGoodsMatrixAndUserVectorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    /**
     * Reducer
     */
    public static class MultiplyGoodsMatrixAndUserVectorReducer extends Reducer<Text,Text,Text,IntWritable>{
        private Text k = new Text(); //
        private IntWritable v = new IntWritable(); //
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TreeSet<String> set = new TreeSet<>();
            values.forEach(value -> set.add(value.toString()));
            List<String> list = new ArrayList<>(set);
            String matrix = list.get(1);
            String vector = list.get(0);
            String[] vectorArr = vector.split("[,]");
            String[] matrixArr = matrix.split("[,]");
            for (String vectorElem : vectorArr) {
                String[] vData = vectorElem.split("[:]");
                for (String matrixElem : matrixArr) {
                    String[] mData = matrixElem.split("[:]");
                    String vm = vData[0] + "," + mData[0];
                    int result = Integer.parseInt(vData[1]) * Integer.parseInt(mData[1]);
                    this.k.set(vm);
                    this.v.set(result);
                    context.write(this.k,this.v);
                }
            }
        }
    }
}
