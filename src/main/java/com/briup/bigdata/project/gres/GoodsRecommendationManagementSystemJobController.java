package com.briup.bigdata.project.gres;

import com.briup.bigdata.project.gres.step1.UserBuyGoodsList;
import com.briup.bigdata.project.gres.step2.GoodsCooccurrenceList;
import com.briup.bigdata.project.gres.step3.GoodsCooccurrenceMatrix;
import com.briup.bigdata.project.gres.step4.UserBuyGoodsVector;
import com.briup.bigdata.project.gres.step5.MultiplyGoodsMatrixAndUserVector;
import com.briup.bigdata.project.gres.step6.MakeSumForMultiplication;
import com.briup.bigdata.project.gres.step7.DuplicateDataForResult;
import com.briup.bigdata.project.gres.step8.SaveRecommendResultToDB;
import com.briup.bigdata.project.gres.step8.UGEWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.List;

public class GoodsRecommendationManagementSystemJobController extends Configured implements Tool{
//

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsRecommendationManagementSystemJobController(),args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in=new Path("/gres/rawdata");     // 作业1的输入，也是作业4的输入，也是作业7的输入
        Path out1=new Path("/gres/step1");   // 作业1的输出，也是作业2的输入
        Path out2=new Path("/gres/step2");   // 作业2的输出，也是作业3的输入
        Path out3=new Path("/gres/step3");   // 作业3的输出，也是作业5的输入
        Path out4=new Path("/gres/step4");   // 作业4的输出，也是作业5的输入
        Path out5=new Path("/gres/step5");   // 作业5的输出，也是作业6的输入
        Path out6=new Path("/gres/step6");   // 作业6的输出，也是作业7的输入
        Path out7=new Path("/gres/step7");   // 作业7的输出，也是作业8的输入
        // 1.创建1-8的Job对象，并进行各自的作业配置
        // Job1作业配置
        Job job1 = Job.getInstance(conf,"Step-1");
        job1.setJarByClass(this.getClass());
        job1.setMapperClass(Mapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job1,in);
        job1.setReducerClass(UserBuyGoodsList.UserBuyGoodsListReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job1,out1);

        // Job2作业配置
        Job job2 = Job.getInstance(conf,"Step-2");
        job2.setJarByClass(this.getClass());
        job2.setMapperClass(GoodsCooccurrenceList.GoodsCooccurrenceListMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job2,out1);
        job2.setReducerClass(Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job2,out2);

        // Job3作业配置
        Job job3 = Job.getInstance(conf,"Step-3");
        job3.setJarByClass(this.getClass());
        job3.setMapperClass(Mapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job3,out2);
        job3.setReducerClass(GoodsCooccurrenceMatrix.GoodsCooccurrenceMatrixReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job3,out3
        );

        // Job4作业配置
        Job job4 = Job.getInstance(conf,"Step-4");
        job4.setJarByClass(UserBuyGoodsVector.class);
        job4.setMapperClass(UserBuyGoodsVector.UserBuyGoodsVectorMapper.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job4,in);
        job4.setReducerClass(UserBuyGoodsVector.UserBuyGoodsVectorReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job4,out4);

        // Job5作业配置
        Job job5 = Job.getInstance(conf,"Step-5");
        job5.setJarByClass(this.getClass());
        job5.setMapperClass(Mapper.class);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);
        job5.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job5,out3);
        SequenceFileInputFormat.addInputPath(job5,out4);
        job5.setReducerClass(MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);
        job5.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job5,out5);

        // Job6作业配置
        Job job6 = Job.getInstance(conf,"Step-6");
        job6.setJarByClass(this.getClass());
        job6.setMapperClass(Mapper.class);
        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(IntWritable.class);
        job6.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job6,out5);
        job6.setReducerClass(IntSumReducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(IntWritable.class);
        job6.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job6,out6);

        // Job7作业配置
        Job job7 = Job.getInstance(conf,"Step-7");
        job7.setJarByClass(this.getClass());
        MultipleInputs.addInputPath(job7,in,
                TextInputFormat.class, DuplicateDataForResult.DuplicateDataForResultFirstMapper.class);
        MultipleInputs.addInputPath(job7,out6,
                TextInputFormat.class, DuplicateDataForResult.DuplicateDataForResultSecondMapper.class);
        job7.setMapOutputKeyClass(Text.class);
        job7.setMapOutputValueClass(Text.class);
        job7.setReducerClass(DuplicateDataForResult.DuplicateDataForResultReducer.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(Text.class);
        job7.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job7,out7);

        // Job8作业配置
        Job job8 = Job.getInstance(conf,"Step-8");
        job8.setJarByClass(this.getClass());
        job8.setMapperClass(SaveRecommendResultToDB.SaveRecommendResultToDBMapper.class);
        job8.setMapOutputKeyClass(UGEWritable.class);
        job8.setMapOutputValueClass(NullWritable.class);
        job8.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job8,out7);
        job8.setReducerClass(Reducer.class);
        job8.setOutputKeyClass(UGEWritable.class);
        job8.setOutputValueClass(NullWritable.class);
        job8.setOutputFormatClass(DBOutputFormat.class);
        // 数据库配置
        DBConfiguration.configureDB(
                job8.getConfiguration(),
                "com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://bt1:3306/bd1905",
                "hadoop",
                "hadoop"
        );
        // 指定数据库表和字段名
        DBOutputFormat.setOutput(job8,"gres",
                "uid","gid","exp");


        // 2.创建8个ControlledJob，将上一步的Job对象转化成可被控制的作业
        ControlledJob cj1 = new ControlledJob(conf);
        ControlledJob cj2 = new ControlledJob(conf);
        ControlledJob cj3 = new ControlledJob(conf);
        ControlledJob cj4 = new ControlledJob(conf);
        ControlledJob cj5 = new ControlledJob(conf);
        ControlledJob cj6 = new ControlledJob(conf);
        ControlledJob cj7 = new ControlledJob(conf);
        ControlledJob cj8 = new ControlledJob(conf);

        cj1.setJob(job1);
        cj2.setJob(job2);
        cj3.setJob(job3);
        cj4.setJob(job4);
        cj5.setJob(job5);
        cj6.setJob(job6);
        cj7.setJob(job7);
        cj8.setJob(job8);

        // 3.对可被控制的作业添加依赖关系。
        cj2.addDependingJob(cj1);
        cj3.addDependingJob(cj2);
        cj4.addDependingJob(cj3);
        cj5.addDependingJob(cj4);
        cj6.addDependingJob(cj5);
        cj7.addDependingJob(cj6);
        cj8.addDependingJob(cj7);

        // 4.构建JobControl对象，将8个可被控制的作业逐个添加。
        JobControl jc = new JobControl("商品推荐");
        jc.addJob(cj1);jc.addJob(cj2);jc.addJob(cj3);jc.addJob(cj4);
        jc.addJob(cj5);jc.addJob(cj6);jc.addJob(cj7);jc.addJob(cj8);

        // 5.构建线程对象，并启动线程，执行作业。
        new Thread(jc).start();

        // 打印作业状态
        while(!jc.allFinished()){
            List<ControlledJob> jobList=jc.getRunningJobList();
            for(ControlledJob cj: jobList){
                Job job=cj.getJob();
                job.monitorAndPrintJob();
            }
        }
        return 0;
    }
}
