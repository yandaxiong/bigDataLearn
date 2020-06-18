package com.z.transformer.mr.etl;


import com.z.transformer.common.EventLogConstants;
import com.z.transformer.common.GlobalConstants;
import com.z.transformer.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

import java.io.File;
import java.io.IOException;

public class AnalysisDataRunner implements Tool {
    private Configuration conf = null;

    public static void main(String[] args) {

    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        //处理事件参数：默认或不合法时间则直接使用昨天日期
        this.processArgs(conf,args);
        //开始创建job
        Job job = Job.getInstance(conf, "Event-ETL");
        //设置job参数
        job.setJarByClass(AnalysisDataRunner.class);
        //mapper参数
        job.setMapperClass(AnalysisDataMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

        //设置reduce参数
        job.setNumReduceTasks(0);

        //配置数据输入
        initJobInputPath(job);
        
        //配置输出到hbase
        initHbaseOutPutConfig(job);

        //Job提交
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 设置输出到HBase的一些操作选项
     * @throws IOException
     */
    private void initHbaseOutPutConfig(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        //获取要ETL的数据是哪一天
        String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
        //格式化HBase后缀名
        String tableNameSuffix = TimeUtil.parseLong2String(TimeUtil.parseString2Long(date), TimeUtil.HBASE_TABLE_NAME_SUFFIX_FORMAT);
        //构建表名
        String tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS+tableNameSuffix;

        //指定输出
        TableMapReduceUtil.initTableReducerJob(tableName,null,job);

        HBaseAdmin admin = null;
        admin = new HBaseAdmin(conf);

        TableName tn = TableName.valueOf(tableName);
        HTableDescriptor htd = new HTableDescriptor(tn);
        //设置列族
        htd.addFamily(new HColumnDescriptor(EventLogConstants.EVENT_LOGS_FAMILY_NAME) );
        //判断表是否存在
        if(admin.tableExists(tn)){
            //存在删除
            if(admin.isTableEnabled(tn)){
                admin.disableTable(tn);
            }
            admin.deleteTable(tn);
        }
        admin.createTable(htd);
        admin.close();
    }

    /**
     * 初始化job数据输入目录
     * @param job
     */
    private void initJobInputPath(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        //获取要执行etl的数据是哪一天的数据
        String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
        //格式化文件路径
        String hdfsPath = TimeUtil.parseLong2String(TimeUtil.parseString2Long(date), "yyyy/MM/dd");
        if(GlobalConstants.HDFS_LOGS_PATH_PREFIX.endsWith("/")){
            hdfsPath = GlobalConstants.HDFS_LOGS_PATH_PREFIX+hdfsPath;
        }else {
            hdfsPath = GlobalConstants.HDFS_LOGS_PATH_PREFIX+ File.separator+hdfsPath;
        }
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(hdfsPath);
        if(fs.exists(path)){
            FileInputFormat.addInputPath(job,path);
        }else {
            throw new RuntimeException("HDFS中该文件目录不存在：" + hdfsPath);
        }

    }
    //处理时间参数，如果没有传递参数的话，默认清洗前一天的。
    /**
     * Job脚本如下： bin/yarn jar ETL.jar com.z.transformer.mr.etl.AnalysisDataRunner -date 20170814
     * @param args
     */
    private void processArgs(Configuration conf, String[] args) {
        String date = null;
        for (int i=0;i<args.length;i++){
            if("-date".equals(args[i])){
                date = args[i+1];
                break;
            }
        }
        if(StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)){
            date = TimeUtil.getYesterday();
        }
        conf.set(GlobalConstants.RUNNING_DATE_PARAMES,date);
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = HBaseConfiguration.create(configuration);
    }
    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
