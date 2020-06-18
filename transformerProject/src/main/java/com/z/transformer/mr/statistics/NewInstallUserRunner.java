package com.z.transformer.mr.statistics;

import com.z.transformer.common.EventLogConstants;
import com.z.transformer.common.GlobalConstants;
import com.z.transformer.dimension.key.stats.StatsUserDimension;
import com.z.transformer.dimension.value.MapWritableValue;
import com.z.transformer.mr.TransformerMySQLOutputFormat;
import com.z.transformer.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NewInstallUserRunner implements Tool {

    private Configuration conf = null;
    public static void main(String[] args) {
        try {
            int run = ToolRunner.run(new NewInstallUserRunner(), args);
            if(run==0){
                System.out.println("运行成功");
            }else {
                System.out.println("运行失败");
            }
        }catch (Exception e){
            System.out.println("运行失败");
            e.printStackTrace();
        }
    }


    @Override
    public void setConf(Configuration conf) {
        //添加自己开发环境所有需要的其他资源文件
        conf.addResource("transformer-env.xml");
        conf.addResource("output-collector.xml");
        conf.addResource("query-mapping.xml");

        this.conf = HBaseConfiguration.create(conf);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }


    @Override
    public int run(String[] args) throws Exception {
        //1、得到conf对象
        Configuration conf = this.getConf();

        //作用是将参数解析出来，得到日期参数
        this.processArgs(conf,args);

        //2、创建job
        Job job = Job.getInstance(conf, "new_install_users");
        job.setJarByClass(NewInstallUserRunner.class);
        
        //3、mapper 设置hbase的mapper，和inputformat
        this.setHbaseInputConfig(job);

        //4.设置reducer
        job.setReducerClass(NewInstallUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);

        //5.设置job的输出
        job.setOutputFormatClass(TransformerMySQLOutputFormat.class);

        return  job.waitForCompletion(true) ? 0 : 1;

    }

    /**
     * 初始化HBase Mapper
     */
    private void setHbaseInputConfig(Job job) {
        Configuration conf = job.getConfiguration();

        String dateStr = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);

        List<Scan> scans = new ArrayList<>();
        //1、构建filter
        FilterList filterList = new FilterList();
        
        //2.构建过滤：只需返回hbase表中的launch时间所代表的数据en=e_l
        filterList.addFilter(new SingleColumnValueFilter(
                EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME,
                Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(EventLogConstants.EventEnum.LAUNCH.alias)));
        //3.构建其他过滤规则，一忽儿要添加到filter中
        String[] columns = new String[]{
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM,//平台名称
                EventLogConstants.LOG_COLUMN_NAME_VERSION,//平台本本
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME,//浏览器名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION,//浏览器版本
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,//服务器时间
                EventLogConstants.LOG_COLUMN_NAME_UUID,//uuid客户唯一标识符
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME,//事件名称
        };
        filterList.addFilter(this.getColumnFilter(columns));
        //4.访问hbase数据
        long startDate,endDate; //scan表区间时间范围
        //传入参数的时间毫米数：当天起始时间
        long date = TimeUtil.parseString2Long(dateStr);
        //传入参数当天的最后时刻: 当天的结束时间
        long endOfDate = date = GlobalConstants.DAY_OF_MILLISECONDS;

        long firstDayOfWeek = TimeUtil.getFirstDayOfThisWeek(date);
        long lastDayOfWeek = TimeUtil.getFirstDayOfNextWeek(date);
        long firstDayOfMonth = TimeUtil.getFirstDayOfThisMonth(date);
        long lastDayOfMonth = TimeUtil.getFirstDayOfNextMonth(date);

        //TimeUtil.getTodayInMills返回系统当天时间0点0分0秒毫秒数：执行代码时，系统的当前时间
        startDate = Math.min(firstDayOfWeek,firstDayOfMonth);

        endDate = TimeUtil.getTodayInMillis() + GlobalConstants.DAY_OF_MILLISECONDS;

        if(endDate > lastDayOfWeek ||  endDate > lastDayOfMonth){
            endDate = Math.max(lastDayOfMonth,lastDayOfWeek);
        }else {
            endDate = endOfDate;
        }

        HBaseAdmin admin = null;

        try {
            admin = new HBaseAdmin(conf);
            for (long begin = startDate;begin < endOfDate; begin += GlobalConstants.DAY_OF_MILLISECONDS){
                //表名组成：tablename = event_logs20170816
                //拼接出来的结果是：20170816
                String tableNameSuffix = TimeUtil.parseLong2String(begin,TimeUtil.HBASE_TABLE_NAME_SUFFIX_FORMAT);
                String tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS + tableNameSuffix;

                if(admin.tableExists(Bytes.toBytes(tableName))){
                    Scan scan = new Scan();
                    scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,Bytes.toBytes(tableName));
                    scan.setFilter(filterList);
                    scans.add(scan);
                }
            }
            if(scans.isEmpty()){
                throw new RuntimeException("没有找到任何对应的数据！");
            }
            TableMapReduceUtil.initTableMapperJob(
                    scans,
                    NewInstallUsersMapper.class,
                    StatsUserDimension.class,
                    Text.class,
                    job,
                    true//如果在Windwos上运行，需要改为false，如果打成 jar包提交linux运行，则需要为true，默认为：true
            );
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


    /**
     * Job脚本如下： bin/yarn jar ETL.jar com.z.transformer.mr.etl.AnalysisDataRunner -date 2017-08-14
     */
    private void processArgs(Configuration conf, String[] args) {
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if ("-date".equals(args[i])) {
                date = args[i + 1];
                break;
            }
        }

        if(StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)){
            //默认清洗昨天的数据到HBase
            date = TimeUtil.getYesterday();
        }
        //将要清洗的目标时间字符串保存到conf对象中
        conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
    }

    /**
     * 得到过滤的列的对象
     * @param columns
     * @return
     */
    private Filter getColumnFilter(String[] columns) {
        byte[][] prefixes = new byte[columns.length][];
        for (int i = 0; i < columns.length; i++){
            prefixes[i]= Bytes.toBytes(columns[i]);
        }
        return  new MultipleColumnPrefixFilter(prefixes);
    }

}
