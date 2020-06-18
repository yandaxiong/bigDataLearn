package com.z.transformer.mr.statistics;

import com.z.transformer.common.DateEnum;
import com.z.transformer.common.EventLogConstants;
import com.z.transformer.common.GlobalConstants;
import com.z.transformer.common.KpiType;
import com.z.transformer.dimension.key.base.BrowserDimension;
import com.z.transformer.dimension.key.base.DateDimension;
import com.z.transformer.dimension.key.base.KpiDimension;
import com.z.transformer.dimension.key.base.PlatformDimension;
import com.z.transformer.dimension.key.stats.StatsCommonDimension;
import com.z.transformer.dimension.key.stats.StatsUserDimension;
import com.z.transformer.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * 思路：HBase读取数据 --> HBaseInPutFormat --> Mapper --> Reducer --> DBOutPutFormat --> 直接写入到Mysql中
 */
public class NewInstallUsersMapper  extends TableMapper<StatsUserDimension, Text> {
    private static final Logger logger = Logger.getLogger(NewInstallUsersMapper.class);
    private byte[] family = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME;

    //维度key
    private StatsUserDimension outputKey = new StatsUserDimension();
    //将outputkey中公共维度取出，便于插座
    private StatsCommonDimension statsCommonDimension = outputKey.getStatsCommon();

    //uuid value
    private Text outputValue = new Text();

    //定义时间变量
    private long date, endOfDate;//描述当前天的起始时间和结束时间
    private long firstThisWeekOfDate, endThisWeekOfDate;//传入日期所在周的起始和结束时间
    private long firstThisMonthOfDate, firstDayOfNextMonth;//传入日期所在月的起始和结束时间

    //创建Kpi指标
    private KpiDimension newInstallUsersKpiDimension = new KpiDimension(KpiType.NEW_INSTALL_USER.name);
    private KpiDimension browserNewInstallUsersKpiDimension = new KpiDimension(KpiType.BROWSER_NEW_INSTALL_USER.name);

    //穿件浏览器维度占位符
    private BrowserDimension defaultBrowserDimension = new BrowserDimension("","");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        1、配置对象初始化
        Configuration configuration = context.getConfiguration();
        //2、提取传入时间
        String date = configuration.get(GlobalConstants.RUNNING_DATE_PARAMES);
        //3、生成对应的时间戳
        this.date = TimeUtil.parseString2Long(date);
        this.endOfDate = this.date + GlobalConstants.DAY_OF_MILLISECONDS;
        this.firstThisWeekOfDate = TimeUtil.getFirstDayOfThisWeek(this.date);
        this.endThisWeekOfDate = TimeUtil.getFirstDayOfNextWeek(this.date);
        this.firstThisMonthOfDate = TimeUtil.getFirstDayOfThisMonth(this.date);
        this.firstDayOfNextMonth = TimeUtil.getFirstDayOfNextMonth(this.date);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //1、读取HBase中的数据 serverTime、platformName、platformVersion、browserName、browserVersion、uuid
        String serverTime = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
        String platformName = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));
        String platformVersion = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_VERSION)));
        String browserName = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
        String uuid = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID)));

        //2、简单过滤
        if(StringUtils.isBlank(platformName) || StringUtils.isBlank(uuid) ){
            logger.debug("服务器数据异常：" + platformName + "," + uuid);
            return;
        }

        //3、数据过滤以及时间字符串转换
        long longOfServerTime = 0L;
        longOfServerTime= Long.valueOf(serverTime);

        //设置输出value--uuid
        this.outputValue.set(uuid);

        //4.设置输出的key-维度
        //4.1构建时间维度信息
        DateDimension dayOfDimenssion = DateDimension.buildDate(longOfServerTime, DateEnum.DAY);
        DateDimension weekOfDimenssion = DateDimension.buildDate(longOfServerTime, DateEnum.WEEK);
        DateDimension monthOfDimenssion = DateDimension.buildDate(longOfServerTime, DateEnum.MONTH);

        //4.2、构建platform维度信息
        List<PlatformDimension> platforms = PlatformDimension.buildList(platformName,platformVersion);

        //4.3、构建browser维度信息
        List<BrowserDimension>  browsers = BrowserDimension.buildList(browserName,browserVersion);

        for (PlatformDimension pf:platforms){
            //设置浏览器维度
            this.outputKey.setBrowser(defaultBrowserDimension);
            //设置platfor维度
            this.statsCommonDimension.setPlatform(pf);
            //设置kpi维度
            this.statsCommonDimension.setKpi(this.newInstallUsersKpiDimension);

            //开始设置时间维度
            /*
             * longOfServerTime：当前消息产生的服务器时间
             * this.date:传入参数的时间
             */
            if(longOfServerTime >= this.date && longOfServerTime <this.endOfDate){
                //设置时间维度为分析当日
                this.statsCommonDimension.setDate(dayOfDimenssion);
                context.write(this.outputKey, this.outputValue);
            }

            if(longOfServerTime >= this.firstThisWeekOfDate && longOfServerTime < this.endThisWeekOfDate){
                this.statsCommonDimension.setDate(weekOfDimenssion);
                context.write(this.outputKey, this.outputValue);
            }
            if(longOfServerTime >= firstThisMonthOfDate && longOfServerTime < firstDayOfNextMonth){
                this.statsCommonDimension.setDate(monthOfDimenssion);
                context.write(this.outputKey, this.outputValue);
            }
            this.statsCommonDimension.setKpi(this.browserNewInstallUsersKpiDimension);

            for(BrowserDimension bd : browsers){
                this.outputKey.setBrowser(bd);
                //时间维度
                if(longOfServerTime >= this.date && longOfServerTime < this.endOfDate){
                    //设置时间维度为分析当日
                    this.statsCommonDimension.setDate(dayOfDimenssion);
                    context.write(this.outputKey, this.outputValue);
                }

                if(longOfServerTime >= firstThisWeekOfDate && longOfServerTime < endThisWeekOfDate){
                    this.statsCommonDimension.setDate(weekOfDimenssion);
                    context.write(this.outputKey, this.outputValue);
                }

                if(longOfServerTime >= firstThisMonthOfDate && longOfServerTime < firstDayOfNextMonth){
                    this.statsCommonDimension.setDate(monthOfDimenssion);
                    context.write(this.outputKey, this.outputValue);
                }
            }

        }

        super.map(key, value, context);
    }
}
