package com.z.transformer.mr.etl;

import com.z.transformer.common.EventLogConstants;
import com.z.transformer.util.LoggerUtil;
import com.z.transformer.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

public class AnalysisDataMapper extends Mapper<Object, Text, NullWritable, Put> {

    private static final Logger logger = Logger.getLogger(AnalysisDataMapper.class);

    private CRC32 crc32 = null;
    private byte[] family = null;
    private long currentDayInMills = -1;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.crc32 = new CRC32();
        this.family = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME;
        this.currentDayInMills = TimeUtil.getTodayInMillis();
    }

    //1、重写map方法
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        //2.将原数据通过longgerUtil解析成map键值对
        Map<String, String> clientInfo = LoggerUtil.handleLogText(value.toString());

        //2.1 如果解析失败，则map集合中无数据
        if (clientInfo.isEmpty()) {
            logger.debug("日志解析失败:" + value.toString());
        }
        //3、根据解析后的数据，生成对应的Event事件类型
        EventLogConstants.EventEnum event = EventLogConstants.EventEnum.valueOfAlias(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME));

        if (event == null) {
            //4.无法处理的事件，直接输出事件类型
            logger.debug("无法匹配对应的事件类型：" + clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME));
        } else {
            //5.处理具体的事件
            handleEventData(clientInfo, event, context, value);
        }
    }

    /**
     * 处理具体的事件
     *
     * @param clientInfo
     * @param event
     * @param context
     * @param value
     */
    private void handleEventData(Map<String, String> clientInfo, EventLogConstants.EventEnum event, Context context, Text value) {
        //6.事件成功通过过滤，则处理时间
        if (filterEventData(clientInfo, event)) {
            //7.输出到hbase
            outPutData(clientInfo,context);
        } else {
            logger.debug("事件格式不正确：" + value.toString());
        }
    }

    //6.事件成功通过过滤，则处理事件
    private boolean filterEventData(Map<String, String> clientInfo, EventLogConstants.EventEnum event) {

        //事件数据全局过滤
        boolean result = StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME))
                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_PLATFORM));
//        public static final String PC_WEBSITE_SDK = "website";
//        public static final String JAVA_SERVER_SDK = "java_server";

        switch (clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)) {
            case EventLogConstants.PlatformNameConstants.JAVA_SERVER_SDK:
                //java server 发来的数据
                //判断会员ID是否存在
                result = result && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID));
                switch (event) {
                    case CHARGEREFUND:
                        //退款时间
                        break;
                    case CHARGESUCCESS:
                        //订单支付成功
                        result = result && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_ID));
                        break;
                    default:
                        logger.debug("无法处理指定事件" + clientInfo);
                        result = false;
                        break;
                }
                break;

            case EventLogConstants.PlatformNameConstants.PC_WEBSITE_SDK:
                //website发来的数据
                switch (event) {
                    case CHARGEREQUEST:
                        //下单
                        result = result
                                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_ID))
                                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_CURRENCY_TYPE))
                                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_PAYMENT_TYPE))
                                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_CURRENCY_AMOUNT));
                        break;
                    case EVENT:
                        result = result
                                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_CATEGORY))
                                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_ACTION));
                        break;
                    case LAUNCH:
                        break;
                    case PAGEVIEW:
                        result = result
                                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL));
                        break;
                    default:
                        logger.debug("无法处理指定事件" + clientInfo);
                        result = false;
                        break;
                }
                break;
            default:
                result = false;
                logger.debug("无法确定的数据来源：" + clientInfo);
                break;

        }
        return result;
    }


//    7、输出到hbase
    private void outPutData(Map<String, String> clientInfo, Context context) {
        String uuid = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID);
        long serverTime = Long.valueOf(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME));
        
        //因为浏览器信息已经解析完成，所有删除元数据
        clientInfo.remove(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT);
        
        //创建rowkey
        byte[] rowkey = generateRowKey(uuid,serverTime,clientInfo);
        
        
    }
    //8、用为向HBase中写入数据依赖Put对象，Put对象的创建依赖RowKey，所以如下方法
    /**
     * crc32
     * 1、uuid
     * 2、clientInfo
     * 3、时间timeBytes + 前两步的内容
     * @return
     */
    private byte[] generateRowKey(String uuid, long serverTime, Map<String, String> clientInfo) {
        //清空crc32集合中的数据内容
        crc32.reset();

        if(StringUtils.isNotBlank(uuid)){
            this.crc32.update(Bytes.toBytes(uuid));
        }
        this.crc32.update(Bytes.toBytes(clientInfo.hashCode()));

        //当前数据访问服务器的时间-当天00:00点的时间戳  8位数字 -- 4字节
        byte[] timeBytes = Bytes.toBytes(serverTime - this.currentDayInMills);
        byte[] uuidAndMapDataBytes = Bytes.toBytes(this.crc32.getValue());

        //综合字节数组
        byte[] buffer = new byte[timeBytes.length + uuidAndMapDataBytes.length];
        System.arraycopy(timeBytes,0,buffer,0,timeBytes.length);
        System.arraycopy(uuidAndMapDataBytes,0,buffer,timeBytes.length,uuidAndMapDataBytes.length);
        return  buffer;
    }
}
