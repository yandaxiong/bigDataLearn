package com.z.transformer.mr.statistics;

import com.z.transformer.common.KpiType;
import com.z.transformer.dimension.key.stats.StatsUserDimension;
import com.z.transformer.dimension.value.MapWritableValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class NewInstallUserReducer extends Reducer<StatsUserDimension, Text,StatsUserDimension, MapWritableValue> {

    private Set<String> distinctUUIDSet = new HashSet<>();
    private  MapWritableValue outputValue =   new MapWritableValue();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //1、UUID去重
        for (Text uuid:values){
            this.distinctUUIDSet.add(uuid.toString());
        }
        //2、组装输出value
        MapWritable map = new MapWritable();
        map.put(new IntWritable(-1), new IntWritable(this.distinctUUIDSet.size()));
        this.outputValue.setValue(map);

        //3、设置outputValue对应的描述的业务指标
        if(KpiType.BROWSER_NEW_INSTALL_USER.name.equals(key.getStatsCommon().getKpi().getKpiName())){
            this.outputValue.setKpi(KpiType.BROWSER_NEW_INSTALL_USER);
        }else  if(KpiType.NEW_INSTALL_USER.name.equals(key.getStatsCommon().getKpi().getKpiName())){
            this.outputValue.setKpi(KpiType.NEW_INSTALL_USER);
        }
        context.write(key,outputValue);
    }
}
