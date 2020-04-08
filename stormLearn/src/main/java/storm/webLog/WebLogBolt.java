package storm.webLog;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class WebLogBolt implements IRichBolt {
    private OutputCollector outputCollector;
    private int num = 0;
    private String  valueValue = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
        try {
//            valueValue = tuple.getString(0);
            // 1 获取传递过来的数据
            valueValue = tuple.getStringByField("log");
// 2 如果输入的数据不为空，行数++
            if(valueValue !=null){
                num++;
                System.out.println(Thread.currentThread().getId()+"line:"+num +"  session_id"+valueValue.split("\t")[1]);
            }
            // 3 应答Spout接收成功
            outputCollector.ack(tuple);

        }catch (Exception e){
            // 4 应答Spout接收失败
            outputCollector.fail(tuple);
            e.printStackTrace();
        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 声明输出字段类型
        outputFieldsDeclarer.declare(new Fields(""));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
