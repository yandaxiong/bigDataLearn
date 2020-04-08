package storm.pv;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class PVBolt1 implements IRichBolt {

    private OutputCollector collector;
    private long pv = 0;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        // 获取传递过来的数据
        String logline = tuple.getString(0);
        // 截取出sessionid
        String session_id = logline.split("\t")[1];
        // 根据会话id不同统计pv次数
        if(session_id!=null){
            pv++;
        }
        collector.emit(new Values(Thread.currentThread().getId(),pv));
        System.err.println("threadid:" + Thread.currentThread().getId() + "  pv:" + pv);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("threadId","pv"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
