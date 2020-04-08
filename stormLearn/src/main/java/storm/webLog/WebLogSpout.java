package storm.webLog;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

public class WebLogSpout implements IRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private BufferedReader br;
    private String str = null;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        try {
            this.br = new BufferedReader( new FileReader(new File("D:\\hadoopTest\\website.log")));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void nextTuple() {
        try {
            while ((str = br.readLine())!=null){
                this.spoutOutputCollector.emit(new Values(str));
                Thread.sleep(500);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 声明输出字段类型
        outputFieldsDeclarer.declare(new Fields("log"));
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }



    public void ack(Object o) {

    }

    public void fail(Object o) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
