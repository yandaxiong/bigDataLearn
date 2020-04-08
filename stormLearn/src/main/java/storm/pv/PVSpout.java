package storm.pv;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

public class PVSpout implements IRichSpout {

    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector spoutOutputCollector;
    private BufferedReader bufferedReader;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("D:/hadoopTest/website.log")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() {
        try {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    private String str;

    @Override
    public void nextTuple() {
        try {
            while ((str = bufferedReader.readLine()) != null) {
                spoutOutputCollector.emit(new Values(str));
                Thread.sleep(500);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("log"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
