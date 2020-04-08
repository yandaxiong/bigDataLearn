package storm.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {
    private Map<String, Integer> map = new HashMap<String, Integer>();

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple) {
        // 1 获取传递过来的数据
        String word = tuple.getString(0);
        Integer num = tuple.getInteger(1);

        // 2 累加单词
        if (map.containsKey(word)) {
            Integer count = map.get(word);
            map.put(word, count + num);
        } else {
            map.put(word, num);
        }

        System.err.println(Thread.currentThread().getId() + "  word:" + word + "  num:" + map.get(word));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
