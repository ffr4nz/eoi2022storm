import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class WordSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this._collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100L);
        String[] words = new String[]{"anacleto", "sandalio", "atanasio", "Torcuato", "Licerio", "Nazario"};
        Random rand = new Random();
        String word = words[rand.nextInt(words.length)];
        this._collector.emit(new Values(new Object[]{word}));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(new String[]{"word"}));
    }
}
