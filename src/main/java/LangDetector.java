import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.tika.language.*;


import java.util.Map;

public class LangDetector extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        LanguageIdentifier identifier = new LanguageIdentifier(tuple.getString(0));
        String language = identifier.getLanguage();
        System.out.println("El idioma es : " + language);
        _collector.emit(new Values(language,tuple.getString(0)));
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("language","text"));
    }
}
