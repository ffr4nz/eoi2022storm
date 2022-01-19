import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class FilterBolt extends BaseRichBolt {

    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getStringByField("word").equals("mike!!!")){
            System.out.println("Filtrado con exito");
            _collector.emit(tuple, new Values(tuple.getStringByField("word")));

        }else{
            _collector.emit("logging",new Values(tuple.getStringByField("word")));
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
        outputFieldsDeclarer.declareStream("logging",new Fields("word"));
    }
}
