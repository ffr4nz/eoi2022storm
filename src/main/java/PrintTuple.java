import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

public class PrintTuple extends BaseRichBolt {
    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println(tuple);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // No necesito declarar si no hay nada que se emita
    }
}
