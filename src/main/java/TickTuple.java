import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

public class TickTuple extends BaseRichBolt {

    OutputCollector _collector;
    String buffer = "";
    Integer count = 0;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if (isTickTuple(tuple)) {
                System.out.println("Tick Tuple!! Tuplas en los últimos 5 segundos");
                System.out.println(count);
                count = 0;
            }else {
                count += 1;
            }
        } catch (Exception e) {
            _collector.reportError(e);
        }
    }

    protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // configure how often a tick tuple will be sent to our bolt
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
        return conf;
    }
}
