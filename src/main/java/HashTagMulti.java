import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class HashTagMulti extends BaseRichBolt {
    OutputCollector _collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector=outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        // List<String> resultado = new ArrayList<String>();
        //resultado.add(3);

        String text = tuple.getString(1);
        String lng = tuple.getString(0);
        String [] palabras;
        palabras = text.split(" ");
        for (int n = 0; n <palabras.length; n++) {
            if (palabras[n].charAt(0) == '#') {
                System.out.println(palabras[n].toString());
                _collector.emit(tuple, new Values(palabras[n].toString(),lng,text));
            }
        }
        _collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag","language","text"));

    }
}
