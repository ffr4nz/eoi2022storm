import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import scala.xml.PrettyPrinter;

import java.util.Map;

public class ParameterFilterBolt extends BaseRichBolt {
    private String palabra;

    public ParameterFilterBolt(String palabra_filtrada){

        this.palabra = palabra_filtrada;
    }

    OutputCollector _collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getString(0).contains(this.palabra)){
            System.out.println("Filtrado con exito");
            _collector.emit(tuple, new Values(tuple.getString(0)));
        }else{
            _collector.emit("logging",new Values(tuple.getString(0)));
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("filteredText"));
        outputFieldsDeclarer.declareStream("logging", new Fields("loggingText"));
    }
}
