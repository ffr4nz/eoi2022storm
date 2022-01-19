import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.*;
import java.util.Map;

public class BoltPrintFile extends BaseRichBolt {

    OutputCollector _collector;
    FileWriter fichero;
    PrintWriter pw;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        _collector = collector;
        try {
            fichero = new FileWriter("/tmp/fichero.txt", true);
            pw = new PrintWriter(fichero);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            pw.println(String.valueOf(tuple));
            pw.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}