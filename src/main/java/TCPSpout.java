import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.Map;
import java.util.Vector;

public class TCPSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private String host;
    private int port;

    private BufferedReader in;

    public TCPSpout(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.collector = spoutOutputCollector;
            try {
                Socket s = new Socket(host, port);
                in = new BufferedReader(
                        new InputStreamReader(s.getInputStream()));
            } catch (IOException e) {
                e.printStackTrace();
            }

    }

    @Override
    public void nextTuple() {
        try {
            String raw = in.readLine();
            collector.emit(new Values(raw));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Text"));

    }
}
