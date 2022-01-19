import backtype.storm.topology.TopologyBuilder;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;

public class ExclamationTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("elnombrequequieras", new TCPSpout("127.0.0.1", 10000), 1);
        //builder.setBolt("ticktuple", new TickTuple(),1).shuffleGrouping("elnombrequequieras");
        builder.setBolt("filterBolt", new ParameterFilterBolt("Guada"), 1).shuffleGrouping("elnombrequequieras");
        builder.setBolt("LogFiler", new BoltPrintFile(),4).shuffleGrouping("filterBolt","logging");
        builder.setBolt("languageDetectorBolt", new LangDetector(),1).shuffleGrouping("filterBolt");
        builder.setBolt("hashtag", new HashTagMulti(),1).shuffleGrouping("languageDetectorBolt");
        builder.setBolt("print", new PrintTuple(),1).shuffleGrouping("hashtag");
        /*builder.setSpout("word", new TCPSpout("127.0.0.1",10000), 1);
        builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word");
        builder.setBolt("filter", new FilterBolt(), 1).shuffleGrouping("exclaim1");
        builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("filter");*/

        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(1000000);
            //cluster.killTopology("test");
            //cluster.shutdown();
        }}
}
