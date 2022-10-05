package examples.ex1_HelloWorld;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;


public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {

        //Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("My-First-Spout", new myFirstSpout());
        builder.setBolt("My-First-Bolt", new myFirstBolt()).shuffleGrouping("My-First-Spout");

        //Configuration
        Config conf = new Config();
        conf.setDebug(true);


        //Submit Topology to cluster
        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology("My-First-Topology", conf, builder.createTopology());
            Thread.sleep(1000);}

        finally{
            cluster.shutdown();}
    }
}
