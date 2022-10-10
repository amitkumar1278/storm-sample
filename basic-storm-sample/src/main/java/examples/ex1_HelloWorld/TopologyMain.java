package examples.ex1_HelloWorld;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * We're done with setting up our spot and bolt. Now let's go ahead and use them to build our topology and then submit that topology to a clustered in the local board.
 * We'll set up a class called TopologyMain, which will be running, so we'll put our code in the main method of this class.
 *
 * Then within this class, the first thing we need to do is to build our topology.
 * Building a topology basically means that you will be adding all the spout and bolts which make up the topology and set up the connections between those components.
 *
 */
public class TopologyMain {
    public static void main(String[] args) throws Exception {

        /**  Build Topology: First, we'll set up our topology builder and use it to specify the components of the topology. later we will call the create topology method of this builder to actually build the topology.         */
        TopologyBuilder builder = new TopologyBuilder();

        /**  Next, we use the setSpout method to add a spout to this builder. to add this spout. We need to specify the name of the spout and pass the spout object that represents that spout.
         *  The spot object is created using the myFirstSpout class that we set up earlier, And we've also set the name of this spout using a string, "My first spout".     */
        builder.setSpout("My-First-Spout", new myFirstSpout());

        /** Just as we added a spout to the topology builder, we need to add a bolt. They do this using the setBolt method.
         *  to the setBolt method, we passed the name of the Bolt and the bolt object itself, which is created using the myFirstBolt class. once the Bolt is set up, we need to connect this spout and the bolt.
         *  in storm exchange of data is called grouping and shuffle grouping is the default methodology for exchanging data between components, so to simply connect the spout and the bolt, We use the shuffle grouping method and we specify which spout the bolt is connected to.
         *  the string "My-First-Spout" refers to that name of this spout Within this topology.         */
        builder.setBolt("My-First-Bolt", new myFirstBolt()).shuffleGrouping("My-First-Spout");

        /** Configuration: Next, we need to set up the configuration for this topology. We will use this config object to set parameters that will be true for the topology overall that will apply to all the components and the topology. */
        Config conf = new Config();
        /** One of the parameters that we have explicitly set here, is the set debug true parameter. By setting this parameter, we'll be able to see that the tuples that are being emitted by the spout and the bolt in the console log when we run this topology. */
        conf.setDebug(true);


        /** Submit Topology to cluster: Now we need to actually submit this topology to a cluster.

         * The local cluster will create a cluster which will run in the local mode, and we can submit our topology that we set up earlier to this cluster, then that topology will run on the cluster until the cluster shuts down.
         * Once this cluster is set up, we need to actually submit our topology to this cluster, to submit our topology will simply use the submitTopology method of the cluster.
         * So this method we need to pass the topology name the topology configuration and the topology object itself, which we set up using the builder.createTopology() method.
         *
         * Once we submit the topology, it will start running, and it will continue running indefinitely. So we'll just let the thread run for a limited amount of time and then shut down the cluster.
         *
         */
        LocalCluster cluster = new LocalCluster();
        try{
            System.out.println("Submitting topology ");
            cluster.submitTopology("My-First-Topology", conf, builder.createTopology());
            System.out.println("Submission done");
            Thread.sleep(1000);
        } finally{
            cluster.shutdown();
        }

        /**
         * Let's go ahead and run this to see the results of the topology running. The log will be quite robust with a lot of detail.
         *
         * So you just need to look for statements that say "Emitting" to see what is being emitted by each component.
         * In log we can see what was emitted by the spout, and you can see that it is a consecutive list of integers.
         * This list of integers is passed on to the bolt and the bolt converts them into even numbers, so each integer gets into the bolt, it is multiplied by two.
         * Therefore, when you look at the bolt's consecutive output, you will see that they are Consecutive even numbers. for instance, as an example, if We have 362, followed by 364, followed by 366, then 368 and so on.
         *
         * here We've successfully created a simple topology that auto generates integer and then converts them to even numbers.
         */
    }
}
