package examples.ex1_HelloWorld;


import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 1. We've set up myFirstSpout of our Hello World Topology, Now let's move on and see what's involved in setting up the Bolt.
 *      The bolt will be basically a class called myFirstBolt that extends the BaseBasicBolt abstract class, and to implement this class, we need to implement three methods.
 *          i. execute
 *          ii. declareOutputFields
 *          iii. cleanup
 *
 * 5. once you implement these three methods, the myFirstBolt class will be setup.
 *      Within this class will implement the logic that will take any integers which are coming into the bolt and multiply them by two before sending them on.
 *      This class needs to extend the BaseBasicBolt class, so that's what we have done here, and we need to implement the three methods execute, declareOutputFields and cleanup to set up this class.
 *      Let's start off with the execute method, where the actual logic of taking the input and multiplying by two happens.
 */
public class myFirstBolt extends BaseBasicBolt {

    /**
     * 2. execute() method is the method where all the action happens, Here you will define the transformation logic that needs to happen inside this bolt.
     * whenever there is a tuple that has been passed on from a previous element and that tuple needs to be processed by this bolt, Then the execute method is called.
     * Within the execute method, You will put in all the logic, to process, the input tuple, and then you will use the collector to pass on the output to the next component, just the way we did in the spout.
     *
     * 6. within this method, We just have one line and this line is doing a bunch of things, So let's go through that in detail.
     *
     * @param input
     * @param collector
     */
    public void execute(Tuple input,BasicOutputCollector collector) {

        /**
         *  7. First, we are taking our input tuple, and from that input tuple we are getting the first value that is present in the tuple and casting it to integer.
         *          a tuple is actually like an ordered map So the values in the tuple will be in the same order as you sent them from the previous component.
         *          In the previous component, we sent just one value, which was an integer to the bolt. So we just need to get the value at the first index, which is zero, and we need to cast it to an integer since it will be sent as just an object.
         *          You can do this by using the get value of method and explicitly casting that object to an integer or use the built in method for the tuple call getInteger, which will make the value at the zero index and cast it to integer automatically.
         *          We take that value and multiplied by two, Then we just create a new values object and use the Emit method to pass on that object to the next component. So this will send a new tuple on to the next component, which contains the input integer multiplied by two that completes our execute method.
         */

        collector.emit(new Values(input.getInteger(0)*2));
    }

    /**
     *  3. Every storm component, whether it's a spout or a bolt, has a fixed schema for the output tuple and that schema or the list of fields that would be emitted as part of the tuple needs to be declared for that component. We will declare that in the declared output fields method.
     *
     *  8. Now we can go ahead and declare the schema of the output tuples in the declared output fields This is pretty straightforward.
     *
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        // We do this exactly as we did last time (in spout).
        declarer.declare(new Fields("field"));
    }

    /**
     * 4. This method is called whenever the cluster is shut down to do cleanup actions, such as closing a database connection, closing a file where you are writing the data and so on. For now will be leaving this method empty because we don't want any specific actions in our hello world topology When the bolt is shut down.
     */
    public void cleanup() {
        // we have the clean up method, which we leave empty for now because we don't really have any actions that need to happen when the cluster shuts down.
    }
}