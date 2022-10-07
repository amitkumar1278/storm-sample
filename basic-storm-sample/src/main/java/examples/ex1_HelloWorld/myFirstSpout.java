package examples.ex1_HelloWorld;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


import java.util.Map;

/**
 * to setup a BaseRichSpout you need to implement 5 different methods.
 */
public class myFirstSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private Integer i = 0;

    /**
     * Initialize the spout.
     * when the storm cluster is initializing or setting up the topology, it will initialize each of the individual components of the topology.
     * when this particular spouts is initialized then the open method will be called.
     * you can use this method to initialize any member variables of the class as you would in a constructor.
     * you could also use this method to initiate a connection to a datasource i.e. pushing out data, in case you are connecting your spout to an external data source.
     * when the open method is called the storm cluster passes some information to the spout.
     *
     * PARAMETER: any parameter that has been specified or configured at topology level, are passed on this method.
     *  this is done using configuration map(Map conf) and the context object(TopologyContext context) of the topology.
     *  along with that another component is passed on the spout this component is setup and managed by the storm cluster for the spout and for every bolt.
     *  for any storm component the collector, basically collects the event they are coming into the component, and it will then passed to the next component.
     *  when the collector is passed on using the open method you can assign it to the member variable of our spout and then use it to pass on data in the other methods.
     *
     * @param conf
     * @param context
     * @param collector
     */
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {

        // initialization for the spout; assigning collector to local variable.
        this.collector = collector;
    }

    /**
     * All the action happen in this method.
     * This method is called continuously by the cluster to process events.
     * so everytime this method is called it will look into a queue where events will have been collected and then do any required processing before passing it on to the next component.
     * in storm the events that have been processing or the event which make up a stream of data are called tuples.
     * the nextTuple() method will look for the next set of tuples that have arrived while it was last being run and the process them.
     * the tuple is a primary data representation in storm, every data point is represented using a tuple.
     * Internally tuple is somewhat like a map or dictionary, it has fields/key and values.
     * Every datapoint which passes through a storm component is expected to have a schema. i.e. a set of field that represent different values in that datapoint.
     * the schema cannot change from tuple to tuple, each component is expact to see same schema(fields) everytime.
     * so for instance if you send in a datapoint with two fields name and gender one time, and a datapoint with three fields Name, gender and address the second time then the spout will not accept. it expects to see same set of fields everytime.
     * so every tuple which enters the storm component has the same schema.
     * once the tuple has been processed it might have different schema.
     */
    public void nextTuple() {
        // here we are auto generating an integer and passing them on to the next component i.e. bolt.
        // we do this by setting up integer i, which is a member variable of the spout. and whenever the nextTuple() method is called, this integer will be incremented.
        //      since the nextTuple() method is continuously called, the integer will be continuously incremented.
        //      each time nextTuple() method is called, we need to emit the data to the next component.
        //      this is done by calling to emit() method of the collector. this emit() method will take the value needs to be emitted and passed it on the next component which will be a bolt.
        // As we know data is exchange between component in the form of tuples consist of fields and value.
        // The fields represent schema of the datapoint that is being emitted.
        // any spout or bolt will have a fixed set of fields that need to be declared later on, so when you emit each individual data point you just needs to emit your value.
        // the Values() constructor just takes list of objects which become the value of the tuple.
        // here we have just one object which is an integer that needs to be passed on to the next component.
        // you might also have situation where you are passing on multiple fields such as name, age, gender and so on, in such situation just passed a comma separated list of objects to the values constructor and the emit using emit() method.

        this.collector.emit(new Values(this.i));
        // once you emit the current integer value, you can just increment that values, so the next time nextTuple method is called it can emit new value of i;
        this.i=this.i+1;
    }

    /**
     * The schema of the output tuple needs to be declared so that any components which are connected to the spout know what to expect.
     * so the schema of the tuple is declared in this method.
     * In this method you will declare the schema or the set of fields which this tuple i.e. outputed by the spout will have.
     *
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // specify the schema of the tuple.
        // here we just need to specify the list of names of the fields using a comma separated list of strings, in the fields constructor then pass on new field object to the declare method.
        declarer.declare(new Fields("field"));
    }

    /**
     * this method is called whenever the tuple successfully passes through topology
     * these are implemented when you want to control and improve the reliability of the spout and how it manages failures.
     * once a spout sents out an event into the topology, it does not forget about it. it remembers that events untill it's receives an acknowledgement that event has been successfully processed or it knows whether it has fails.
     * as the user you can define the action the spout should take.
     * in the case of success or failure, in the ack or fail method.
     *
     *
     * @param msgId
     */
    public void ack(Object msgId) {}


    /**
     * this method is called whenever the tuple fail through topology
     * these are implemented when you want to control and improve the reliability of the spout and how it manages failures.
     * once a spout sents out an event into the topology, it does not forget about it. it remembers that events untill it's receives an acknowledgement that event has been successfully processed or it knows whether it has fails.
     * as the user you can define the action the spout should take.
     * in the case of success or failure, in the ack or fail method.
     *
     * @param msgId
     */
    public void fail(Object msgId) {}

}
