package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;



public class Pipe {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        // The first step to write a steams application is to create a java.util.Properties map
       //to specify diffferent STreams execution configuration values as defined Streamsconfig
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        //gives the unique identifier of your streams application to distinguish itself with
       //other applications talking to the same Kafka cluster
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //specifies a list of host/port pairs to use for establishing
       //The initial connect to the Kafka cluster
        
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // You can customize other configurations in the same map, for example, default
       //serialization and deserialization libraries for the record key-value pairs

        final StreamsBuilder builder = new StreamsBuilder();
        // Next we will define the computational logic of our Streams application
       // In Kafka Streams this computation logic is defined as a topology of connected
       //processor nodes
       // We use a topology builder to construct such a topology

        builder.stream("streams-plaintext-input").to("streams-pipe-output");
        // Now we get a KStream that is continuously generating records from its source Kafka
        //topic "streams-plaintext-input"
        // The records are organized as String typed key-value pairs
        //The simplest thing we can do with this stream is write it into another Kafka topic,
        //"streams-pipe-output"

        final Topology topology = builder.build();
    //We can inspect what kind of topology is created from this builder by doing the following
       // this will indicate two processor nodes
       //--> and <-- arrows dictates the downstream and upstream processor nodes of this
       //node i.e "children" and "parents" within the topology graph
       // it'll also illustrate that this simple topology has no global stores associated with it


        final KafkaStreams streams = new KafkaStreams(topology, props);
        // now construct the Streams client with two components we have just constructed above
       // these are "props" and "topology"
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}