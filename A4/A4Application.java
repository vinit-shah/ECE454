import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Arrays;
import java.util.Properties;

class StudentProcessor extends AbstractProcessor<String, String> {
    
    private ProcessorContext context;
    private KeyValueStore<String,String> studentStore;
    private KeyValueStore<String,Long> classroomMaxStore;
    private KeyValueStore<String,Long> classroomCurrentStore;

    public void init(ProcessorContext context) {
        this.context = context;
        studentStore = (KeyValueStore<String, String>) this.context.getStateStore("student-store");
        classroomMaxStore = (KeyValueStore<String, Long>) this.context.getStateStore("classroom-max-store");
        classroomCurrentStore = (KeyValueStore<String, Long>) this.context.getStateStore("classroom-current-store");
    }

    public void process(String studentId, String newRoomId) {
        String currentRoomId = studentStore.get(studentId);
        studentStore.put(studentId, newRoomId);
        if (null != currentRoomId) {
            Long currentRoomMaxOccupancy = classroomMaxStore.get(currentRoomId);
            Long currentRoomCurrentOccupancy = classroomCurrentStore.get(currentRoomId);
            classroomCurrentStore.put(currentRoomId, currentRoomCurrentOccupancy - 1);
            currentRoomCurrentOccupancy = classroomCurrentStore.get(currentRoomId); // update variable
            if (currentRoomMaxOccupancy != null) {
                // the room has a bounded occupancy so we may need to output things, if it was unbounded we  don't need to output anything
                if (currentRoomCurrentOccupancy == currentRoomMaxOccupancy){
                    context().forward(currentRoomId, "OK");
                    context().commit();
                } else if (currentRoomCurrentOccupancy > currentRoomMaxOccupancy) {
                    context().forward(currentRoomId, Long.toString(currentRoomCurrentOccupancy));
                    context().commit();
                }
            }
        }
        Long newRoomMaxOccupancy = classroomMaxStore.get(newRoomId);
        Long newRoomCurrentOccupancy = classroomCurrentStore.get(newRoomId);
        classroomCurrentStore.put(newRoomId, newRoomCurrentOccupancy == null ? 1 : newRoomCurrentOccupancy + 1);
        newRoomCurrentOccupancy = classroomCurrentStore.get(newRoomId); // update variable
        if (newRoomMaxOccupancy != null) {
            // the room has a bounded occupancy
            if (newRoomCurrentOccupancy > newRoomMaxOccupancy) {
                context().forward(newRoomId, Long.toString(newRoomCurrentOccupancy));
                context().commit();
            }
        }
    }
}

class ClassroomProcessor extends AbstractProcessor<String, String> {
 
    private ProcessorContext context;
    private KeyValueStore<String, String> studentStore;
    private KeyValueStore<String, Long> classroomMaxStore;
    private KeyValueStore<String, Long> classroomCurrentStore;

    public void init(ProcessorContext context) {
        this.context = context;
        studentStore = (KeyValueStore<String, String>) this.context.getStateStore("student-store");
        classroomMaxStore = (KeyValueStore<String, Long>) this.context.getStateStore("classroom-max-store");
        classroomCurrentStore = (KeyValueStore<String, Long>) this.context.getStateStore("classroom-current-store");
    }

    public void process(String roomId, String MaxOccupancy) {
        Long newMaxOccupancy = Long.parseLong(MaxOccupancy);
        Long currentOccupancy = classroomCurrentStore.get(roomId);
        Long oldMax = classroomMaxStore.get(roomId);
        if (oldMax != null) {
            if (oldMax < currentOccupancy && newMaxOccupancy >= currentOccupancy) {
                context().forward(roomId, "OK");
                context().commit();
            }
        }
        classroomMaxStore.put(roomId, newMaxOccupancy);
    }

}

public class A4Application {

    public static void main(String[] args) throws Exception {
        // do not modify the structure of the command line
        String bootstrapServers = args[0];
        String appName = args[1];
        String studentTopic = args[2];
        String classroomTopic = args[3];
        String outputTopic = args[4];
        String stateStoreDir = args[5];

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);

        // add code here if you need any additional configuration options

        //StreamsBuilder builder = new StreamsBuilder();
        Topology builder = new Topology();

        // add code here
        // 
            // ... = builder.stream(studentTopic);
            // ... = builder.stream(classroomTopic);
        // ...
        // ...to(outputTopic);
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        builder.addSource("STUDENT-SOURCE", studentTopic)
            .addSource("CLASSROOM-SOURCE",  classroomTopic)
            .addProcessor("student-processor", StudentProcessor::new, "STUDENT-SOURCE")
            .addProcessor("classroom-processor", ClassroomProcessor::new, "CLASSROOM-SOURCE")
            .addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("student-store"), stringSerde, stringSerde), "student-processor", "classroom-processor")
            .addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("classroom-max-store"), stringSerde, longSerde), "student-processor", "classroom-processor")
            .addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("classroom-current-store"), stringSerde, longSerde), "student-processor", "classroom-processor")
            .addSink("output", outputTopic, "student-processor", "classroom-processor");

        KafkaStreams streams = new KafkaStreams(builder, props);

        // this line initiates processing
        streams.start();

        // shutdown hook for Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
