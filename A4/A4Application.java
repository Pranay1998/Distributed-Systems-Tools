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

import java.util.Arrays;
import java.util.Properties;


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

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> student_info = builder.stream(studentTopic);
		KStream<String, String> room_occupancy = builder.stream(classroomTopic);

		KTable<String, String> studentToClassroom = student_info.groupByKey().reduce((x,y) -> y);
		KTable<String, Long> classroomToOccupancy = studentToClassroom.groupBy((x,y) -> new KeyValue<>(y, x)).count();

		KTable<String, String> classroomToCapacity = room_occupancy.groupByKey().reduce((x,y) -> y);

		KTable<String, String> classroomStatus = classroomToOccupancy.join(classroomToCapacity, (x,y) -> {
			return x.toString() + "," + y.toString();
		});

		KTable<String, String> output = classroomStatus.toStream().groupByKey().aggregate(() -> null, (x,y,z) -> {
			int a = Integer.parseInt(y.split(",")[0]);
			int b = Integer.parseInt(y.split(",")[1]);

			if (a > b) {
				return String.valueOf(a);
			} else {
				try {
					if (z == null) {
						return null;
					}
					Integer.parseInt(z);
					return "OK";
				} catch (Exception ex) {
					return null;
				}
			}
		});

		Serde<String> stringSerde = Serdes.String();

		output.toStream().filter((key, value) -> value != null).to(outputTopic, Produced.with(stringSerde, stringSerde));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
