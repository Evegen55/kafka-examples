package app;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class App {

    private static final String HOST = "localhost";
    private static final String PORT = "9092";

    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";

    private static final String GROUP = "test-apps";

    private static final String APP_1_NAME = "producer";
    private static final String APP_2_NAME = "client-1";
    private static final String APP_3_NAME = "streams-processor-1-wordcount-application";
    private static final String APP_4_NAME = "client-2";

    private static final String TOPIC_1 = "quickstart-events";
    private static final String TOPIC_2 = "WordsWithCountsTopic";

    public static void main(String[] args) throws InterruptedException {
        new Thread(() -> startSimpleKafkaClient(APP_2_NAME, TOPIC_1)).start();
        new Thread(() -> startSimpleKafkaProducer(APP_1_NAME, TOPIC_1)).start();
        new Thread(() -> startSimpleKafkaStreamsApp(APP_3_NAME, TOPIC_1, TOPIC_2)).start();
        new Thread(() -> startSimpleKafkaClient(APP_4_NAME, TOPIC_2)).start();

        Thread.sleep(100000);
    }

    private static void startSimpleKafkaProducer(final String appName, final String topicName) {
        final Properties config = new Properties();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, HOST + ":" + PORT);
        config.put(CommonClientConfigs.CLIENT_ID_CONFIG, appName);
        config.put("acks", "all");
        config.put(KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(VALUE_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");

        final Producer<String, String> producer = new KafkaProducer<>(config);
        for (int i = 0; i < 100; i++) {
            final String toSend = Integer.toString(i) + UUID.randomUUID();
            producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), toSend));
            System.out.println(Thread.currentThread() + "\t" + appName + "\t" + toSend);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    private static void startSimpleKafkaClient(final String appName, final String topicName) {
        final Properties config = new Properties();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, HOST + ":" + PORT);
        config.put(CommonClientConfigs.GROUP_ID_CONFIG, GROUP);
        config.put(CommonClientConfigs.CLIENT_ID_CONFIG, appName);
        config.put("enable.auto.commit", "true");
        config.put("auto.commit.interval.ms", "1000");
        config.put(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG, "30000");
        config.put(KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(List.of(topicName));

        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("%s, %s, offset = %d, key = %s, value = %s%n",
                                  Thread.currentThread(),
                                  appName,
                                  record.offset(),
                                  record.key(),
                                  record.value());
            consumer.commitSync();
        }
    }

    private static void startSimpleKafkaStreamsApp(
            final String appName,
            final String topicNameFrom,
            final String topicNameTo
    ) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, HOST + ":" + PORT);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream(topicNameFrom);
        KTable<String, Long> wordCounts = textLines
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));

        wordCounts
                .toStream()
                .to(topicNameTo, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

}
