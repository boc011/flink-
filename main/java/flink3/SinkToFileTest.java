package flink3;

import flink2.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SinkToFileTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);
        DataStreamSource<Event> stream = environment.fromElements
                (
                        new Event("Mary", "./home", 1000L),
                        new Event("Tom", "./cart", 2000L),
                        new Event("Mary", "./fav", 3000L),
                        new Event("Tom", "./paymentsystem", 4000L),
                        new Event("Tom", "./ordersystem", 4900L),
                        new Event("Nancy", "./favorite", 5000L),
                        new Event("Mary", "./home", 6000L),
                        new Event("Tom", "./cart", 7000L),
                        new Event("Mary", "./fav", 8000L),
                        new Event("Tom", "./paymentsystem", 8000L),
                        new Event("Tom", "./ordersystem", 9000L),
                        new Event("Nancy", "./favorite", 9100L)
                );
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(
                new Path("D:\\repo\\flink-example\\src\\main\\resources"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .withRolloverInterval(TimeUnit.MINUTES.toMinutes(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMinutes(5)).build()
                ).build();
        stream.map(data -> data.toString()).addSink(streamingFileSink);
        environment.execute();
    }
}
