package flink2;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;

public class ReadCollection {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment =  StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream1 = environment.fromCollection(Arrays.asList("a", "b", "c"));

        DataStreamSource dataStream2 = environment.fromElements(1, 2, 3, 4);

        DataStreamSource dataStream3 = environment.generateSequence(0, 100);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Tom", "./cart", 2000L));
        events.add(new Event("Ana", "./fav", 3000L));

        DataStreamSource<Event> dataStream4 = environment.fromCollection(events);
        DataStreamSource<Event> customStream = environment.addSource(new ClickSource());
        customStream.print();
        environment.execute();
    }
}
