package flink3;

import flink2.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformSimpleAggTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
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
        stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        }).max("timestamp").print("max: ");
        stream.keyBy(data -> data.user).maxBy("timestamp").print("maxby: ");

        environment.execute();
    }
}
