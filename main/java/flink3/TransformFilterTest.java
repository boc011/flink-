package flink3;

import flink2.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformFilterTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
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

        SingleOutputStreamOperator<Event> result = stream.filter(new MyFilter());
        result.print();
        environment.execute();
    }
    public static class MyFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event value) throws Exception {
            return value.user.length() == 4;
        }
    }
}
