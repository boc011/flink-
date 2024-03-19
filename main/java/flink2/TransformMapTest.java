package flink2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformMapTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Event> stream = environment.fromElements
                (
                        new Event("Mary", "./home", 1000L),
                        new Event("Tom", "./cart", 2000L),
                        new Event("Ana", "./fav", 3000L)
                );
        //stream.map(new MyMapper()).print();
        SingleOutputStreamOperator<String> result = stream.map(data -> data.url);
        result.print();
        environment.execute();
    }
    public static class MyMapper implements MapFunction<Event, String> {

        @Override
        public String map(Event value) throws Exception {
            return value.url;
        }
    }
}
