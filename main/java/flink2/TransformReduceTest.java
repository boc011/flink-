package flink2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformReduceTest {

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
                        new Event("Nancy", "./favorite", 5000L)
                );
        SingleOutputStreamOperator<Tuple2<String, Long>> clickByUser = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.user, 1L);
            }
        }).keyBy(data -> data.f0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> result = clickByUser.keyBy(data -> "key")
                        .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                                if (value1.f1 >= value2.f1) {
                                    return value1;
                                }else {
                                    return value2;
                                }
                            }
                        });

        result.print();
        environment.execute();
    }
}
