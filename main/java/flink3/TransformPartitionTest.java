package flink3;

import flink2.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TransformPartitionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
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
        //stream.shuffle().print().setParallelism(4);
        //stream.rebalance().print().setParallelism(3);

        environment.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 1; i < 8; i++) {
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        ctx.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2).rescale().print().setParallelism(2);
        environment.execute();
    }
}
