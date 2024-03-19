package flink2;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformRichMapFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements
                (
                        new Event("Mary", "./home", 1000L),
                        new Event("Tom", "./cart", 2000L),
                        new Event("Ana", "./fav", 3000L),
                        new Event("Bob", "./paymentsystem", 4000L),
                        new Event("Alice", "./ordersystem", 4900L),
                        new Event("Nancy", "./favorite", 5000L)
                );
        stream.map(new MyRichMapper()).print();
        env.execute();
    }

    public static class MyRichMapper extends RichMapFunction<Event, Integer> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("start of the lifecycle: " + getRuntimeContext().getIndexOfThisSubtask());
        }
        @Override
        public Integer map(Event value) throws Exception {
            return value.url.length();
        }
        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close of the lifecycle: " + getRuntimeContext().getIndexOfThisSubtask());
        }
    }
}
