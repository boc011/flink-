package flink1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapExercise {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream stream = env.fromElements("white", "black", "gray");
        //如果是white，输出两次，如果是black输出一次，如果是gray不输出
        stream.flatMap(new MyFlatMapper()).print();
        env.execute();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            if (value.equals("white")) {
                out.collect(value);
                out.collect(value);
            } else if (value.equals("black")) {
                out.collect(value);
            }
        }
    }
}
