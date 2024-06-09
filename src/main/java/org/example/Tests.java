package org.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Tests {
    public static void main(String[] args) throws Exception {
        // Utwórz środowisko wykonawcze Flink
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Zdefiniuj niestandardowe źródło
        DataStreamSource<Long> rateSource = env.addSource(new SourceFunction<Long>() {
            private volatile boolean isRunning = true;
            private long currentIndex = 0;

            @Override
            public void run(SourceContext<Long> ctx) throws Exception {
                while (isRunning) {
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(currentIndex++);
                    }
                    Thread.sleep(1000); // Generowanie wartości co sekundę
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).setParallelism(1); // Ustawienie zrównoleglenia na 1 dla źródła

        WatermarkStrategy<Long> watermarkStrategy = WatermarkStrategy
                .<Long>forBoundedOutOfOrderness(Duration.ofMillis(10)) // Watermark o 10 ms mniejszy od największego znacznika czasowego
                .withTimestampAssigner(new SerializableTimestampAssigner<Long>() {
                    @Override
                    public long extractTimestamp(Long element, long recordTimestamp) {
                        return element; // Użyj wartości elementu jako znacznik czasowy
                    }
                });

        DataStream<Long> timestampedStream = rateSource
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // Wypisz dane na standardowe wyjście
        DataStream<Long> processedStream = timestampedStream
                .process(new ProcessFunction<Long, Long>() {
                    @Override
                    public void processElement(Long value, Context ctx, Collector<Long> out) throws Exception {
                        long timestamp = ctx.timestamp();
                        long watermark = ctx.timerService().currentWatermark();
                        long threadId = Thread.currentThread().getId();
                        int processFunctionInstanceId = this.hashCode();
                        System.out.println("BEFORE KEY: Event: " + value + ", Timestamp: " + timestamp + ", Current Watermark: " + watermark +
                                ", Thread ID: " + threadId + ", ProcessFunction Instance ID: " + processFunctionInstanceId);
                        out.collect(value);
                    }
                })
                .keyBy(value -> Math.toIntExact(value % 10)) // Kluczowanie wymagane przez KeyedProcessFunction
                .process(new KeyedProcessFunction<Integer, Long, Long>() {
                    @Override
                    public void processElement(Long value, Context ctx, Collector<Long> out) throws Exception {
                        long timestamp = ctx.timestamp();
                        long watermark = ctx.timerService().currentWatermark();
                        long threadId = Thread.currentThread().getId();
                        int processFunctionInstanceId = this.hashCode();
                        System.out.println("AFTER KEY: Event: " + value + ", Timestamp: " + timestamp + ", Current Watermark: " + watermark +
                                ", Thread ID: " + threadId + ", ProcessFunction Instance ID: " + processFunctionInstanceId);
                        out.collect(value);
                    }
                })
                .keyBy(value -> Math.toIntExact(value % 5)) // ponownie, bo process niszczy informację na temat klucza - wraca do DataStream
                //window(TumblingEventTimeWindows.of(Duration.ofMillis(10)))
                .window(new Mod10WindowAssigner())
                .sum(0)
                .process(new ProcessFunction<Long, Long>() {

                    @Override
                    public void processElement(Long value, Context ctx, Collector<Long> out) throws Exception {
                        long timestamp = ctx.timestamp();
                        long watermark = ctx.timerService().currentWatermark();
                        long threadId = Thread.currentThread().getId();
                        int processFunctionInstanceId = this.hashCode();
                        System.out.println("AFTER WINAGG: Event: " + value + ", Timestamp: " + timestamp + ", Current Watermark: " + watermark +
                                ", Thread ID: " + threadId + ", ProcessFunction Instance ID: " + processFunctionInstanceId);
                        out.collect(value);
                    }
                });

        // Wypisz dane na standardowe wyjście
        processedStream.print();

        // Uruchom środowisko Flink
        env.execute("Rate Source Example");
    }
}
