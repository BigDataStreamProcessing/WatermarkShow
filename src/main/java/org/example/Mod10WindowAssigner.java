package org.example;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;

public class Mod10WindowAssigner extends WindowAssigner<Object, TimeWindow> {

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        // Pobierz wartość elementu
        long value = (Long) element;

        // Oblicz początek i koniec okna
        long windowStart = value - (value % 10);
        long windowEnd = windowStart + 10;

        // Przypisz element do jednego okna
        return Collections.singletonList(new TimeWindow(windowStart, windowEnd));
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        // W tym przypadku możemy zwrócić domyślny wyzwalacz, ponieważ nie używamy go w procesie
        return EventTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }

    @Override
    public String toString() {
        return "Mod10WindowAssigner";
    }
}
