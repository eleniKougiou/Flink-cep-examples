/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flinkCEP.cases;

import java.util.List;
import java.util.Map;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import flinkCEP.events.Event;

// Different contiguity choices, looping pattern example

public class CEPCase2 {

    public static void main (String[] args) throws Exception {

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set parallelism to 1
        env.setParallelism(1);

        // Create input sequence
        DataStream<Event> input = env.fromElements(
                new Event(1, "a"),
                new Event(1, "b1"),
                new Event(2, "c"),
                new Event(3, "b2"),
                new Event(4, "b3"),
                new Event(4, "d")
        );

        // ------ Create pattern "a b+" ------
        Pattern<Event, ?> start = Pattern.<Event>begin("start").where(new SimpleCondition<>() {
            @Override
            public boolean filter(Event value){
                return value.getName().startsWith("a");
            }
        });

        // Use oneOrMore() in  order to create the looping pattern b+

        // Strict contiguity
        // Use consecutive() in order to apply strict contiguity in a looping pattern
        Pattern<Event, ?> strict = start.next("next").where(new SimpleCondition<>() {
            @Override
            public boolean filter(Event value){
                return value.getName().startsWith("b");
            }
        }).oneOrMore().consecutive();

        // Relaxed contiguity
        // Default contiguity for looping patterns is Relaxed contiguity
        Pattern<Event, ?> relaxed = start.followedBy("next").where(new SimpleCondition<>() {
            @Override
            public boolean filter(Event value){
                return value.getName().startsWith("b");
            }
        }).oneOrMore();

        // Non - Deterministic Relaxed contiguity
        // Use allowCombinations() in order to apply non-deterministic relaxed contiguity in a looping pattern
        Pattern<Event, ?> nonDRelaxed = start.followedByAny("next").where(new SimpleCondition<>() {
            @Override
            public boolean filter(Event value){
                return value.getName().startsWith("b");
            }
        }).oneOrMore().allowCombinations();

        // Choose contiguity condition
        Pattern<Event, ?> pattern = strict;

        PatternStream<Event> patternStream = CEP.pattern(input, pattern);

        // Create result with matches
        DataStream<String> result = patternStream.select((Map<String, List<Event>> p) -> {
            String strResult = "";
            // Check if sth equals null so that the optional quantifier can be used
            if (p.get("start") != null){
                for (int i = 0; i < p.get("start").size(); i++){ // for looping patterns
                    strResult += p.get("start").get(i).getName() + " ";
                }
            }
            if (p.get("next") != null){
                for (int i = 0; i < p.get("next").size(); i++){
                    strResult += p.get("next").get(i).getName() + " ";
                }
            }
            return strResult;
        });

        // Print matches
        result.print();

        env.execute("Flink CEP Contiguity Conditions, Looping Pattern Example");
    }
}



