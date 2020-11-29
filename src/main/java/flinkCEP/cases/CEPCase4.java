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
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import flinkCEP.events.Event;

// Different after match skip strategies, looping pattern example 2
// Selected contiguity condition: Relaxed contiguity between a b and Strict between b's

public class CEPCase4 {

    public static void main (String[] args) throws Exception {

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Setting parallelism to 1
        env.setParallelism(1);

        DataStream<Event> input = env.fromElements(
                new Event(1, "a1"),
                new Event(1, "a2"),
                new Event(2, "b1"),
                new Event(3, "b2"),
                new Event(4, "b3")
        );

        // Create possible after match skip strategies
        AfterMatchSkipStrategy noSkip = AfterMatchSkipStrategy.noSkip();
        AfterMatchSkipStrategy skipToNext = AfterMatchSkipStrategy.skipToNext();
        AfterMatchSkipStrategy skipPastLastEvent = AfterMatchSkipStrategy.skipPastLastEvent();
        AfterMatchSkipStrategy skipToFirst = AfterMatchSkipStrategy.skipToFirst("start"); // Skip "start" event (a)
        AfterMatchSkipStrategy skipToLast = AfterMatchSkipStrategy.skipToLast("start"); // Skip "start" event (a)

        // Choose after match skip strategy
        AfterMatchSkipStrategy skipStrategy = skipToLast;

        // ------ Create pattern "a b+" ------
        Pattern<Event, ?> pattern = Pattern.<Event>begin("start", skipStrategy).where(new SimpleCondition<>() {
            @Override
            public boolean filter(Event value){
                return value.getName().startsWith("a");
            }
        }).followedBy("next").where(new SimpleCondition<>() {
            @Override
            public boolean filter(Event value){
                return value.getName().startsWith("b");
            }
        }).oneOrMore().consecutive();

        PatternStream<Event> patternStream = CEP.pattern(input, pattern);

        // Create result with matches
        DataStream<String> result = patternStream.select((Map<String, List<Event>> p) -> {
            String strResult = "";
            if (p.get("start") != null){
                for (int i = 0; i < p.get("start").size(); i++){
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

        env.execute("Flink CEP After Match Skip Strategies, Looping Pattern Example 2");
    }
}

