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
package org.apache.beam.sdk.transforms;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.sdk.testing.CombineFnTester.testCombineFn;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasNamespace;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.includesDisplayDataFor;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineTest.TestCombineFn.Accumulator;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.Context;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TenantAwareValue;
import org.apache.beam.sdk.values.TenantAwareValue.TenantAwareValueCoder;
import org.apache.beam.sdk.values.TimestampedValue;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for Combine transforms. */
@RunWith(JUnit4.class)
public class CombineTest implements Serializable {
  // This test is Serializable, just so that it's easy to have
  // anonymous inner classes inside the non-static test methods.

  static final List<TenantAwareValue<KV<String, Integer>>> EMPTY_TABLE = Collections.emptyList();

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  PCollection<KV<String, Integer>> createInput(
      Pipeline p, List<TenantAwareValue<KV<String, Integer>>> table) {
    return p.apply(
        Create.of(table).withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));
  }

  private void runTestSimpleCombine(
      List<TenantAwareValue<KV<String, Integer>>> table,
      int globalSum,
      List<TenantAwareValue<KV<String, String>>> perKeyCombines) {
    PCollection<KV<String, Integer>> input = createInput(pipeline, table);

    PCollection<Integer> sum = input.apply(Values.create()).apply(Combine.globally(new SumInts()));

    PCollection<KV<String, String>> sumPerKey = input.apply(Combine.perKey(new TestCombineFn()));

    PAssert.that(sum)
        .containsInAnyOrder(TenantAwareValue.of(TenantAwareValue.NULL_TENANT, globalSum));
    PAssert.that(sumPerKey).containsInAnyOrder(perKeyCombines);

    pipeline.run();
  }

  private void runTestSimpleCombineWithContext(
      List<TenantAwareValue<KV<String, Integer>>> table,
      int globalSum,
      List<TenantAwareValue<KV<String, String>>> perKeyCombines,
      TenantAwareValue<String>[] globallyCombines) {
    PCollection<KV<String, Integer>> perKeyInput = createInput(pipeline, table);
    PCollection<Integer> globallyInput = perKeyInput.apply(Values.create());

    PCollection<Integer> sum = globallyInput.apply("Sum", Combine.globally(new SumInts()));

    PCollectionView<Integer> globallySumView = sum.apply(View.asSingleton());

    PCollection<KV<String, String>> combinePerKey =
        perKeyInput.apply(
            Combine.<String, Integer, String>perKey(new TestCombineFnWithContext(globallySumView))
                .withSideInputs(globallySumView));

    PCollection<String> combineGlobally =
        globallyInput.apply(
            Combine.globally(new TestCombineFnWithContext(globallySumView))
                .withoutDefaults()
                .withSideInputs(globallySumView));

    PAssert.that(sum)
        .containsInAnyOrder(TenantAwareValue.of(TenantAwareValue.NULL_TENANT, globalSum));
    PAssert.that(combinePerKey).containsInAnyOrder(perKeyCombines);
    PAssert.that(combineGlobally).containsInAnyOrder(globallyCombines);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testSimpleCombine() {
    runTestSimpleCombine(
        Arrays.asList(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 1)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 1)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 4)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", 1)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", 13))),
        20,
        Arrays.asList(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", "114")),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", "113"))));
  }

  @Test
  @Category(ValidatesRunner.class)
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testSimpleCombineWithContext() {
    runTestSimpleCombineWithContext(
        Arrays.asList(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 1)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 1)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 4)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", 1)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", 13))),
        20,
        Arrays.asList(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", "20:114")),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", "20:113"))),
        new TenantAwareValue[] {TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "20:111134")});
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSimpleCombineWithContextEmpty() {
    runTestSimpleCombineWithContext(
        EMPTY_TABLE, 0, Collections.emptyList(), new TenantAwareValue[] {});
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSimpleCombineEmpty() {
    runTestSimpleCombine(EMPTY_TABLE, 0, Collections.emptyList());
  }

  @SuppressWarnings("unchecked")
  private void runTestBasicCombine(
      List<TenantAwareValue<KV<String, Integer>>> table,
      TenantAwareValue<Set<Integer>> globalUnique,
      List<TenantAwareValue<KV<String, Set<Integer>>>> perKeyUnique) {
    PCollection<KV<String, Integer>> input = createInput(pipeline, table);

    PCollection<Set<Integer>> unique =
        input.apply(Values.create()).apply(Combine.globally(new UniqueInts()));

    PCollection<KV<String, Set<Integer>>> uniquePerKey =
        input.apply(Combine.perKey(new UniqueInts()));

    PAssert.that(unique).containsInAnyOrder(globalUnique);
    PAssert.that(uniquePerKey).containsInAnyOrder(perKeyUnique);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testBasicCombine() {
    runTestBasicCombine(
        Arrays.asList(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 1)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 1)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 4)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", 1)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", 13))),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, ImmutableSet.of(1, 13, 4)),
        Arrays.asList(
            TenantAwareValue.of(
                TenantAwareValue.NULL_TENANT, KV.of("a", (Set<Integer>) ImmutableSet.of(1, 4))),
            TenantAwareValue.of(
                TenantAwareValue.NULL_TENANT, KV.of("b", (Set<Integer>) ImmutableSet.of(1, 13)))));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testBasicCombineEmpty() {
    runTestBasicCombine(
        EMPTY_TABLE,
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, ImmutableSet.of()),
        Collections.emptyList());
  }

  private void runTestAccumulatingCombine(
      List<TenantAwareValue<KV<String, Integer>>> table,
      TenantAwareValue<Double> globalMean,
      List<TenantAwareValue<KV<String, Double>>> perKeyMeans) {
    PCollection<KV<String, Integer>> input = createInput(pipeline, table);

    PCollection<Double> mean = input.apply(Values.create()).apply(Combine.globally(new MeanInts()));

    PCollection<KV<String, Double>> meanPerKey = input.apply(Combine.perKey(new MeanInts()));

    PAssert.that(mean).containsInAnyOrder(globalMean);
    PAssert.that(meanPerKey).containsInAnyOrder(perKeyMeans);

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testFixedWindowsCombine() {
    PCollection<KV<String, Integer>> input =
        pipeline
            .apply(
                Create.timestamped(
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("a", 1), new Instant(0L))),
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("a", 1), new Instant(1L))),
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("a", 4), new Instant(6L))),
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("b", 1), new Instant(7L))),
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("b", 13), new Instant(8L))))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
            .apply(Window.into(FixedWindows.of(Duration.millis(2))));

    PCollection<Integer> sum =
        input.apply(Values.create()).apply(Combine.globally(new SumInts()).withoutDefaults());

    PCollection<KV<String, String>> sumPerKey = input.apply(Combine.perKey(new TestCombineFn()));

    PAssert.that(sum)
        .containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 5),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 13));
    PAssert.that(sumPerKey)
        .containsInAnyOrder(
            Arrays.asList(
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", "11")),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", "4")),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", "1")),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", "13"))));
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testFixedWindowsCombineWithContext() {
    PCollection<KV<String, Integer>> perKeyInput =
        pipeline
            .apply(
                Create.timestamped(
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("a", 1), new Instant(0L))),
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("a", 1), new Instant(1L))),
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("a", 4), new Instant(6L))),
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("b", 1), new Instant(7L))),
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("b", 13), new Instant(8L))))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
            .apply(Window.into(FixedWindows.of(Duration.millis(2))));

    PCollection<Integer> globallyInput = perKeyInput.apply(Values.create());

    PCollection<Integer> sum =
        globallyInput.apply("Sum", Combine.globally(new SumInts()).withoutDefaults());

    PCollectionView<Integer> globallySumView = sum.apply(View.asSingleton());

    PCollection<KV<String, String>> combinePerKeyWithContext =
        perKeyInput.apply(
            Combine.<String, Integer, String>perKey(new TestCombineFnWithContext(globallySumView))
                .withSideInputs(globallySumView));

    PCollection<String> combineGloballyWithContext =
        globallyInput.apply(
            Combine.globally(new TestCombineFnWithContext(globallySumView))
                .withoutDefaults()
                .withSideInputs(globallySumView));

    PAssert.that(sum)
        .containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 5),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 13));
    PAssert.that(combinePerKeyWithContext)
        .containsInAnyOrder(
            Arrays.asList(
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", "2:11")),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", "5:4")),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", "5:1")),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", "13:13"))));
    PAssert.that(combineGloballyWithContext)
        .containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "2:11"),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "5:14"),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "13:13"));
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSlidingWindowsCombine() {
    PCollection<String> input =
        pipeline
            .apply(
                Create.timestamped(
                    TenantAwareValue.of(
                        TenantAwareValue.NULL_TENANT, TimestampedValue.of("a", new Instant(1L))),
                    TenantAwareValue.of(
                        TenantAwareValue.NULL_TENANT, TimestampedValue.of("b", new Instant(2L))),
                    TenantAwareValue.of(
                        TenantAwareValue.NULL_TENANT, TimestampedValue.of("c", new Instant(3L)))))
            .apply(Window.into(SlidingWindows.of(Duration.millis(3)).every(Duration.millis(1L))));
    PCollection<List<String>> combined =
        input.apply(
            Combine.globally(
                    new CombineFn<String, List<String>, List<String>>() {
                      @Override
                      public TenantAwareValue<List<String>> createAccumulator() {
                        return TenantAwareValue.of(TenantAwareValue.NULL_TENANT, new ArrayList<>());
                      }

                      @Override
                      public TenantAwareValue<List<String>> addInput(
                          TenantAwareValue<List<String>> accumulator,
                          TenantAwareValue<String> input) {
                        accumulator.getValue().add(input.getValue());
                        return TenantAwareValue.of(input.getTenantId(), accumulator.getValue());
                      }

                      @Override
                      public TenantAwareValue<List<String>> mergeAccumulators(
                          Iterable<TenantAwareValue<List<String>>> accumulators) {
                        // Mutate all of the accumulators. Instances should be used in only one
                        // place, and not
                        // reused after merging.
                        TenantAwareValue<List<String>> cur = createAccumulator();
                        String tenantId = TenantAwareValue.NULL_TENANT;
                        for (TenantAwareValue<List<String>> accumulator : accumulators) {
                          accumulator.getValue().addAll(cur.getValue());
                          tenantId = cur.getTenantId();
                          cur = accumulator;
                        }
                        return cur;
                      }

                      @Override
                      public TenantAwareValue<List<String>> extractOutput(
                          TenantAwareValue<List<String>> accumulator) {
                        List<String> result = new ArrayList<>(accumulator.getValue());
                        Collections.sort(result);
                        return TenantAwareValue.of(accumulator.getTenantId(), result);
                      }
                    })
                .withoutDefaults());

    PAssert.that(combined)
        .containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, ImmutableList.of("a")),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, ImmutableList.of("a", "b")),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, ImmutableList.of("a", "b", "c")),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, ImmutableList.of("b", "c")),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, ImmutableList.of("c")));

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSlidingWindowsCombineWithContext() {
    // [a: 1, 1], [a: 4; b: 1], [b: 13]
    PCollection<KV<String, Integer>> perKeyInput =
        pipeline
            .apply(
                Create.timestamped(
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("a", 1), new Instant(2L))),
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("a", 1), new Instant(3L))),
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("a", 4), new Instant(8L))),
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("b", 1), new Instant(9L))),
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("b", 13), new Instant(10L))))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
            .apply(Window.into(SlidingWindows.of(Duration.millis(2))));

    PCollection<Integer> globallyInput = perKeyInput.apply(Values.create());

    PCollection<Integer> sum = globallyInput.apply("Sum", Sum.integersGlobally().withoutDefaults());

    PCollectionView<Integer> globallySumView = sum.apply(View.asSingleton());

    PCollection<KV<String, String>> combinePerKeyWithContext =
        perKeyInput.apply(
            Combine.<String, Integer, String>perKey(new TestCombineFnWithContext(globallySumView))
                .withSideInputs(globallySumView));

    PCollection<String> combineGloballyWithContext =
        globallyInput.apply(
            Combine.globally(new TestCombineFnWithContext(globallySumView))
                .withoutDefaults()
                .withSideInputs(globallySumView));

    PAssert.that(sum)
        .containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 5),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 14),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 13));
    PAssert.that(combinePerKeyWithContext)
        .containsInAnyOrder(
            Arrays.asList(
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", "1:1")),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", "2:11")),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", "1:1")),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", "4:4")),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", "5:4")),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", "5:1")),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", "14:113")),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", "13:13"))));
    PAssert.that(combineGloballyWithContext)
        .containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "1:1"),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "2:11"),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "1:1"),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "4:4"),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "5:14"),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "14:113"),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "13:13"));
    pipeline.run();
  }

  private static class FormatPaneInfo extends DoFn<Integer, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element() + ": " + c.pane().isLast());
    }
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testGlobalCombineWithDefaultsAndTriggers() {
    PCollection<Integer> input =
        pipeline.apply(
            Create.of(
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1)));

    PCollection<String> output =
        input
            .apply(
                Window.<Integer>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                    .accumulatingFiredPanes()
                    .withAllowedLateness(new Duration(0), ClosingBehavior.FIRE_ALWAYS))
            .apply(Sum.integersGlobally())
            .apply(ParDo.of(new FormatPaneInfo()));

    // The actual elements produced are nondeterministic. Could be one, could be two.
    // But it should certainly have a final element with the correct final sum.
    PAssert.that(output)
        .satisfies(
            input1 -> {
              assertThat(input1.getValue(), hasItem("2: true"));
              return null;
            });

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSessionsCombine() {
    PCollection<KV<String, Integer>> input =
        pipeline
            .apply(
                Create.timestamped(
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("a", 1), new Instant(0L))),
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("a", 1), new Instant(4L))),
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("a", 4), new Instant(7L))),
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("b", 1), new Instant(10L))),
                        TenantAwareValue.of(
                            TenantAwareValue.NULL_TENANT,
                            TimestampedValue.of(KV.of("b", 13), new Instant(16L))))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
            .apply(Window.into(Sessions.withGapDuration(Duration.millis(5))));

    PCollection<Integer> sum =
        input.apply(Values.create()).apply(Combine.globally(new SumInts()).withoutDefaults());

    PCollection<KV<String, String>> sumPerKey = input.apply(Combine.perKey(new TestCombineFn()));

    PAssert.that(sum)
        .containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 7),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 13));
    PAssert.that(sumPerKey)
        .containsInAnyOrder(
            Arrays.asList(
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", "114")),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", "1")),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", "13"))));
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSessionsCombineWithContext() {
    PCollection<KV<String, Integer>> perKeyInput =
        pipeline.apply(
            Create.timestamped(
                    TenantAwareValue.of(
                        TenantAwareValue.NULL_TENANT,
                        TimestampedValue.of(KV.of("a", 1), new Instant(0L))),
                    TenantAwareValue.of(
                        TenantAwareValue.NULL_TENANT,
                        TimestampedValue.of(KV.of("a", 1), new Instant(4L))),
                    TenantAwareValue.of(
                        TenantAwareValue.NULL_TENANT,
                        TimestampedValue.of(KV.of("a", 4), new Instant(7L))),
                    TenantAwareValue.of(
                        TenantAwareValue.NULL_TENANT,
                        TimestampedValue.of(KV.of("b", 1), new Instant(10L))),
                    TenantAwareValue.of(
                        TenantAwareValue.NULL_TENANT,
                        TimestampedValue.of(KV.of("b", 13), new Instant(16L))))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PCollection<Integer> globallyInput = perKeyInput.apply(Values.create());

    PCollection<Integer> fixedWindowsSum =
        globallyInput
            .apply("FixedWindows", Window.into(FixedWindows.of(Duration.millis(5))))
            .apply("Sum", Combine.globally(new SumInts()).withoutDefaults());

    PCollectionView<Integer> globallyFixedWindowsView =
        fixedWindowsSum.apply(View.<Integer>asSingleton().withDefaultValue(0));

    PCollection<KV<String, String>> sessionsCombinePerKey =
        perKeyInput
            .apply(
                "PerKey Input Sessions", Window.into(Sessions.withGapDuration(Duration.millis(5))))
            .apply(
                Combine.<String, Integer, String>perKey(
                        new TestCombineFnWithContext(globallyFixedWindowsView))
                    .withSideInputs(globallyFixedWindowsView));

    PCollection<String> sessionsCombineGlobally =
        globallyInput
            .apply(
                "Globally Input Sessions",
                Window.into(Sessions.withGapDuration(Duration.millis(5))))
            .apply(
                Combine.globally(new TestCombineFnWithContext(globallyFixedWindowsView))
                    .withoutDefaults()
                    .withSideInputs(globallyFixedWindowsView));

    PAssert.that(fixedWindowsSum)
        .containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 13));
    PAssert.that(sessionsCombinePerKey)
        .containsInAnyOrder(
            Arrays.asList(
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", "1:114")),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", "1:1")),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", "0:13"))));
    PAssert.that(sessionsCombineGlobally)
        .containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "1:1114"),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "0:13"));
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testWindowedCombineEmpty() {
    PCollection<Double> mean =
        pipeline
            .apply(Create.empty(BigEndianIntegerCoder.of()))
            .apply(Window.into(FixedWindows.of(Duration.millis(1))))
            .apply(Combine.globally(new MeanInts()).withoutDefaults());

    PAssert.that(mean).empty();

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testAccumulatingCombine() {
    runTestAccumulatingCombine(
        Arrays.asList(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 1)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 1)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 4)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", 1)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", 13))),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4.0),
        Arrays.asList(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 2.0)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", 7.0))));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testAccumulatingCombineEmpty() {
    runTestAccumulatingCombine(
        EMPTY_TABLE,
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 0.0),
        Collections.emptyList());
  }

  // Checks that Min, Max, Mean, Sum (operations that pass-through to Combine) have good names.
  @Test
  public void testCombinerNames() {
    Combine.PerKey<String, Integer, Integer> min = Min.integersPerKey();
    Combine.PerKey<String, Integer, Integer> max = Max.integersPerKey();
    Combine.PerKey<String, Integer, Double> mean = Mean.perKey();
    Combine.PerKey<String, Integer, Integer> sum = Sum.integersPerKey();

    assertThat(min.getName(), equalTo("Combine.perKey(MinInteger)"));
    assertThat(max.getName(), equalTo("Combine.perKey(MaxInteger)"));
    assertThat(mean.getName(), equalTo("Combine.perKey(Mean)"));
    assertThat(sum.getName(), equalTo("Combine.perKey(SumInteger)"));
  }

  private static final SerializableFunction<String, Integer> hotKeyFanout =
      input ->
          input.getValue().equals("a")
              ? TenantAwareValue.of(input.getTenantId(), 3)
              : TenantAwareValue.of(input.getTenantId(), 0);

  private static final SerializableFunction<String, Integer> splitHotKeyFanout =
      input ->
          Math.random() < 0.5
              ? TenantAwareValue.of(input.getTenantId(), 3)
              : TenantAwareValue.of(input.getTenantId(), 0);

  @Test
  @Category(ValidatesRunner.class)
  public void testHotKeyCombining() {
    PCollection<KV<String, Integer>> input =
        copy(
            createInput(
                pipeline,
                Arrays.asList(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 1)),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 1)),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 4)),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", 1)),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", 13)))),
            10);

    CombineFn<Integer, ?, Double> mean = new MeanInts();
    PCollection<KV<String, Double>> coldMean =
        input.apply("ColdMean", Combine.<String, Integer, Double>perKey(mean).withHotKeyFanout(0));
    PCollection<KV<String, Double>> warmMean =
        input.apply(
            "WarmMean",
            Combine.<String, Integer, Double>perKey(mean).withHotKeyFanout(hotKeyFanout));
    PCollection<KV<String, Double>> hotMean =
        input.apply("HotMean", Combine.<String, Integer, Double>perKey(mean).withHotKeyFanout(5));
    PCollection<KV<String, Double>> splitMean =
        input.apply(
            "SplitMean",
            Combine.<String, Integer, Double>perKey(mean).withHotKeyFanout(splitHotKeyFanout));

    List<TenantAwareValue<KV<String, Double>>> expected =
        Arrays.asList(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 2.0)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", 7.0)));
    PAssert.that(coldMean).containsInAnyOrder(expected);
    PAssert.that(warmMean).containsInAnyOrder(expected);
    PAssert.that(hotMean).containsInAnyOrder(expected);
    PAssert.that(splitMean).containsInAnyOrder(expected);

    pipeline.run();
  }

  private static class GetLast extends DoFn<Integer, Integer> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.pane().isLast()) {
        c.output(c.element());
      }
    }
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testHotKeyCombiningWithAccumulationMode() {
    PCollection<Integer> input =
        pipeline.apply(
            Create.of(
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 3),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 5)));

    PCollection<Integer> output =
        input
            .apply(
                Window.<Integer>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                    .accumulatingFiredPanes()
                    .withAllowedLateness(new Duration(0), ClosingBehavior.FIRE_ALWAYS))
            .apply(Sum.integersGlobally().withoutDefaults().withFanout(2))
            .apply(ParDo.of(new GetLast()));

    PAssert.that(output)
        .satisfies(
            input1 -> {
              assertThat(input1.getValue(), hasItem(15));
              return null;
            });

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testBinaryCombineFn() {
    PCollection<KV<String, Integer>> input =
        copy(
            createInput(
                pipeline,
                Arrays.asList(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 1)),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 1)),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 4)),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", 1)),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", 13)))),
            2);
    PCollection<KV<String, Integer>> intProduct =
        input.apply("IntProduct", Combine.perKey(new TestProdInt()));
    PCollection<KV<String, Integer>> objProduct =
        input.apply("ObjProduct", Combine.perKey(new TestProdObj()));

    List<TenantAwareValue<KV<String, Integer>>> expected =
        Arrays.asList(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("a", 16)),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("b", 169)));
    PAssert.that(intProduct).containsInAnyOrder(expected);
    PAssert.that(objProduct).containsInAnyOrder(expected);

    pipeline.run();
  }

  @Test
  public void testBinaryCombineFnWithNulls() {
    testCombineFn(
        new NullCombiner(),
        Arrays.asList(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 3),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 3),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 5)),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 45));
    testCombineFn(
        new NullCombiner(),
        Arrays.asList(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, (Integer) null),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 3),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 5)),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 30));
    testCombineFn(
        new NullCombiner(),
        Arrays.asList(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 3),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 3),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, (Integer) null)),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 18));
    testCombineFn(
        new NullCombiner(),
        Arrays.asList(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, (Integer) null),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 3),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, (Integer) null)),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 12));
    testCombineFn(
        new NullCombiner(),
        Arrays.asList(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, (Integer) null),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, (Integer) null),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, (Integer) null)),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 8));
  }

  private static final class TestProdInt extends Combine.BinaryCombineIntegerFn {
    @Override
    public TenantAwareValue<Integer> apply(
        TenantAwareValue<Integer> left, TenantAwareValue<Integer> right) {
      return TenantAwareValue.of(right.getTenantId(), left.getValue() * right.getValue());
    }

    @Override
    public int identity() {
      return 1;
    }
  }

  private static final class TestProdObj extends Combine.BinaryCombineFn<Integer> {
    public TenantAwareValue<Integer> apply(
        TenantAwareValue<Integer> left, TenantAwareValue<Integer> right) {
      return TenantAwareValue.of(right.getTenantId(), left.getValue() * right.getValue());
    }
  }

  /** Computes the product, considering null values to be 2. */
  private static final class NullCombiner extends Combine.BinaryCombineFn<Integer> {
    @Override
    public TenantAwareValue<Integer> apply(
        TenantAwareValue<Integer> left, TenantAwareValue<Integer> right) {
      return TenantAwareValue.of(
          right.getTenantId(),
          (left == null ? 2 : left.getValue()) * (right == null ? 2 : right.getValue()));
    }
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testCombineGloballyAsSingletonView() {
    final PCollectionView<Integer> view =
        pipeline
            .apply("CreateEmptySideInput", Create.empty(BigEndianIntegerCoder.of()))
            .apply(Sum.integersGlobally().asSingletonView());

    PCollection<Integer> output =
        pipeline
            .apply(
                "CreateVoidMainInput",
                Create.of(TenantAwareValue.of(TenantAwareValue.NULL_TENANT, (Void) null)))
            .apply(
                "OutputSideInput",
                ParDo.of(
                        new DoFn<Void, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(c.sideInput(view));
                          }
                        })
                    .withSideInputs(view));

    PAssert.thatSingleton(output).isEqualTo(TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 0));
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testWindowedCombineGloballyAsSingletonView() {
    FixedWindows windowFn = FixedWindows.of(Duration.standardMinutes(1));
    final PCollectionView<Integer> view =
        pipeline
            .apply(
                "CreateSideInput",
                Create.timestamped(
                    TenantAwareValue.of(
                        TenantAwareValue.NULL_TENANT, TimestampedValue.of(1, new Instant(100))),
                    TenantAwareValue.of(
                        TenantAwareValue.NULL_TENANT, TimestampedValue.of(3, new Instant(100)))))
            .apply("WindowSideInput", Window.into(windowFn))
            .apply("CombineSideInput", Sum.integersGlobally().asSingletonView());

    TimestampedValue<Void> nonEmptyElement = TimestampedValue.of(null, new Instant(100));
    TimestampedValue<Void> emptyElement = TimestampedValue.atMinimumTimestamp(null);
    PCollection<Integer> output =
        pipeline
            .apply(
                "CreateMainInput",
                Create.timestamped(
                        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, nonEmptyElement),
                        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, emptyElement))
                    .withCoder(VoidCoder.of()))
            .apply("WindowMainInput", Window.into(windowFn))
            .apply(
                "OutputSideInput",
                ParDo.of(
                        new DoFn<Void, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output(c.sideInput(view));
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output)
        .containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 0));
    PAssert.that(output)
        .inWindow(windowFn.assignWindow(nonEmptyElement.getTimestamp()))
        .containsInAnyOrder(TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4));
    PAssert.that(output)
        .inWindow(windowFn.assignWindow(emptyElement.getTimestamp()))
        .containsInAnyOrder(TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 0));
    pipeline.run();
  }

  @Test
  public void testCombineGetName() {
    assertEquals("Combine.globally(SumInts)", Combine.globally(new SumInts()).getName());
    assertEquals(
        "Combine.GloballyAsSingletonView",
        Combine.globally(new SumInts()).asSingletonView().getName());
    assertEquals("Combine.perKey(Test)", Combine.perKey(new TestCombineFn()).getName());
    assertEquals(
        "Combine.perKeyWithFanout(Test)",
        Combine.perKey(new TestCombineFn()).withHotKeyFanout(10).getName());
  }

  @Test
  public void testDisplayData() {
    UniqueInts combineFn =
        new UniqueInts() {
          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("fnMetadata", "foobar"));
          }
        };
    Combine.Globally<?, ?> combine = Combine.globally(combineFn).withFanout(1234);
    DisplayData displayData = DisplayData.from(combine);

    assertThat(displayData, hasDisplayItem("combineFn", combineFn.getClass()));
    assertThat(displayData, hasDisplayItem("emitDefaultOnEmptyInput", true));
    assertThat(displayData, hasDisplayItem("fanout", 1234));
    assertThat(displayData, includesDisplayDataFor("combineFn", combineFn));
  }

  @Test
  public void testDisplayDataForWrappedFn() {
    UniqueInts combineFn =
        new UniqueInts() {
          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("foo", "bar"));
          }
        };
    Combine.PerKey<?, ?, ?> combine = Combine.perKey(combineFn);
    DisplayData displayData = DisplayData.from(combine);

    assertThat(displayData, hasDisplayItem("combineFn", combineFn.getClass()));
    assertThat(displayData, hasDisplayItem(hasNamespace(combineFn.getClass())));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testCombinePerKeyPrimitiveDisplayData() {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();

    CombineTest.UniqueInts combineFn = new CombineTest.UniqueInts();
    PTransform<PCollection<KV<Integer, Integer>>, ? extends POutput> combine =
        Combine.perKey(combineFn);

    Set<DisplayData> displayData =
        evaluator.displayDataForPrimitiveTransforms(
            combine, KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));

    assertThat(
        "Combine.perKey should include the combineFn in its primitive transform",
        displayData,
        hasItem(hasDisplayItem("combineFn", combineFn.getClass())));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testCombinePerKeyWithHotKeyFanoutPrimitiveDisplayData() {
    int hotKeyFanout = 2;
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();

    CombineTest.UniqueInts combineFn = new CombineTest.UniqueInts();
    PTransform<PCollection<KV<Integer, Integer>>, PCollection<KV<Integer, Set<Integer>>>> combine =
        Combine.<Integer, Integer, Set<Integer>>perKey(combineFn).withHotKeyFanout(hotKeyFanout);

    Set<DisplayData> displayData =
        evaluator.displayDataForPrimitiveTransforms(
            combine, KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));

    assertThat(
        "Combine.perKey.withHotKeyFanout should include the combineFn in its primitive "
            + "transform",
        displayData,
        hasItem(hasDisplayItem("combineFn", combineFn.getClass())));
    assertThat(
        "Combine.perKey.withHotKeyFanout(int) should include the fanout in its primitive "
            + "transform",
        displayData,
        hasItem(hasDisplayItem("fanout", hotKeyFanout)));
  }

  ////////////////////////////////////////////////////////////////////////////
  // Test classes, for different kinds of combining fns.

  /** Example SerializableFunction combiner. */
  public static class SumInts implements SerializableFunction<Iterable<Integer>, Integer> {
    @Override
    public TenantAwareValue<Integer> apply(TenantAwareValue<Iterable<Integer>> input) {
      int sum = 0;
      String tenantId = TenantAwareValue.NULL_TENANT;
      for (int item : input.getValue()) {
        tenantId = input.getTenantId();
        sum += item;
      }
      return TenantAwareValue.of(tenantId, sum);
    }
  }

  /** Example CombineFn. */
  public static class UniqueInts extends Combine.CombineFn<Integer, Set<Integer>, Set<Integer>> {

    @Override
    public TenantAwareValue<Set<Integer>> createAccumulator() {
      return TenantAwareValue.of(TenantAwareValue.NULL_TENANT, new HashSet<>());
    }

    @Override
    public TenantAwareValue<Set<Integer>> addInput(
        TenantAwareValue<Set<Integer>> accumulator, TenantAwareValue<Integer> input) {
      accumulator.getValue().add(input.getValue());

      return TenantAwareValue.of(input.getTenantId(), accumulator.getValue());
    }

    @Override
    public TenantAwareValue<Set<Integer>> mergeAccumulators(
        Iterable<TenantAwareValue<Set<Integer>>> accumulators) {
      Set<Integer> all = new HashSet<>();
      String tenantId = TenantAwareValue.NULL_TENANT;
      for (TenantAwareValue<Set<Integer>> part : accumulators) {
        all.addAll(part.getValue());
        tenantId = part.getTenantId();
      }
      return TenantAwareValue.of(tenantId, all);
    }

    @Override
    public TenantAwareValue<Set<Integer>> extractOutput(
        TenantAwareValue<Set<Integer>> accumulator) {
      return accumulator;
    }
  }

  /** Example AccumulatingCombineFn. */
  private static class MeanInts
      extends Combine.AccumulatingCombineFn<Integer, MeanInts.CountSum, Double> {
    private static final Coder<Long> LONG_CODER = BigEndianLongCoder.of();
    private static final Coder<Double> DOUBLE_CODER = DoubleCoder.of();

    class CountSum implements Combine.AccumulatingCombineFn.Accumulator<Integer, CountSum, Double> {
      long count = 0;
      double sum = 0.0;
      String tenantId;

      CountSum(long count, double sum) {
        this.count = count;
        this.sum = sum;
        this.tenantId = TenantAwareValue.NULL_TENANT;
      }

      @Override
      public void addInput(TenantAwareValue<Integer> element) {
        count++;
        sum += element.getValue().doubleValue();
      }

      @Override
      public void mergeAccumulator(CountSum accumulator) {
        count += accumulator.count;
        sum += accumulator.sum;
      }

      @Override
      public TenantAwareValue<Double> extractOutput() {
        return TenantAwareValue.of(tenantId, count == 0 ? 0.0 : sum / count);
      }

      @Override
      public int hashCode() {
        return Objects.hash(count, sum);
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == this) {
          return true;
        }
        if (!(obj instanceof CountSum)) {
          return false;
        }
        CountSum other = (CountSum) obj;
        return this.count == other.count && (Math.abs(this.sum - other.sum) < 0.1);
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper(this).add("count", count).add("sum", sum).toString();
      }
    }

    @Override
    public TenantAwareValue<CountSum> createAccumulator() {
      return TenantAwareValue.of(TenantAwareValue.NULL_TENANT, new CountSum(0, 0.0));
    }

    @Override
    public TenantAwareValueCoder<CountSum> getAccumulatorCoder(
        CoderRegistry registry, Coder<Integer> inputCoder) {
      return TenantAwareValueCoder.of(new CountSumCoder());
    }

    /** A {@link Coder} for {@link CountSum}. */
    private class CountSumCoder extends AtomicCoder<CountSum> {
      @Override
      public void encode(CountSum value, OutputStream outStream) throws IOException {
        LONG_CODER.encode(value.count, outStream);
        DOUBLE_CODER.encode(value.sum, outStream);
      }

      @Override
      public CountSum decode(InputStream inStream) throws IOException {
        long count = LONG_CODER.decode(inStream);
        double sum = DOUBLE_CODER.decode(inStream);
        return new CountSum(count, sum);
      }

      @Override
      public void verifyDeterministic() throws NonDeterministicException {}

      @Override
      public boolean isRegisterByteSizeObserverCheap(CountSum value) {
        return true;
      }

      @Override
      public void registerByteSizeObserver(CountSum value, ElementByteSizeObserver observer)
          throws Exception {
        LONG_CODER.registerByteSizeObserver(value.count, observer);
        DOUBLE_CODER.registerByteSizeObserver(value.sum, observer);
      }
    }
  }

  /**
   * A {@link CombineFn} that results in a sorted list of all characters occurring in the key and
   * the decimal representations of each value.
   */
  public static class TestCombineFn extends CombineFn<Integer, TestCombineFn.Accumulator, String> {

    // Not serializable.
    static class Accumulator {
      final String seed;
      String value;

      public Accumulator(String seed, String value) {
        this.seed = seed;
        this.value = value;
      }

      public static Coder<Accumulator> getCoder() {
        return new AtomicCoder<Accumulator>() {
          @Override
          public void encode(Accumulator accumulator, OutputStream outStream) throws IOException {
            StringUtf8Coder.of().encode(accumulator.seed, outStream);
            StringUtf8Coder.of().encode(accumulator.value, outStream);
          }

          @Override
          public Accumulator decode(InputStream inStream) throws IOException {
            String seed = StringUtf8Coder.of().decode(inStream);
            String value = StringUtf8Coder.of().decode(inStream);
            return new Accumulator(seed, value);
          }
        };
      }
    }

    @Override
    public TenantAwareValueCoder<Accumulator> getAccumulatorCoder(
        CoderRegistry registry, Coder<Integer> inputCoder) {
      return TenantAwareValueCoder.of(Accumulator.getCoder());
    }

    @Override
    public TenantAwareValue<Accumulator> createAccumulator() {
      return TenantAwareValue.of(TenantAwareValue.NULL_TENANT, new Accumulator("", ""));
    }

    @Override
    public TenantAwareValue<Accumulator> addInput(
        TenantAwareValue<Accumulator> accumulator, TenantAwareValue<Integer> value) {
      try {
        return TenantAwareValue.of(
            value.getTenantId(),
            new Accumulator(
                accumulator.getValue().seed,
                accumulator.getValue().value + String.valueOf(value.getValue())));
      } finally {
        accumulator.getValue().value = "cleared in addInput";
      }
    }

    @Override
    public TenantAwareValue<Accumulator> mergeAccumulators(
        Iterable<TenantAwareValue<Accumulator>> accumulators) {
      Accumulator seedAccumulator = null;
      String tenatnId = TenantAwareValue.NULL_TENANT;
      StringBuilder all = new StringBuilder();
      for (TenantAwareValue<Accumulator> accumulator : accumulators) {
        if (seedAccumulator == null) {
          seedAccumulator = accumulator.getValue();
          tenatnId = accumulator.getTenantId();
        } else {
          assertEquals(
              String.format(
                  "Different seed values in accumulator: %s vs. %s", seedAccumulator, accumulator),
              seedAccumulator.seed,
              accumulator.getValue().seed);
        }
        all.append(accumulator.getValue().value);
        accumulator.getValue().value = "cleared in mergeAccumulators";
      }
      return TenantAwareValue.of(
          tenatnId, new Accumulator(checkNotNull(seedAccumulator).seed, all.toString()));
    }

    @Override
    public TenantAwareValue<String> extractOutput(TenantAwareValue<Accumulator> accumulator) {
      char[] chars = accumulator.getValue().value.toCharArray();
      Arrays.sort(chars);
      return TenantAwareValue.of(accumulator.getTenantId(), new String(chars));
    }
  }

  /**
   * A {@link CombineFnWithContext} that produces a sorted list of all characters occurring in the
   * key and the decimal representations of main and side inputs values.
   */
  public class TestCombineFnWithContext extends CombineFnWithContext<Integer, Accumulator, String> {
    private final PCollectionView<Integer> view;

    public TestCombineFnWithContext(PCollectionView<Integer> view) {
      this.view = view;
    }

    @Override
    public TenantAwareValueCoder<Accumulator> getAccumulatorCoder(
        CoderRegistry registry, Coder<Integer> inputCoder) {
      return TenantAwareValueCoder.of(TestCombineFn.Accumulator.getCoder());
    }

    @Override
    public TenantAwareValue<TestCombineFn.Accumulator> createAccumulator(Context c) {
      Integer sideInputValue = c.sideInput(view);
      return TenantAwareValue.of(
          TenantAwareValue.NULL_TENANT,
          new TestCombineFn.Accumulator(sideInputValue.toString(), ""));
    }

    @Override
    public TenantAwareValue<TestCombineFn.Accumulator> addInput(
        TenantAwareValue<TestCombineFn.Accumulator> accumulator,
        TenantAwareValue<Integer> value,
        Context c) {
      try {
        assertThat(
            "Not expecting view contents to change",
            accumulator.getValue().seed,
            Matchers.equalTo(Integer.toString(c.sideInput(view))));
        return TenantAwareValue.of(
            value.getTenantId(),
            new TestCombineFn.Accumulator(
                accumulator.getValue().seed,
                accumulator.getValue().value + String.valueOf(value.getValue())));
      } finally {
        accumulator.getValue().value = "cleared in addInput";
      }
    }

    @Override
    public TenantAwareValue<TestCombineFn.Accumulator> mergeAccumulators(
        Iterable<TenantAwareValue<TestCombineFn.Accumulator>> accumulators, Context c) {
      String sideInputValue = c.sideInput(view).toString();
      StringBuilder all = new StringBuilder();
      String tenantId = TenantAwareValue.NULL_TENANT;

      for (TenantAwareValue<TestCombineFn.Accumulator> accumulator : accumulators) {
        assertThat(
            "Accumulators should all have the same Side Input Value",
            createAccumulator(c).getValue().seed,
            Matchers.equalTo(sideInputValue));
        all.append(accumulator.getValue().value);
        tenantId = accumulator.getTenantId();

        accumulator.getValue().value = "cleared in mergeAccumulators";
      }
      return TenantAwareValue.of(
          tenantId, new TestCombineFn.Accumulator(sideInputValue, all.toString()));
    }

    @Override
    public TenantAwareValue<String> extractOutput(
        TenantAwareValue<TestCombineFn.Accumulator> accumulator, Context c) {
      assertThat(accumulator.getValue().seed, Matchers.startsWith(c.sideInput(view).toString()));
      char[] chars = accumulator.getValue().value.toCharArray();
      Arrays.sort(chars);
      return TenantAwareValue.of(
          accumulator.getTenantId(), accumulator.getValue().seed + ":" + new String(chars));
    }
  }

  /** Another example AccumulatingCombineFn. */
  public static class TestCounter
      extends Combine.AccumulatingCombineFn<Integer, TestCounter.Counter, Iterable<Long>> {

    /** An accumulator that observes its merges and outputs. */
    public class Counter
        implements Combine.AccumulatingCombineFn.Accumulator<Integer, Counter, Iterable<Long>>,
            Serializable {

      public long sum = 0;
      public long inputs = 0;
      public long merges = 0;
      public long outputs = 0;
      String tenantId = TenantAwareValue.NULL_TENANT;

      public Counter(long sum, long inputs, long merges, long outputs) {
        this.sum = sum;
        this.inputs = inputs;
        this.merges = merges;
        this.outputs = outputs;
      }

      @Override
      public void addInput(TenantAwareValue<Integer> element) {
        checkState(merges == 0);
        checkState(outputs == 0);

        tenantId = element.getTenantId();
        inputs++;
        sum += element.getValue();
      }

      @Override
      public void mergeAccumulator(Counter accumulator) {
        checkState(outputs == 0);
        assertEquals(0, accumulator.outputs);

        merges += accumulator.merges + 1;
        inputs += accumulator.inputs;
        sum += accumulator.sum;
      }

      @Override
      public TenantAwareValue<Iterable<Long>> extractOutput() {
        checkState(outputs == 0);

        return TenantAwareValue.of(tenantId, Arrays.asList(sum, inputs, merges, outputs));
      }

      @Override
      public int hashCode() {
        return (int) (sum * 17 + inputs * 31 + merges * 43 + outputs * 181);
      }

      @Override
      public boolean equals(Object otherObj) {
        if (otherObj instanceof Counter) {
          Counter other = (Counter) otherObj;
          return (sum == other.sum
              && inputs == other.inputs
              && merges == other.merges
              && outputs == other.outputs);
        }
        return false;
      }

      @Override
      public String toString() {
        return sum + ":" + inputs + ":" + merges + ":" + outputs;
      }
    }

    @Override
    public TenantAwareValue<Counter> createAccumulator() {
      return TenantAwareValue.of(TenantAwareValue.NULL_TENANT, new Counter(0, 0, 0, 0));
    }

    @Override
    public TenantAwareValueCoder<Counter> getAccumulatorCoder(
        CoderRegistry registry, Coder<Integer> inputCoder) {
      // This is a *very* inefficient encoding to send over the wire, but suffices
      // for tests.
      return TenantAwareValueCoder.of(SerializableCoder.of(Counter.class));
    }
  }

  private static <T> PCollection<T> copy(PCollection<T> pc, final int n) {
    return pc.apply(
        ParDo.of(
            new DoFn<T, T>() {
              @ProcessElement
              public void processElement(ProcessContext c) throws Exception {
                for (int i = 0; i < n; i++) {
                  c.output(c.element());
                }
              }
            }));
  }
}
