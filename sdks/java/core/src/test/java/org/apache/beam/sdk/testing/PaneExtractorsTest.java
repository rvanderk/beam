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
package org.apache.beam.sdk.testing;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.values.TenantAwareValue;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PaneExtractors}. */
@RunWith(JUnit4.class)
public class PaneExtractorsTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void onlyPaneNoFiring() {
    SerializableFunction<Iterable<ValueInSingleWindow<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.onlyPane(PAssert.PAssertionSite.capture(""));
    TenantAwareValue<Iterable<ValueInSingleWindow<Integer>>> noFiring =
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT,
            ImmutableList.of(
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 9),
                    BoundedWindow.TIMESTAMP_MIN_VALUE,
                    GlobalWindow.INSTANCE,
                    PaneInfo.NO_FIRING),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 19),
                    BoundedWindow.TIMESTAMP_MIN_VALUE,
                    GlobalWindow.INSTANCE,
                    PaneInfo.NO_FIRING)));
    assertThat(
        extractor.apply(noFiring).getValue(),
        containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 9),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 19)));
  }

  @Test
  public void onlyPaneOnlyOneFiring() {
    SerializableFunction<Iterable<ValueInSingleWindow<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.onlyPane(PAssert.PAssertionSite.capture(""));
    TenantAwareValue<Iterable<ValueInSingleWindow<Integer>>> onlyFiring =
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT,
            ImmutableList.of(
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
                    BoundedWindow.TIMESTAMP_MIN_VALUE,
                    GlobalWindow.INSTANCE,
                    PaneInfo.NO_FIRING),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
                    BoundedWindow.TIMESTAMP_MIN_VALUE,
                    GlobalWindow.INSTANCE,
                    PaneInfo.NO_FIRING)));

    assertThat(
        extractor.apply(onlyFiring).getValue(),
        containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1)));
  }

  @Test
  public void onlyPaneMultiplePanesFails() {
    SerializableFunction<Iterable<ValueInSingleWindow<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.onlyPane(PAssert.PAssertionSite.capture(""));
    TenantAwareValue<Iterable<ValueInSingleWindow<Integer>>> multipleFiring =
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT,
            ImmutableList.of(
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(true, false, Timing.EARLY)),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L)),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(false, false, Timing.LATE, 2L, 1L))));

    thrown.expectMessage("trigger that fires at most once");
    extractor.apply(multipleFiring).getValue();
  }

  @Test
  public void onTimePane() {
    SerializableFunction<Iterable<ValueInSingleWindow<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.onTimePane();
    TenantAwareValue<Iterable<ValueInSingleWindow<Integer>>> onlyOnTime =
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT,
            ImmutableList.of(
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L)),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L))));

    assertThat(
        extractor.apply(onlyOnTime).getValue(),
        containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4)));
  }

  @Test
  public void onTimePaneOnlyEarlyAndLate() {
    SerializableFunction<Iterable<ValueInSingleWindow<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.onTimePane();
    TenantAwareValue<Iterable<ValueInSingleWindow<Integer>>> onlyOnTime =
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT,
            ImmutableList.of(
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 8),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(false, false, Timing.LATE, 2L, 1L)),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L)),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L)),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(true, false, Timing.EARLY))));

    assertThat(
        extractor.apply(onlyOnTime).getValue(),
        containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4)));
  }

  @Test
  public void finalPane() {
    SerializableFunction<Iterable<ValueInSingleWindow<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.finalPane();
    TenantAwareValue<Iterable<ValueInSingleWindow<Integer>>> onlyOnTime =
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT,
            ImmutableList.of(
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 8),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(false, true, Timing.LATE, 2L, 1L)),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L)),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(true, false, Timing.EARLY))));

    assertThat(
        extractor.apply(onlyOnTime).getValue(),
        containsInAnyOrder(TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 8)));
  }

  @Test
  public void finalPaneNoExplicitFinalEmpty() {
    SerializableFunction<Iterable<ValueInSingleWindow<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.finalPane();
    TenantAwareValue<Iterable<ValueInSingleWindow<Integer>>> onlyOnTime =
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT,
            ImmutableList.of(
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 8),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(false, false, Timing.LATE, 2L, 1L)),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L)),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(true, false, Timing.EARLY))));

    assertThat(extractor.apply(onlyOnTime).getValue(), emptyIterable());
  }

  @Test
  public void nonLatePanesSingleOnTime() {
    SerializableFunction<Iterable<ValueInSingleWindow<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.nonLatePanes();
    TenantAwareValue<Iterable<ValueInSingleWindow<Integer>>> onlyOnTime =
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT,
            ImmutableList.of(
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 8),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.ON_TIME_AND_ONLY_FIRING),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.ON_TIME_AND_ONLY_FIRING),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.ON_TIME_AND_ONLY_FIRING)));

    assertThat(
        extractor.apply(onlyOnTime).getValue(),
        containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 8)));
  }

  @Test
  public void nonLatePanesSingleEarly() {
    SerializableFunction<Iterable<ValueInSingleWindow<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.nonLatePanes();
    TenantAwareValue<Iterable<ValueInSingleWindow<Integer>>> onlyOnTime =
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT,
            ImmutableList.of(
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 8),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(true, false, Timing.EARLY)),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(true, false, Timing.EARLY))));

    assertThat(
        extractor.apply(onlyOnTime).getValue(),
        containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 8)));
  }

  @Test
  public void allPanesSingleLate() {
    SerializableFunction<Iterable<ValueInSingleWindow<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.nonLatePanes();
    TenantAwareValue<Iterable<ValueInSingleWindow<Integer>>> onlyOnTime =
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT,
            ImmutableList.of(
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 8),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(false, false, Timing.LATE, 0L, 0L))));

    assertThat(extractor.apply(onlyOnTime).getValue(), emptyIterable());
  }

  @Test
  public void nonLatePanesMultiplePanes() {
    SerializableFunction<Iterable<ValueInSingleWindow<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.nonLatePanes();
    TenantAwareValue<Iterable<ValueInSingleWindow<Integer>>> onlyOnTime =
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT,
            ImmutableList.of(
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 8),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(false, false, Timing.LATE, 2L, 1L)),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 7),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.NO_FIRING),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L)),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(true, false, Timing.EARLY))));

    assertThat(
        extractor.apply(onlyOnTime).getValue(),
        containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 7)));
  }

  @Test
  public void allPanesSinglePane() {
    SerializableFunction<Iterable<ValueInSingleWindow<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.allPanes();
    TenantAwareValue<Iterable<ValueInSingleWindow<Integer>>> onlyOnTime =
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT,
            ImmutableList.of(
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 8),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.ON_TIME_AND_ONLY_FIRING),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.ON_TIME_AND_ONLY_FIRING),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.ON_TIME_AND_ONLY_FIRING)));

    assertThat(
        extractor.apply(onlyOnTime).getValue(),
        containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 8)));
  }

  @Test
  public void allPanesMultiplePanes() {
    SerializableFunction<Iterable<ValueInSingleWindow<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.allPanes();
    TenantAwareValue<Iterable<ValueInSingleWindow<Integer>>> onlyOnTime =
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT,
            ImmutableList.of(
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 8),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(false, false, Timing.LATE, 2L, 1L)),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L)),
                ValueInSingleWindow.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
                    new Instant(0L),
                    GlobalWindow.INSTANCE,
                    PaneInfo.createPane(true, false, Timing.EARLY))));

    assertThat(
        extractor.apply(onlyOnTime).getValue(),
        containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 8),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1)));
  }

  @Test
  public void allPanesEmpty() {
    SerializableFunction<Iterable<ValueInSingleWindow<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.allPanes();
    TenantAwareValue<Iterable<ValueInSingleWindow<Integer>>> noPanes =
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, ImmutableList.of());

    assertThat(extractor.apply(noPanes).getValue(), emptyIterable());
  }
}
