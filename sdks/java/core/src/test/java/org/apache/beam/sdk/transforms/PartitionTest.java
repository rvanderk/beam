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

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TenantAwareValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Partition}. */
@RunWith(JUnit4.class)
public class PartitionTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  static class ModFn implements PartitionFn<Integer> {
    @Override
    public int partitionFor(Integer elem, int numPartitions) {
      return elem % numPartitions;
    }
  }

  static class IdentityFn implements PartitionFn<Integer> {
    @Override
    public int partitionFor(Integer elem, int numPartitions) {
      return elem;
    }
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testEvenOddPartition() {

    PCollectionList<Integer> outputs =
        pipeline
            .apply(
                Create.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 591),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 11789),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1257),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 24578),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 24799),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 307)))
            .apply(Partition.of(2, new ModFn()));
    assertTrue(outputs.size() == 2);
    PAssert.that(outputs.get(0))
        .containsInAnyOrder(TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 24578));
    PAssert.that(outputs.get(1))
        .containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 591),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 11789),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1257),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 24799),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 307));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testModPartition() {

    PCollectionList<Integer> outputs =
        pipeline
            .apply(
                Create.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 5)))
            .apply(Partition.of(3, new ModFn()));
    assertTrue(outputs.size() == 3);
    PAssert.that(outputs.get(0)).empty();
    PAssert.that(outputs.get(1))
        .containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4));
    PAssert.that(outputs.get(2))
        .containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 5));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOutOfBoundsPartitions() {

    pipeline
        .apply(Create.of(TenantAwareValue.of(TenantAwareValue.NULL_TENANT, -1)))
        .apply(Partition.of(5, new IdentityFn()));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Partition function returned out of bounds index: -1 not in [0..5)");
    pipeline.run();
  }

  @Test
  public void testZeroNumPartitions() {

    PCollection<Integer> input =
        pipeline.apply(Create.of(TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 591)));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("numPartitions must be > 0");
    input.apply(Partition.of(0, new IdentityFn()));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDroppedPartition() {

    // Compute the set of integers either 1 or 2 mod 3, the hard way.
    PCollectionList<Integer> outputs =
        pipeline
            .apply(
                Create.of(
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 3),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 5),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 6),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 7),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 8),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 9),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 10),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 11),
                    TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 12)))
            .apply(Partition.of(3, new ModFn()));

    List<PCollection<Integer>> outputsList = new ArrayList<>(outputs.getAll());
    outputsList.remove(0);
    outputs = PCollectionList.of(outputsList);
    assertTrue(outputs.size() == 2);

    PCollection<Integer> output = outputs.apply(Flatten.pCollections());
    PAssert.that(output)
        .containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 5),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 7),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 8),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 10),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 11));
    pipeline.run();
  }

  @Test
  public void testPartitionGetName() {
    assertEquals("Partition", Partition.of(3, new ModFn()).getName());
  }

  @Test
  public void testDisplayData() {
    Partition<?> partition = Partition.of(123, new IdentityFn());
    DisplayData displayData = DisplayData.from(partition);

    assertThat(displayData, hasDisplayItem("numPartitions", 123));
    assertThat(displayData, hasDisplayItem("partitionFn", IdentityFn.class));
  }
}
