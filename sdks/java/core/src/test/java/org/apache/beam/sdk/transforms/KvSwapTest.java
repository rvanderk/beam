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

import java.util.Arrays;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TenantAwareValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for KvSwap transform. */
@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class KvSwapTest {
  private static final TenantAwareValue<KV<String, Integer>>[] TABLE;

  static {
    TABLE =
        new TenantAwareValue[] {
          TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("one", 1)),
          TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("two", 2)),
          TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("three", 3)),
          TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("four", 4)),
          TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("dup", 4)),
          TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("dup", 5)),
          TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("null", null)),
        };
  }

  private static final TenantAwareValue<KV<String, Integer>>[] EMPTY_TABLE =
      new TenantAwareValue[] {};

  @Rule public final TestPipeline p = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testKvSwap() {
    PCollection<KV<String, Integer>> input =
        p.apply(
            Create.of(Arrays.asList(TABLE))
                .withCoder(
                    KvCoder.of(
                        StringUtf8Coder.of(), NullableCoder.of(BigEndianIntegerCoder.of()))));

    PCollection<KV<Integer, String>> output = input.apply(KvSwap.create());

    PAssert.that(output)
        .containsInAnyOrder(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of(1, "one")),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of(2, "two")),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of(3, "three")),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of(4, "four")),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of(4, "dup")),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of(5, "dup")),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of((Integer) null, "null")));
    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testKvSwapEmpty() {
    PCollection<KV<String, Integer>> input =
        p.apply(
            Create.of(Arrays.asList(EMPTY_TABLE))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PCollection<KV<Integer, String>> output = input.apply(KvSwap.create());

    PAssert.that(output).empty();
    p.run();
  }
}
