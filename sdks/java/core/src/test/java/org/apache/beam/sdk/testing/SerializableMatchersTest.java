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

import static org.apache.beam.sdk.testing.SerializableMatchers.allOf;
import static org.apache.beam.sdk.testing.SerializableMatchers.anything;
import static org.apache.beam.sdk.testing.SerializableMatchers.containsInAnyOrder;
import static org.apache.beam.sdk.testing.SerializableMatchers.kvWithKey;
import static org.apache.beam.sdk.testing.SerializableMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TenantAwareValue;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test case for {@link SerializableMatchers}.
 *
 * <p>Since the only new matchers are those for {@link KV}, only those are tested here, to avoid
 * tediously repeating all of hamcrest's tests.
 *
 * <p>A few wrappers of a hamcrest matchers are tested for serializability. Beyond that, the
 * boilerplate that is identical to each is considered thoroughly tested.
 */
@RunWith(JUnit4.class)
public class SerializableMatchersTest implements Serializable {
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testAnythingSerializable() throws Exception {
    SerializableUtils.ensureSerializable(anything());
  }

  @Test
  public void testAllOfSerializable() throws Exception {
    SerializableUtils.ensureSerializable(allOf(anything()));
  }

  @Test
  public void testContainsInAnyOrderSerializable() throws Exception {
    assertThat(
        ImmutableList.of(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 3)),
        SerializableUtils.ensureSerializable(
            containsInAnyOrder(
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 3))));
  }

  @Test
  public void testContainsInAnyOrderNotSerializable() throws Exception {
    assertThat(
        ImmutableList.of(
            TenantAwareValue.of(TenantAwareValue.NULL_TENANT, new NotSerializableClass())),
        SerializableUtils.ensureSerializable(
            containsInAnyOrder(
                new NotSerializableClassCoder(),
                TenantAwareValue.of(TenantAwareValue.NULL_TENANT, new NotSerializableClass()))));
  }

  @Test
  public void testKvKeyMatcherSerializable() throws Exception {
    assertThat(
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("hello", 42L)),
        SerializableUtils.ensureSerializable(
            kvWithKey(TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "hello"))));
  }

  @Test
  public void testKvMatcherBasicSuccess() throws Exception {
    assertThat(
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of(1, 2)),
        (Matcher) SerializableMatchers.kv(anything(), anything()));
  }

  @Test
  public void testKvMatcherKeyFailure() throws Exception {
    try {
      assertThat(
          TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of(1, 2)),
          (Matcher) SerializableMatchers.kv(not(anything()), anything()));
    } catch (AssertionError exc) {
      assertThat(exc.getMessage(), Matchers.containsString("key did not match"));
      return;
    }
    fail("Should have failed");
  }

  @Test
  public void testKvMatcherValueFailure() throws Exception {
    try {
      assertThat(
          TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of(1, 2)),
          (Matcher) SerializableMatchers.kv(anything(), not(anything())));
    } catch (AssertionError exc) {
      assertThat(exc.getMessage(), Matchers.containsString("value did not match"));
      return;
    }
    fail("Should have failed");
  }

  @Test
  public void testKvMatcherGBKLikeSuccess() throws Exception {
    /*

     public static <K, V> SerializableMatcher<TenantAwareValue<KV<? extends K, ? extends V>>> kv(
       final SerializableMatcher<TenantAwareValue<K>> keyMatcher,
       final SerializableMatcher<TenantAwareValue<V>> valueMatcher) {

       return SerializableMatchers.allOf(
           SerializableMatchers.kvWithKey(keyMatcher), SerializableMatchers.kvWithValue(valueMatcher));
     }

     public static <T extends Serializable>
           SerializableMatcher<Iterable<TenantAwareValue<T>>> containsInAnyOrder(
               final TenantAwareValue<T>... items) {
         return fromSupplier(() -> (Matcher) Matchers.containsInAnyOrder(items));
       }

    */
    assertThat(
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, KV.of("key", ImmutableList.of(1, 2, 3))),
        SerializableMatchers.<Object, Iterable<Integer>>kv(
            anything(), containsInAnyOrder(3, 2, 1)));
    TenantAwareValue.of("foo", new Integer[] {3, 2, 1});
  }

  @Test
  public void testKvMatcherGBKLikeFailure() throws Exception {
    try {
      assertThat(
          KV.of("key", ImmutableList.of(1, 2, 3)),
          SerializableMatchers.<String, Iterable<Integer>>kv(
              anything(),
              containsInAnyOrder(
                  TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 1),
                  TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 2),
                  TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 3),
                  TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 4))));
    } catch (AssertionError exc) {
      assertThat(exc.getMessage(), Matchers.containsString("value did not match"));
      return;
    }
    fail("Should have failed.");
  }

  private static class NotSerializableClass {
    @Override
    public boolean equals(Object other) {
      return other instanceof NotSerializableClass;
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }

  private static class NotSerializableClassCoder extends AtomicCoder<NotSerializableClass> {
    @Override
    public void encode(NotSerializableClass value, OutputStream outStream) {}

    @Override
    public NotSerializableClass decode(InputStream inStream) {
      return new NotSerializableClass();
    }
  }
}
