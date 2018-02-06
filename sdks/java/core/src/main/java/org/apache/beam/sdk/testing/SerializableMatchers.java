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

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TenantAwareValue;
import org.apache.beam.sdk.values.TenantAwareValue.TenantAwareValueCoder;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.hamcrest.core.IsAnything;
import org.hamcrest.core.StringContains;
import org.hamcrest.core.StringEndsWith;
import org.hamcrest.core.StringStartsWith;
import org.hamcrest.number.IsCloseTo;
import org.hamcrest.number.OrderingComparison;

/**
 * Static class for building and using {@link SerializableMatcher} instances.
 *
 * <p>Most matchers are wrappers for hamcrest's {@link Matchers}. Please be familiar with the
 * documentation there. Values retained by a {@link SerializableMatcher} are required to be
 * serializable, either via Java serialization or via a provided {@link Coder}.
 *
 * <p>The following matchers are novel to Apache Beam:
 *
 * <ul>
 *   <li>{@link #kvWithKey} for matching just the key of a {@link KV}.
 *   <li>{@link #kvWithValue} for matching just the value of a {@link KV}.
 *   <li>{@link #kv} for matching the key and value of a {@link KV}.
 * </ul>
 *
 * <p>For example, to match a group from {@link org.apache.beam.sdk.transforms.GroupByKey}, which
 * has type {@code KV<K, Iterable<V>>} for some {@code K} and {@code V} and where the order of the
 * iterable is undefined, use a matcher like {@code kv(equalTo("some key"), containsInAnyOrder(1, 2,
 * 3))}.
 */
class SerializableMatchers implements Serializable {

  // Serializable only because of capture by anonymous inner classes
  private SerializableMatchers() {} // not instantiable

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#allOf(Iterable)}. */
  public static <T> SerializableMatcher<TenantAwareValue<T>> allOf(
      Iterable<SerializableMatcher<TenantAwareValue<T>>> serializableMatchers) {

    @SuppressWarnings({"rawtypes", "unchecked"}) // safe covariant cast
    final Iterable<Matcher<? super TenantAwareValue<T>>> matchers = (Iterable) serializableMatchers;

    return fromSupplier(() -> Matchers.allOf(matchers));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#allOf(Matcher[])}. */
  @SafeVarargs
  public static <T> SerializableMatcher<TenantAwareValue<T>> allOf(
      final SerializableMatcher<TenantAwareValue<T>>... matchers) {
    return fromSupplier(() -> Matchers.allOf(matchers));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#anyOf(Iterable)}. */
  public static <T> SerializableMatcher<TenantAwareValue<T>> anyOf(
      Iterable<SerializableMatcher<TenantAwareValue<T>>> serializableMatchers) {

    @SuppressWarnings({"rawtypes", "unchecked"}) // safe covariant cast
    final Iterable<Matcher<? super TenantAwareValue<T>>> matchers = (Iterable) serializableMatchers;

    return fromSupplier(() -> Matchers.anyOf(matchers));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#anyOf(Matcher[])}. */
  @SafeVarargs
  public static <T> SerializableMatcher<TenantAwareValue<T>> anyOf(
      final SerializableMatcher<TenantAwareValue<T>>... matchers) {
    return fromSupplier(() -> Matchers.anyOf(matchers));
  }

  //  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#anything()}. */
  //  public static <T> SerializableMatcher<TenantAwareValue<T>> anything() {
  //    return fromSupplier(() -> new IsAnything<TenantAwareValue<T>>());
  //  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#anything()}. */
  public static <T> SerializableMatcher<T> anything() {
    return fromSupplier(() -> new IsAnything<T>());
  }
  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayContaining(Object[])}.
   */
  @SafeVarargs
  public static <T extends Serializable> SerializableMatcher<TenantAwareValue<T>[]> arrayContaining(
      final TenantAwareValue<T>... items) {
    return fromSupplier(() -> Matchers.arrayContaining(items));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayContaining(Object[])}.
   *
   * <p>The items of type {@code T} will be serialized using the provided {@link Coder}. They are
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<TenantAwareValue<T>[]> arrayContaining(
      Coder<T> coder, TenantAwareValue<T>... items) {

    final SerializableSupplier<TenantAwareValue<T>[]> itemsSupplier =
        new SerializableArrayViaCoder<>(TenantAwareValueCoder.of(coder), items);

    return fromSupplier(() -> Matchers.arrayContaining(itemsSupplier.get()));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayContaining(Matcher[])}.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<TenantAwareValue<T>[]> arrayContaining(
      final SerializableMatcher<TenantAwareValue<T>>... matchers) {
    return fromSupplier(() -> Matchers.arrayContaining(matchers));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayContaining(List)}.
   */
  public static <T> SerializableMatcher<TenantAwareValue<T>[]> arrayContaining(
      List<SerializableMatcher<TenantAwareValue<T>>> serializableMatchers) {

    @SuppressWarnings({"rawtypes", "unchecked"}) // safe covariant cast
    final List<Matcher<? super TenantAwareValue<T>>> matchers = (List) serializableMatchers;

    return fromSupplier(() -> Matchers.arrayContaining(matchers));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayContainingInAnyOrder(Object[])}.
   */
  @SafeVarargs
  public static <T extends Serializable>
      SerializableMatcher<TenantAwareValue<T>[]> arrayContainingInAnyOrder(
          final TenantAwareValue<T>... items) {

    return fromSupplier(() -> Matchers.arrayContainingInAnyOrder(items));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayContainingInAnyOrder(Object[])}.
   *
   * <p>The items of type {@code T} will be serialized using the provided {@link Coder}. They are
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<TenantAwareValue<T>[]> arrayContainingInAnyOrder(
      Coder<T> coder, TenantAwareValue<T>... items) {

    final SerializableSupplier<TenantAwareValue<T>[]> itemsSupplier =
        new SerializableArrayViaCoder<>(TenantAwareValueCoder.of(coder), items);

    return fromSupplier(() -> Matchers.arrayContaining(itemsSupplier.get()));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayContainingInAnyOrder(Matcher[])}.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<TenantAwareValue<T>[]> arrayContainingInAnyOrder(
      final SerializableMatcher<TenantAwareValue<T>>... matchers) {
    return fromSupplier(() -> Matchers.arrayContainingInAnyOrder(matchers));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayContainingInAnyOrder(Collection)}.
   */
  public static <T> SerializableMatcher<TenantAwareValue<T>[]> arrayContainingInAnyOrder(
      Collection<SerializableMatcher<TenantAwareValue<T>>> serializableMatchers) {

    @SuppressWarnings({"rawtypes", "unchecked"}) // safe covariant cast
    final Collection<Matcher<? super TenantAwareValue<T>>> matchers =
        (Collection) serializableMatchers;

    return fromSupplier(() -> Matchers.arrayContainingInAnyOrder(matchers));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#arrayWithSize(int)}.
   */
  public static <T> SerializableMatcher<TenantAwareValue<T>[]> arrayWithSize(final int size) {
    return fromSupplier(() -> Matchers.arrayWithSize(size));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#arrayWithSize(Matcher)}.
   */
  public static <T> SerializableMatcher<TenantAwareValue<T>[]> arrayWithSize(
      final SerializableMatcher<? super Integer> sizeMatcher) {
    return fromSupplier(() -> Matchers.arrayWithSize(sizeMatcher));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#closeTo(double,double)}.
   */
  public static SerializableMatcher<TenantAwareValue<Double>> closeTo(
      final TenantAwareValue<Double> target, final double error) {
    return fromSupplier(
        () ->
            new TypeSafeMatcher<TenantAwareValue<Double>>() {
              IsCloseTo isCloseTo = new IsCloseTo(target.getValue(), error);

              @Override
              public void describeTo(Description description) {
                isCloseTo.describeTo(description);
              }

              @Override
              protected boolean matchesSafely(TenantAwareValue<Double> doubleTenantAwareValue) {
                return doubleTenantAwareValue.getTenantId().equals(target.getTenantId())
                    && isCloseTo.matchesSafely(doubleTenantAwareValue.getValue());
              }
            });
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#contains(Object[])}.
   */
  @SafeVarargs
  public static <T extends Serializable>
      SerializableMatcher<Iterable<TenantAwareValue<T>>> contains(
          final TenantAwareValue<T>... items) {
    return fromSupplier(() -> (Matcher) Matchers.contains(items));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#contains(Object[])}.
   *
   * <p>The items of type {@code T} will be serialized using the provided {@link Coder}. They are
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<Iterable<TenantAwareValue<T>>> contains(
      Coder<T> coder, TenantAwareValue<T>... items) {

    final SerializableSupplier<TenantAwareValue<T>[]> itemsSupplier =
        new SerializableArrayViaCoder<>(TenantAwareValueCoder.of(coder), items);

    return fromSupplier(() -> (Matcher) Matchers.containsInAnyOrder(itemsSupplier.get()));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#contains(Matcher[])}.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<Iterable<TenantAwareValue<T>>> contains(
      final SerializableMatcher<TenantAwareValue<T>>... matchers) {
    return fromSupplier(() -> (Matcher) Matchers.contains(matchers));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#contains(List)}. */
  public static <T extends Serializable>
      SerializableMatcher<Iterable<TenantAwareValue<T>>> contains(
          List<SerializableMatcher<TenantAwareValue<T>>> serializableMatchers) {

    @SuppressWarnings({"rawtypes", "unchecked"}) // safe covariant cast
    final List<Matcher<? super TenantAwareValue<T>>> matchers = (List) serializableMatchers;

    return fromSupplier(() -> (Matcher) Matchers.contains(matchers));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#containsInAnyOrder(Object[])}.
   */
  @SafeVarargs
  public static <T extends Serializable>
      SerializableMatcher<Iterable<TenantAwareValue<T>>> containsInAnyOrder(
          final TenantAwareValue<T>... items) {
    return fromSupplier(() -> (Matcher) Matchers.containsInAnyOrder(items));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#containsInAnyOrder(Object[])}.
   */
  @SafeVarargs
  public static <T extends Serializable> SerializableMatcher<Iterable<T>> containsInAnyOrder(
      final T... items) {
    return fromSupplier(() -> (Matcher) Matchers.containsInAnyOrder(items));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#containsInAnyOrder(Object[])}.
   *
   * <p>The items of type {@code T} will be serialized using the provided {@link Coder}. It is
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<Iterable<TenantAwareValue<T>>> containsInAnyOrder(
      Coder<T> coder, TenantAwareValue<T>... items) {

    final SerializableSupplier<TenantAwareValue<T>[]> itemsSupplier =
        new SerializableArrayViaCoder<>(TenantAwareValueCoder.of(coder), items);

    return fromSupplier(() -> (Matcher) Matchers.containsInAnyOrder(itemsSupplier.get()));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#containsInAnyOrder(Matcher[])}.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<Iterable<TenantAwareValue<T>>> containsInAnyOrder(
      final SerializableMatcher<TenantAwareValue<T>>... matchers) {

    return fromSupplier(() -> (Matcher) Matchers.containsInAnyOrder(matchers));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#containsInAnyOrder(Collection)}.
   */
  public static <T> SerializableMatcher<Iterable<TenantAwareValue<T>>> containsInAnyOrder(
      Collection<SerializableMatcher<TenantAwareValue<T>>> serializableMatchers) {

    @SuppressWarnings({"rawtypes", "unchecked"}) // safe covariant cast
    final Collection<Matcher<? super TenantAwareValue<T>>> matchers =
        (Collection) serializableMatchers;

    return fromSupplier(() -> (Matcher) Matchers.containsInAnyOrder(matchers));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#containsString}. */
  public static SerializableMatcher<TenantAwareValue<String>> containsString(
      final TenantAwareValue<String> substring) {
    return fromSupplier(
        () ->
            new TypeSafeMatcher<TenantAwareValue<String>>() {
              StringContains stringContains = new StringContains(substring.getValue());

              @Override
              public void describeTo(Description description) {
                stringContains.describeTo(description);
              }

              @Override
              protected boolean matchesSafely(TenantAwareValue<String> stringTenantAwareValue) {
                return stringTenantAwareValue.getTenantId().equals(substring.getTenantId())
                    && stringContains.matchesSafely(stringTenantAwareValue.getValue());
              }
            });
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#empty()}. */
  public static <T> SerializableMatcher<Collection<TenantAwareValue<T>>> empty() {
    return fromSupplier(() -> (Matcher) Matchers.empty());
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#emptyArray()}. */
  public static <T> SerializableMatcher<TenantAwareValue<T>[]> emptyArray() {
    return fromSupplier(() -> (Matcher) Matchers.emptyArray());
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#emptyIterable()}. */
  public static <T> SerializableMatcher<Iterable<TenantAwareValue<T>>> emptyIterable() {
    return fromSupplier(() -> (Matcher) Matchers.emptyIterable());
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#endsWith}. */
  public static SerializableMatcher<TenantAwareValue<String>> endsWith(
      final TenantAwareValue<String> substring) {
    return fromSupplier(
        () ->
            new TypeSafeMatcher<TenantAwareValue<String>>() {
              StringEndsWith stringEndsWith = new StringEndsWith(substring.getValue());

              @Override
              public void describeTo(Description description) {
                stringEndsWith.describeTo(description);
              }

              @Override
              protected boolean matchesSafely(TenantAwareValue<String> stringTenantAwareValue) {
                return stringTenantAwareValue.getTenantId().equals(substring.getTenantId())
                    && stringEndsWith.matchesSafely(stringTenantAwareValue.getValue());
              }
            });
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#equalTo(Object)}. */
  public static <T extends Serializable> SerializableMatcher<TenantAwareValue<T>> equalTo(
      final TenantAwareValue<T> expected) {
    return fromSupplier(() -> Matchers.equalTo(expected));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#equalTo(Object)}.
   *
   * <p>The expected value of type {@code T} will be serialized using the provided {@link Coder}. It
   * is explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <T> SerializableMatcher<TenantAwareValue<T>> equalTo(
      Coder<T> coder, TenantAwareValue<T> expected) {

    final SerializableSupplier<TenantAwareValue<T>> expectedSupplier =
        new SerializableViaCoder<>(TenantAwareValueCoder.of(coder), expected);

    return fromSupplier(() -> Matchers.equalTo(expectedSupplier.get()));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#greaterThan(Comparable)}.
   */
  public static <T extends Comparable<T> & Serializable>
      SerializableMatcher<TenantAwareValue<T>> greaterThan(final TenantAwareValue<T> target) {
    return fromSupplier(
        () ->
            new TypeSafeMatcher<TenantAwareValue<T>>() {
              Matcher<T> greaterThan = OrderingComparison.greaterThan(target.getValue());

              @Override
              public void describeTo(Description description) {
                greaterThan.describeTo(description);
              }

              @Override
              protected boolean matchesSafely(TenantAwareValue<T> tTenantAwareValue) {
                return tTenantAwareValue.getTenantId().equals(target.getTenantId())
                    && greaterThan.matches(target.getValue());
              }
            });
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#greaterThan(Comparable)}.
   *
   * <p>The target value of type {@code T} will be serialized using the provided {@link Coder}. It
   * is explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <T extends Comparable<T> & Serializable>
      SerializableMatcher<TenantAwareValue<T>> greaterThan(
          final Coder<T> coder, TenantAwareValue<T> target) {
    final SerializableSupplier<TenantAwareValue<T>> targetSupplier =
        new SerializableViaCoder<>(TenantAwareValueCoder.of(coder), target);

    return fromSupplier(
        () ->
            new TypeSafeMatcher<TenantAwareValue<T>>() {
              Matcher<T> greaterThan =
                  OrderingComparison.greaterThan(targetSupplier.get().getValue());

              @Override
              public void describeTo(Description description) {
                greaterThan.describeTo(description);
              }

              @Override
              protected boolean matchesSafely(TenantAwareValue<T> tTenantAwareValue) {
                return tTenantAwareValue.getTenantId().equals(targetSupplier.get().getTenantId())
                    && greaterThan.matches(targetSupplier.get().getValue());
              }
            });
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#greaterThanOrEqualTo(Comparable)}.
   */
  public static <T extends Comparable<T>>
      SerializableMatcher<TenantAwareValue<T>> greaterThanOrEqualTo(
          final TenantAwareValue<T> target) {
    return fromSupplier(
        () ->
            new TypeSafeMatcher<TenantAwareValue<T>>() {
              Matcher<T> greaterThanOrEqual =
                  OrderingComparison.greaterThanOrEqualTo(target.getValue());

              @Override
              public void describeTo(Description description) {
                greaterThanOrEqual.describeTo(description);
              }

              @Override
              protected boolean matchesSafely(TenantAwareValue<T> tTenantAwareValue) {
                return tTenantAwareValue.getTenantId().equals(target.getTenantId())
                    && greaterThanOrEqual.matches(target.getValue());
              }
            });
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#greaterThanOrEqualTo(Comparable)}.
   *
   * <p>The target value of type {@code T} will be serialized using the provided {@link Coder}. It
   * is explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <T extends Comparable<T> & Serializable>
      SerializableMatcher<TenantAwareValue<T>> greaterThanOrEqualTo(
          final Coder<T> coder, TenantAwareValue<T> target) {
    final SerializableSupplier<TenantAwareValue<T>> targetSupplier =
        new SerializableViaCoder<>(TenantAwareValueCoder.of(coder), target);

    return fromSupplier(
        () ->
            new TypeSafeMatcher<TenantAwareValue<T>>() {
              Matcher<T> greaterThanOrEqual =
                  OrderingComparison.greaterThanOrEqualTo(targetSupplier.get().getValue());

              @Override
              public void describeTo(Description description) {
                greaterThanOrEqual.describeTo(description);
              }

              @Override
              protected boolean matchesSafely(TenantAwareValue<T> tTenantAwareValue) {
                return tTenantAwareValue.getTenantId().equals(targetSupplier.get().getTenantId())
                    && greaterThanOrEqual.matches(targetSupplier.get().getValue());
              }
            });
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#hasItem(Object)}. */
  public static <T extends Serializable> SerializableMatcher<Iterable<TenantAwareValue<T>>> hasItem(
      final TenantAwareValue<T> target) {
    return fromSupplier(() -> (Matcher) Matchers.hasItem(target));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#hasItem(Object)}.
   *
   * <p>The item of type {@code T} will be serialized using the provided {@link Coder}. It is
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <T> SerializableMatcher<Iterable<TenantAwareValue<T>>> hasItem(
      Coder<T> coder, TenantAwareValue<T> target) {
    final SerializableSupplier<TenantAwareValue<T>> targetSupplier =
        new SerializableViaCoder<>(TenantAwareValueCoder.of(coder), target);
    return fromSupplier(() -> (Matcher) Matchers.hasItem(targetSupplier.get()));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#hasItem(Matcher)}. */
  public static <T> SerializableMatcher<Iterable<TenantAwareValue<T>>> hasItem(
      final SerializableMatcher<TenantAwareValue<T>> matcher) {
    return fromSupplier(() -> (Matcher) Matchers.hasItem(matcher));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#hasSize(int)}. */
  public static <T> SerializableMatcher<Collection<TenantAwareValue<T>>> hasSize(final int size) {
    return fromSupplier(() -> (Matcher) Matchers.hasSize(size));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#hasSize(Matcher)}. */
  public static <T> SerializableMatcher<Collection<TenantAwareValue<T>>> hasSize(
      final SerializableMatcher<? super Integer> sizeMatcher) {
    return fromSupplier(() -> (Matcher) Matchers.hasSize(sizeMatcher));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#iterableWithSize(int)}.
   */
  public static <T> SerializableMatcher<Iterable<TenantAwareValue<T>>> iterableWithSize(
      final int size) {
    return fromSupplier(() -> Matchers.iterableWithSize(size));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#iterableWithSize(Matcher)}.
   */
  public static <T> SerializableMatcher<Iterable<TenantAwareValue<T>>> iterableWithSize(
      final SerializableMatcher<? super Integer> sizeMatcher) {
    return fromSupplier(() -> Matchers.iterableWithSize(sizeMatcher));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#isIn(Collection)}. */
  public static <T extends Serializable> SerializableMatcher<TenantAwareValue<T>> isIn(
      final Collection<TenantAwareValue<T>> collection) {
    return fromSupplier(() -> Matchers.isIn(collection));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#isIn(Collection)}.
   *
   * <p>The items of type {@code T} will be serialized using the provided {@link Coder}. They are
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <T> SerializableMatcher<TenantAwareValue<T>> isIn(
      Coder<T> coder, Collection<TenantAwareValue<T>> collection) {
    @SuppressWarnings("unchecked")
    TenantAwareValue<T>[] items = (TenantAwareValue<T>[]) collection.toArray();
    final SerializableSupplier<TenantAwareValue<T>[]> itemsSupplier =
        new SerializableArrayViaCoder<>(TenantAwareValueCoder.of(coder), items);
    return fromSupplier(() -> Matchers.isIn(itemsSupplier.get()));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#isIn(Object[])}. */
  public static <T extends Serializable> SerializableMatcher<TenantAwareValue<T>> isIn(
      final TenantAwareValue<T>[] items) {
    return fromSupplier(() -> Matchers.isIn(items));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#isIn(Object[])}.
   *
   * <p>The items of type {@code T} will be serialized using the provided {@link Coder}. They are
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <T> SerializableMatcher<TenantAwareValue<T>> isIn(
      Coder<T> coder, TenantAwareValue<T>[] items) {
    final SerializableSupplier<TenantAwareValue<T>[]> itemsSupplier =
        new SerializableArrayViaCoder<>(TenantAwareValueCoder.of(coder), items);
    return fromSupplier(() -> Matchers.isIn(itemsSupplier.get()));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#isOneOf}. */
  @SafeVarargs
  public static <T extends Serializable> SerializableMatcher<TenantAwareValue<T>> isOneOf(
      final TenantAwareValue<T>... elems) {
    return fromSupplier(() -> Matchers.isOneOf(elems));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#isOneOf}.
   *
   * <p>The items of type {@code T} will be serialized using the provided {@link Coder}. They are
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  @SafeVarargs
  public static <T> SerializableMatcher<TenantAwareValue<T>> isOneOf(
      Coder<T> coder, TenantAwareValue<T>... items) {
    final SerializableSupplier<TenantAwareValue<T>[]> itemsSupplier =
        new SerializableArrayViaCoder<>(TenantAwareValueCoder.of(coder), items);
    return fromSupplier(() -> Matchers.isOneOf(itemsSupplier.get()));
  }

  /** A {@link SerializableMatcher} that matches any {@link KV} with the specified key. */
  public static <K extends Serializable, V>
      SerializableMatcher<? extends TenantAwareValue<KV<? extends K, ? extends V>>> kvWithKey(
          TenantAwareValue<K> key) {
    return new KvKeyMatcher<>(key.getTenantId(), equalTo(key));
  }

  /**
   * A {@link SerializableMatcher} that matches any {@link KV} with the specified key.
   *
   * <p>The key of type {@code K} will be serialized using the provided {@link Coder}. It is
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <K, V>
      SerializableMatcher<TenantAwareValue<KV<? extends K, ? extends V>>> kvWithKey(
          Coder<K> coder, TenantAwareValue<K> key) {
    return new KvKeyMatcher<>(key.getTenantId(), equalTo(coder, key));
  }

  /** A {@link SerializableMatcher} that matches any {@link KV} with matching key. */
  public static <K, V>
      SerializableMatcher<TenantAwareValue<KV<? extends K, ? extends V>>> kvWithKey(
          final SerializableMatcher<TenantAwareValue<K>> keyMatcher) {
    return new KvKeyMatcher<>(TenantAwareValue.NULL_TENANT, keyMatcher);
  }

  /** A {@link SerializableMatcher} that matches any {@link KV} with the specified value. */
  public static <K, V extends Serializable>
      SerializableMatcher<TenantAwareValue<KV<? extends K, ? extends V>>> kvWithValue(
          TenantAwareValue<V> value) {
    return new KvValueMatcher<>(equalTo(value));
  }

  /**
   * A {@link SerializableMatcher} that matches any {@link KV} with the specified value.
   *
   * <p>The value of type {@code V} will be serialized using the provided {@link Coder}. It is
   * explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <K, V>
      SerializableMatcher<TenantAwareValue<KV<? extends K, ? extends V>>> kvWithValue(
          Coder<V> coder, TenantAwareValue<V> value) {
    return new KvValueMatcher<>(equalTo(coder, value));
  }

  /** A {@link SerializableMatcher} that matches any {@link KV} with matching value. */
  public static <K, V>
      SerializableMatcher<TenantAwareValue<KV<? extends K, ? extends V>>> kvWithValue(
          final SerializableMatcher<TenantAwareValue<V>> valueMatcher) {
    return new KvValueMatcher<>(valueMatcher);
  }

  /** A {@link SerializableMatcher} that matches any {@link KV} with matching key and value. */
  public static <K, V> SerializableMatcher<TenantAwareValue<KV<? extends K, ? extends V>>> kv(
      final SerializableMatcher<TenantAwareValue<K>> keyMatcher,
      final SerializableMatcher<TenantAwareValue<V>> valueMatcher) {

    return SerializableMatchers.allOf(
        SerializableMatchers.kvWithKey(keyMatcher), SerializableMatchers.kvWithValue(valueMatcher));
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#lessThan(Comparable)}.
   */
  public static <T extends Comparable<T> & Serializable>
      SerializableMatcher<TenantAwareValue<T>> lessThan(final TenantAwareValue<T> target) {
    return fromSupplier(
        () ->
            new TypeSafeMatcher<TenantAwareValue<T>>() {
              Matcher<T> lessThan = OrderingComparison.lessThan(target.getValue());

              @Override
              public void describeTo(Description description) {
                lessThan.describeTo(description);
              }

              @Override
              protected boolean matchesSafely(TenantAwareValue<T> tTenantAwareValue) {
                return tTenantAwareValue.getTenantId().equals(target.getTenantId())
                    && lessThan.matches(target.getValue());
              }
            });
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link Matchers#lessThan(Comparable)}.
   *
   * <p>The target value of type {@code T} will be serialized using the provided {@link Coder}. It
   * is explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <T extends Comparable<T>> SerializableMatcher<TenantAwareValue<T>> lessThan(
      Coder<T> coder, TenantAwareValue<T> target) {
    final SerializableSupplier<TenantAwareValue<T>> targetSupplier =
        new SerializableViaCoder<>(TenantAwareValueCoder.of(coder), target);

    return fromSupplier(
        () ->
            new TypeSafeMatcher<TenantAwareValue<T>>() {
              Matcher<T> lessThan = OrderingComparison.lessThan(targetSupplier.get().getValue());

              @Override
              public void describeTo(Description description) {
                lessThan.describeTo(description);
              }

              @Override
              protected boolean matchesSafely(TenantAwareValue<T> tTenantAwareValue) {
                return tTenantAwareValue.getTenantId().equals(targetSupplier.get().getTenantId())
                    && lessThan.matches(targetSupplier.get().getValue());
              }
            });
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#lessThanOrEqualTo(Comparable)}.
   */
  public static <T extends Comparable<T> & Serializable>
      SerializableMatcher<TenantAwareValue<T>> lessThanOrEqualTo(final TenantAwareValue<T> target) {
    return fromSupplier(
        () ->
            new TypeSafeMatcher<TenantAwareValue<T>>() {
              Matcher<T> lessThanOrEqual = OrderingComparison.lessThanOrEqualTo(target.getValue());

              @Override
              public void describeTo(Description description) {
                lessThanOrEqual.describeTo(description);
              }

              @Override
              protected boolean matchesSafely(TenantAwareValue<T> tTenantAwareValue) {
                return tTenantAwareValue.getTenantId().equals(target.getTenantId())
                    && lessThanOrEqual.matches(target.getValue());
              }
            });
  }

  /**
   * A {@link SerializableMatcher} with identical criteria to {@link
   * Matchers#lessThanOrEqualTo(Comparable)}.
   *
   * <p>The target value of type {@code T} will be serialized using the provided {@link Coder}. It
   * is explicitly <i>not</i> required or expected to be serializable via Java serialization.
   */
  public static <T extends Comparable<T>>
      SerializableMatcher<TenantAwareValue<T>> lessThanOrEqualTo(
          Coder<T> coder, TenantAwareValue<T> target) {
    final SerializableSupplier<TenantAwareValue<T>> targetSupplier =
        new SerializableViaCoder<>(TenantAwareValueCoder.of(coder), target);

    return fromSupplier(
        () ->
            new TypeSafeMatcher<TenantAwareValue<T>>() {
              Matcher<T> lessThanOrEqual =
                  OrderingComparison.lessThanOrEqualTo(targetSupplier.get().getValue());

              @Override
              public void describeTo(Description description) {
                lessThanOrEqual.describeTo(description);
              }

              @Override
              protected boolean matchesSafely(TenantAwareValue<T> tTenantAwareValue) {
                return tTenantAwareValue.getTenantId().equals(targetSupplier.get().getTenantId())
                    && lessThanOrEqual.matches(targetSupplier.get().getValue());
              }
            });
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#not}. */
  public static <T> SerializableMatcher<TenantAwareValue<T>> not(
      final SerializableMatcher<TenantAwareValue<T>> matcher) {
    return fromSupplier(() -> Matchers.not(matcher));
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#nullValue}. */
  public static SerializableMatcher<Object> nullValue() {
    return fromSupplier(Matchers::nullValue);
  }

  /** A {@link SerializableMatcher} with identical criteria to {@link Matchers#startsWith}. */
  public static SerializableMatcher<TenantAwareValue<String>> startsWith(
      final TenantAwareValue<String> substring) {
    return fromSupplier(
        () ->
            new TypeSafeMatcher<TenantAwareValue<String>>() {
              StringStartsWith stringStartsWith = new StringStartsWith(substring.getValue());

              @Override
              public void describeTo(Description description) {
                stringStartsWith.describeTo(description);
              }

              @Override
              protected boolean matchesSafely(TenantAwareValue<String> stringTenantAwareValue) {
                return stringTenantAwareValue.getTenantId().equals(substring.getTenantId())
                    && stringStartsWith.matchesSafely(stringTenantAwareValue.getValue());
              }
            });
  }

  private static class KvKeyMatcher<K, V>
      extends BaseMatcher<TenantAwareValue<KV<? extends K, ? extends V>>>
      implements SerializableMatcher<TenantAwareValue<KV<? extends K, ? extends V>>> {
    private final SerializableMatcher<TenantAwareValue<K>> keyMatcher;
    private final String tenantId;

    public KvKeyMatcher(String tenantId, SerializableMatcher<TenantAwareValue<K>> keyMatcher) {
      this.tenantId = tenantId;
      this.keyMatcher = keyMatcher;
    }

    @Override
    public boolean matches(Object item) {
      @SuppressWarnings("unchecked")
      TenantAwareValue<KV<K, ?>> kvItem = (TenantAwareValue<KV<K, ?>>) item;
      return (kvItem.getTenantId().equals(tenantId)
              || kvItem.getTenantId().equals(TenantAwareValue.NULL_TENANT))
          && keyMatcher.matches(
              TenantAwareValue.of(kvItem.getValue().getTenantId(), kvItem.getValue().getKey()));
    }

    @Override
    public void describeMismatch(Object item, Description mismatchDescription) {
      @SuppressWarnings("unchecked")
      KV<K, ?> kvItem = (KV<K, ?>) item;
      if (!keyMatcher.matches(kvItem.getKey())) {
        mismatchDescription.appendText("key did not match: ");
        keyMatcher.describeMismatch(kvItem.getKey(), mismatchDescription);
      }
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("KV with key matching ");
      keyMatcher.describeTo(description);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).addValue(keyMatcher).toString();
    }
  }

  private static class KvValueMatcher<K, V>
      extends BaseMatcher<TenantAwareValue<KV<? extends K, ? extends V>>>
      implements SerializableMatcher<TenantAwareValue<KV<? extends K, ? extends V>>> {
    private final SerializableMatcher<TenantAwareValue<V>> valueMatcher;

    public KvValueMatcher(SerializableMatcher<TenantAwareValue<V>> valueMatcher) {
      this.valueMatcher = valueMatcher;
    }

    @Override
    public boolean matches(Object item) {
      @SuppressWarnings("unchecked")
      KV<?, V> kvItem = (KV<?, V>) item;
      return valueMatcher.matches(kvItem.getValue());
    }

    @Override
    public void describeMismatch(Object item, Description mismatchDescription) {
      @SuppressWarnings("unchecked")
      KV<?, V> kvItem = (KV<?, V>) item;
      if (!valueMatcher.matches(kvItem.getValue())) {
        mismatchDescription.appendText("value did not match: ");
        valueMatcher.describeMismatch(kvItem.getValue(), mismatchDescription);
      }
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("KV with value matching ");
      valueMatcher.describeTo(description);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).addValue(valueMatcher).toString();
    }
  }

  /**
   * Constructs a {@link SerializableMatcher} from a non-serializable {@link Matcher} via
   * indirection through {@link SerializableSupplier}.
   *
   * <p>To wrap a {@link Matcher} which is not serializable, provide a {@link SerializableSupplier}
   * with a {@link SerializableSupplier#get()} method that returns a fresh instance of the {@link
   * Matcher} desired. The resulting {@link SerializableMatcher} will behave according to the {@link
   * Matcher} returned by {@link SerializableSupplier#get() get()} when it is invoked during
   * matching (which may occur on another machine).
   *
   * <pre>{@code
   * return fromSupplier(new SerializableSupplier<Matcher<T>>() {
   *   *     @Override
   *     public Matcher<T> get() {
   *       return new MyMatcherForT();
   *     }
   * });
   * }</pre>
   */
  public static <T> SerializableMatcher<T> fromSupplier(SerializableSupplier<Matcher<T>> supplier) {
    return new SerializableMatcherFromSupplier<>(supplier);
  }

  /**
   * Supplies values of type {@code T}, and is serializable. Thus, even if {@code T} is not
   * serializable, the supplier can be serialized and provide a {@code T} wherever it is
   * deserialized.
   *
   * @param <T> the type of value supplied.
   */
  public interface SerializableSupplier<T> extends Serializable {
    T get();
  }

  /**
   * Since the delegate {@link Matcher} is not generally serializable, instead this takes a nullary
   * SerializableFunction to return such a matcher.
   */
  private static class SerializableMatcherFromSupplier<T> extends BaseMatcher<T>
      implements SerializableMatcher<T> {

    private SerializableSupplier<Matcher<T>> supplier;

    public SerializableMatcherFromSupplier(SerializableSupplier<Matcher<T>> supplier) {
      this.supplier = supplier;
    }

    @Override
    public void describeTo(Description description) {
      supplier.get().describeTo(description);
    }

    @Override
    public boolean matches(Object item) {
      return supplier.get().matches(item);
    }

    @Override
    public void describeMismatch(Object item, Description mismatchDescription) {
      supplier.get().describeMismatch(item, mismatchDescription);
    }
  }

  /**
   * Wraps any value that can be encoded via a {@link Coder} to make it {@link Serializable}. This
   * is not likely to be a good encoding, so should be used only for tests, where data volume is
   * small and minor costs are not critical.
   */
  private static class SerializableViaCoder<T> implements SerializableSupplier<T> {
    /** Cached value that is not serialized. */
    @Nullable private transient T value;

    /** The bytes of {@link #value} when encoded via {@link #coder}. */
    private byte[] encodedValue;

    private Coder<T> coder;

    public SerializableViaCoder(Coder<T> coder, T value) {
      this.coder = coder;
      this.value = value;
      try {
        this.encodedValue = CoderUtils.encodeToByteArray(this.coder, value);
      } catch (CoderException exc) {
        throw new RuntimeException("Error serializing via Coder", exc);
      }
    }

    @Override
    public T get() {
      if (value == null) {
        try {
          value = CoderUtils.decodeFromByteArray(coder, encodedValue);
        } catch (CoderException exc) {
          throw new RuntimeException("Error deserializing via Coder", exc);
        }
      }
      return value;
    }
  }

  /**
   * Wraps any array with values that can be encoded via a {@link Coder} to make it {@link
   * Serializable}. This is not likely to be a good encoding, so should be used only for tests,
   * where data volume is small and minor costs are not critical.
   */
  private static class SerializableArrayViaCoder<T> implements SerializableSupplier<T[]> {
    /** Cached value that is not serialized. */
    @Nullable private transient T[] value;

    /** The bytes of {@link #value} when encoded via {@link #coder}. */
    private byte[] encodedValue;

    private Coder<List<T>> coder;

    public SerializableArrayViaCoder(Coder<T> elementCoder, T[] value) {
      this.coder = ListCoder.of(elementCoder);
      this.value = value;
      try {
        this.encodedValue = CoderUtils.encodeToByteArray(coder, Arrays.asList(value));
      } catch (CoderException exc) {
        throw UserCodeException.wrap(exc);
      }
    }

    @Override
    public T[] get() {
      if (value == null) {
        try {
          @SuppressWarnings("unchecked")
          T[] decoded = (T[]) CoderUtils.decodeFromByteArray(coder, encodedValue).toArray();
          value = decoded;
        } catch (CoderException exc) {
          throw new RuntimeException("Error deserializing via Coder", exc);
        }
      }
      return value;
    }
  }
}
