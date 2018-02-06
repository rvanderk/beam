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
package org.apache.beam.sdk.values;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableComparator;

/**
 * An immutable key/value pair.
 *
 * <p>Various {@link PTransform PTransforms} like {@link GroupByKey} and {@link Combine#perKey}
 * operate on {@link PCollection PCollections} of {@link KV KVs}.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class KV<K, V> implements Serializable {
  private static final String NULL_TENANT = "NO_TENANT";

  /** Returns a {@link KV} with the given key and value. */
  public static <K, V> KV<K, V> of(@Nullable K key, TenantAwareValue<V> value) {
    return new KV<>(TenantAwareValue.of(value.getTenantId(), key), value.getValue());
  }

  /** Returns a {@link KV} with the given key and value. */
  public static <K, V> KV<K, V> of(@Nullable K key, V value) {
    return new KV<>(TenantAwareValue.of(NULL_TENANT, key), value);
  }

  /** Returns the key of this {@link KV}. */
  public @Nullable K getKey() {
    return key.getValue();
  }

  /** Returns the value of this {@link KV}. */
  public V getValue() {
    return value;
  }

  /** Returns the tenantId of this {@link KV}. */
  public String getTenantId() {
    return key.getTenantId();
  }

  /** Returns the tenant-aware value of this {@link KV}. */
  public TenantAwareValue<V> getTenantAwareValue() {
    if (key.getTenantId() == NULL_TENANT)
      throw new IllegalStateException("Tenant has not been assigned yet");

    return TenantAwareValue.of(key.getTenantId(), value);
  }

  public boolean hasTenant() {
    return key.getTenantId() != NULL_TENANT;
  }

  public KV<K, V> withTenant(String tenantId) {
    if (key.getTenantId() != NULL_TENANT) // TODO: also ignore when setting to same tenantId?
    throw new IllegalStateException("Tenant has already been assigned yet");
    return KV.of(key.getValue(), TenantAwareValue.of(tenantId, value));
  }

  /////////////////////////////////////////////////////////////////////////////

  final TenantAwareValue<K> key;
  final @Nullable V value;

  private KV(TenantAwareValue<K> key, @Nullable V value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof KV)) {
      return false;
    }
    KV<?, ?> otherKv = (KV<?, ?>) other;
    // Arrays are very common as values and keys, so deepEquals is mandatory
    return Objects.deepEquals(this.key, otherKv.key)
        && Objects.deepEquals(this.value, otherKv.value);
  }

  /**
   * A {@link Comparator} that orders {@link KV KVs} by the natural ordering of their keys.
   *
   * <p>A {@code null} key is less than any non-{@code null} key.
   */
  public static class OrderByKey<K extends Comparable<? super K>, V>
      implements SerializableComparator<KV<K, V>> {
    @Override
    public int compare(KV<K, V> a, KV<K, V> b) {
      if (a.key == null) {
        return b.key == null ? 0 : -1;
      } else if (b.key == null) {
        return 1;
      } else {
        int tenantCompare = a.key.getTenantId().compareTo(b.key.getTenantId());
        if (tenantCompare != 0) return tenantCompare;
        return a.key.getValue().compareTo(b.key.getValue());
      }
    }
  }

  /**
   * A {@link Comparator} that orders {@link KV KVs} by the natural ordering of their values.
   *
   * <p>A {@code null} value is less than any non-{@code null} value.
   */
  public static class OrderByValue<K, V extends Comparable<? super V>>
      implements SerializableComparator<KV<K, V>> {
    @Override
    public int compare(KV<K, V> a, KV<K, V> b) {
      if (a.value == null) {
        return b.value == null ? 0 : -1;
      } else if (b.value == null) {
        return 1;
      } else {
        return a.value.compareTo(b.value);
      }
    }
  }

  @Override
  public int hashCode() {
    // Objects.deepEquals requires Arrays.deepHashCode for correctness
    return Arrays.deepHashCode(new Object[] {key, value});
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).addValue(key.getValue()).addValue(value).toString();
  }
}
