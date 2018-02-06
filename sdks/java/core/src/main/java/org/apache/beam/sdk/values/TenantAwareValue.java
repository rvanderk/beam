package org.apache.beam.sdk.values;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;

/**
 * An immutable pair of a value and a tenant id.
 *
 * @param <V> the type of the value
 */
public class TenantAwareValue<V> {
  public static final String NULL_TENANT = "NULL";

  /** Returns a new {@link TenantAwareValue} with the given value. */
  public static <V> TenantAwareValue<V> of(String tenantId, @Nullable V value) {
    return new TenantAwareValue<>(tenantId, value);
  }

  /**
   * Returns a new {@link TenantAwareValue} with the given <code>KV</code> pair. The <code>KV</code>
   * pair's tenant is set to the provided tenantid
   */
  public static <K, V> TenantAwareValue<KV<K, V>> of(String tenantId, KV<K, V> value) {
    return new TenantAwareValue<>(tenantId, value.withTenant(tenantId));
  }

  /**
   * Returns a new {@link TenantAwareValue} with the given KV pair. The tenantId for the {@link
   * TenantAwareValue} is set to the tenantId as stored in the <code>KV</code>
   */
  public static <K, V> TenantAwareValue<KV<K, V>> of(KV<K, V> value) {
    return new TenantAwareValue<>(value.getTenantId(), value);
  }

  public String getTenantId() {
    return tenantId;
  }

  public V getValue() {
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TenantAwareValue)) {
      return false;
    }
    TenantAwareValue<?> that = (TenantAwareValue<?>) other;
    return Objects.equals(value, that.value)
        && Objects.equals(that.getTenantId(), this.getTenantId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantId, value);
  }

  @Override
  public String toString() {
    return "TenantAwareValue(" + tenantId + ", " + value + ")";
  }

  /////////////////////////////////////////////////////////////////////////////

  /** A {@link Coder} for {@link TenantAwareValue}. */
  public static class TenantAwareValueCoder<T> extends StructuredCoder<TenantAwareValue<T>> {

    private final Coder<T> valueCoder;

    public static <T> TenantAwareValueCoder<T> of(Coder<T> valueCoder) {
      return new TenantAwareValueCoder<>(valueCoder);
    }

    @SuppressWarnings("unchecked")
    TenantAwareValueCoder(Coder<T> valueCoder) {
      this.valueCoder = checkNotNull(valueCoder);
    }

    @Override
    public void encode(TenantAwareValue<T> windowedElem, OutputStream outStream)
        throws IOException {
      StringUtf8Coder.of().encode(windowedElem.getTenantId(), outStream);
      valueCoder.encode(windowedElem.getValue(), outStream);
    }

    @Override
    public TenantAwareValue<T> decode(InputStream inStream) throws IOException {
      String tenantId = StringUtf8Coder.of().decode(inStream);
      T value = valueCoder.decode(inStream);
      return TenantAwareValue.of(tenantId, value);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(
          this, "TenantAwareValueCoder requires a deterministic valueCoder", valueCoder);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.<Coder<?>>asList(valueCoder);
    }

    public Coder<T> getValueCoder() {
      return valueCoder;
    }

    @Override
    public TypeDescriptor<TenantAwareValue<T>> getEncodedTypeDescriptor() {
      return new TypeDescriptor<TenantAwareValue<T>>() {}.where(
          new TypeParameter<T>() {}, valueCoder.getEncodedTypeDescriptor());
    }

    @Override
    public List<? extends Coder<?>> getComponents() {
      return Collections.singletonList(valueCoder);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  private final String tenantId;
  private final @Nullable V value;

  protected TenantAwareValue(String tenantId, @Nullable V value) {
    checkNotNull(tenantId, "tenantId must be non-null");

    this.tenantId = tenantId;
    this.value = value;
  }
}
