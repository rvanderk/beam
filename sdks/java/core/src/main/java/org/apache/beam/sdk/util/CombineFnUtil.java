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
package org.apache.beam.sdk.util;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.Context;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.TenantAwareValue;
import org.apache.beam.sdk.values.TenantAwareValue.TenantAwareValueCoder;

/** Static utility methods that create combine function instances. */
public class CombineFnUtil {

  /**
   * Returns the partial application of the {@link CombineFnWithContext} to a specific context to
   * produce a {@link CombineFn}.
   *
   * <p>The returned {@link CombineFn} cannot be serialized.
   */
  public static <K, InputT, AccumT, OutputT> CombineFn<InputT, AccumT, OutputT> bindContext(
      CombineFnWithContext<InputT, AccumT, OutputT> combineFn, StateContext<?> stateContext) {
    Context context = CombineContextFactory.createFromStateContext(stateContext);
    return new NonSerializableBoundedCombineFn<>(combineFn, context);
  }

  /** Return a {@link CombineFnWithContext} from the given {@link GlobalCombineFn}. */
  public static <InputT, AccumT, OutputT>
      CombineFnWithContext<InputT, AccumT, OutputT> toFnWithContext(
          GlobalCombineFn<InputT, AccumT, OutputT> globalCombineFn) {
    if (globalCombineFn instanceof CombineFnWithContext) {
      @SuppressWarnings("unchecked")
      CombineFnWithContext<InputT, AccumT, OutputT> combineFnWithContext =
          (CombineFnWithContext<InputT, AccumT, OutputT>) globalCombineFn;
      return combineFnWithContext;
    } else {
      @SuppressWarnings("unchecked")
      final CombineFn<InputT, AccumT, OutputT> combineFn =
          (CombineFn<InputT, AccumT, OutputT>) globalCombineFn;
      return new CombineFnWithContext<InputT, AccumT, OutputT>() {
        @Override
        public TenantAwareValue<AccumT> createAccumulator(Context c) {
          return combineFn.createAccumulator();
        }

        @Override
        public TenantAwareValue<AccumT> addInput(
            TenantAwareValue<AccumT> accumulator, TenantAwareValue<InputT> input, Context c) {
          return combineFn.addInput(accumulator, input);
        }

        @Override
        public TenantAwareValue<AccumT> mergeAccumulators(
            Iterable<TenantAwareValue<AccumT>> accumulators, Context c) {
          return combineFn.mergeAccumulators(accumulators);
        }

        @Override
        public TenantAwareValue<OutputT> extractOutput(
            TenantAwareValue<AccumT> accumulator, Context c) {
          return combineFn.extractOutput(accumulator);
        }

        @Override
        public TenantAwareValue<AccumT> compact(TenantAwareValue<AccumT> accumulator, Context c) {
          return combineFn.compact(accumulator);
        }

        @Override
        public TenantAwareValue<OutputT> defaultValue() {
          return combineFn.defaultValue();
        }

        @Override
        public TenantAwareValueCoder<AccumT> getAccumulatorCoder(
            CoderRegistry registry, Coder<InputT> inputCoder) throws CannotProvideCoderException {
          return combineFn.getAccumulatorCoder(registry, inputCoder);
        }

        @Override
        public Coder<OutputT> getDefaultOutputCoder(
            CoderRegistry registry, Coder<InputT> inputCoder) throws CannotProvideCoderException {
          return combineFn.getDefaultOutputCoder(registry, inputCoder);
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
          super.populateDisplayData(builder);
          combineFn.populateDisplayData(builder);
        }
      };
    }
  }

  private static class NonSerializableBoundedCombineFn<InputT, AccumT, OutputT>
      extends CombineFn<InputT, AccumT, OutputT> {
    private final CombineFnWithContext<InputT, AccumT, OutputT> combineFn;
    private final Context context;

    private NonSerializableBoundedCombineFn(
        CombineFnWithContext<InputT, AccumT, OutputT> combineFn, Context context) {
      this.combineFn = combineFn;
      this.context = context;
    }

    @Override
    public TenantAwareValue<AccumT> createAccumulator() {
      return combineFn.createAccumulator(context);
    }

    @Override
    public TenantAwareValue<AccumT> addInput(
        TenantAwareValue<AccumT> accumulator, TenantAwareValue<InputT> value) {
      return combineFn.addInput(accumulator, value, context);
    }

    @Override
    public TenantAwareValue<AccumT> mergeAccumulators(
        Iterable<TenantAwareValue<AccumT>> accumulators) {
      return combineFn.mergeAccumulators(accumulators, context);
    }

    @Override
    public TenantAwareValue<OutputT> extractOutput(TenantAwareValue<AccumT> accumulator) {
      return combineFn.extractOutput(accumulator, context);
    }

    @Override
    public TenantAwareValue<AccumT> compact(TenantAwareValue<AccumT> accumulator) {
      return combineFn.compact(accumulator, context);
    }

    @Override
    public TenantAwareValueCoder<AccumT> getAccumulatorCoder(
        CoderRegistry registry, Coder<InputT> inputCoder) throws CannotProvideCoderException {
      return combineFn.getAccumulatorCoder(registry, inputCoder);
    }

    @Override
    public Coder<OutputT> getDefaultOutputCoder(CoderRegistry registry, Coder<InputT> inputCoder)
        throws CannotProvideCoderException {
      return combineFn.getDefaultOutputCoder(registry, inputCoder);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      combineFn.populateDisplayData(builder);
    }

    private void writeObject(@SuppressWarnings("unused") ObjectOutputStream out)
        throws IOException {
      throw new NotSerializableException(
          "Cannot serialize the CombineFn resulting from CombineFnUtil.bindContext.");
    }
  }
}
