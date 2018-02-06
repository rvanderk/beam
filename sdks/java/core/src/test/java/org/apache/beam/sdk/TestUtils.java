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
package org.apache.beam.sdk;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TenantAwareValue;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/** Utilities for tests. */
public class TestUtils {
  // Do not instantiate.
  private TestUtils() {}

  public static final TenantAwareValue<String>[] NO_LINES_ARRAY = new TenantAwareValue[] {};

  public static final List<TenantAwareValue<String>> NO_LINES = Arrays.asList(NO_LINES_ARRAY);

  public static final TenantAwareValue<String>[] LINES_ARRAY =
      new TenantAwareValue[] {
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "To be, or not to be: that is the question: "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "Whether 'tis nobler in the mind to suffer "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "The slings and arrows of outrageous fortune, "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "Or to take arms against a sea of troubles, "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "And by opposing end them? To die: to sleep; "),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "No more; and by a sleep to say we end "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "The heart-ache and the thousand natural shocks "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "That flesh is heir to, 'tis a consummation "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "Devoutly to be wish'd. To die, to sleep; "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "To sleep: perchance to dream: ay, there's the rub; "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "For in that sleep of death what dreams may come "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "When we have shuffled off this mortal coil, "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "Must give us pause: there's the respect "),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "That makes calamity of so long life; "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "For who would bear the whips and scorns of time, "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "The oppressor's wrong, the proud man's contumely, "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "The pangs of despised love, the law's delay, "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "The insolence of office and the spurns "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "That patient merit of the unworthy takes, "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "When he himself might his quietus make "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "With a bare bodkin? who would fardels bear, "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "To grunt and sweat under a weary life, "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "But that the dread of something after death, "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "The undiscover'd country from whose bourn "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "No traveller returns, puzzles the will "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "And makes us rather bear those ills we have "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "Than fly to others that we know not of? "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "Thus conscience does make cowards of us all; "),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "And thus the native hue of resolution "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "Is sicklied o'er with the pale cast of thought, "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "And enterprises of great pith and moment "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "With this regard their currents turn awry, "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "And lose the name of action.--Soft you now! "),
        TenantAwareValue.of(
            TenantAwareValue.NULL_TENANT, "The fair Ophelia! Nymph, in thy orisons "),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "Be all my sins remember'd.")
      };

  public static final List<TenantAwareValue<String>> LINES = Arrays.asList(LINES_ARRAY);

  public static final TenantAwareValue<String>[] LINES2_ARRAY =
      new TenantAwareValue[] {
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "hi"),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "there"),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, "bob!")
      };

  public static final List<TenantAwareValue<String>> LINES2 = Arrays.asList(LINES2_ARRAY);

  public static final TenantAwareValue<Integer>[] NO_INTS_ARRAY = new TenantAwareValue[] {};

  public static final List<TenantAwareValue<Integer>> NO_INTS = Arrays.asList(NO_INTS_ARRAY);

  public static final TenantAwareValue<Integer>[] INTS_ARRAY =
      new TenantAwareValue[] {
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 3),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 42),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, Integer.MAX_VALUE),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 0),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, -1),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, Integer.MIN_VALUE),
        TenantAwareValue.of(TenantAwareValue.NULL_TENANT, 666)
      };

  public static final List<TenantAwareValue<Integer>> INTS = Arrays.asList(INTS_ARRAY);

  /** Matcher for KVs. */
  public static class KvMatcher<K, V> extends TypeSafeMatcher<KV<? extends K, ? extends V>> {
    final Matcher<? super K> keyMatcher;
    final Matcher<? super V> valueMatcher;

    public static <K, V> KvMatcher<K, V> isKv(Matcher<K> keyMatcher, Matcher<V> valueMatcher) {
      return new KvMatcher<>(keyMatcher, valueMatcher);
    }

    public KvMatcher(Matcher<? super K> keyMatcher, Matcher<? super V> valueMatcher) {
      this.keyMatcher = keyMatcher;
      this.valueMatcher = valueMatcher;
    }

    @Override
    public boolean matchesSafely(KV<? extends K, ? extends V> kv) {
      return keyMatcher.matches(kv.getKey()) && valueMatcher.matches(kv.getValue());
    }

    @Override
    public void describeTo(Description description) {
      description
          .appendText("a KV(")
          .appendValue(keyMatcher)
          .appendText(", ")
          .appendValue(valueMatcher)
          .appendText(")");
    }
  }
}
