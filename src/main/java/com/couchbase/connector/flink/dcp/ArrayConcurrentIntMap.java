/*
 * Copyright 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connector.flink.dcp;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class ArrayConcurrentIntMap<T> implements ConcurrentIntMap<T>, Serializable {
  private static final long serialVersionUID = 1;

  private final AtomicReferenceArray<T> array;

  public ArrayConcurrentIntMap(int backingArraySize) {
    this.array = new AtomicReferenceArray<>(backingArraySize);
  }

  @Override
  public T put(int key, T value) {
    return array.getAndSet(key, value);
  }

  @Override
  public T get(int key) {
    return array.get(key);
  }
}
