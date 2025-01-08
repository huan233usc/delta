/*
 * Copyright (2023) The Delta Lake Project Authors.
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

package io.delta.kernel.utils;

import java.io.IOException;
import java.util.function.Function;

public class InterceptCloseableIterator<T> implements CloseableIterator<T> {

  private CloseableIterator<T> delegate;

  private Function<T, Void> interceptFunc;

  public InterceptCloseableIterator(
      CloseableIterator<T> delegate, Function<T, Void> interceptFunc) {
    this.delegate = delegate;
    this.interceptFunc = interceptFunc;
  }

  @Override
  public boolean hasNext() {
    return delegate.hasNext();
  }

  @Override
  public T next() {
    T next = delegate.next();
    interceptFunc.apply(next);
    return next;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
