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
package io.delta.kernel;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import java.io.IOException;

public class LoggingTransactionStatCollector implements TransactionStatCollector {
  @Override
  public void recordAddedFile(AddFile addFile) {
    System.out.println(addFile);
  }

  @Override
  public void recordMetadata(Metadata metadata) {
    System.out.println(metadata);
  }

  @Override
  public void recordProtocol(Protocol protocol) {
    System.out.println(protocol);
  }

  @Override
  public void onCommitSucceeds(Engine engine, long commitAsVersion) throws IOException {
    System.out.println("on commit succeeds");
  }
}
