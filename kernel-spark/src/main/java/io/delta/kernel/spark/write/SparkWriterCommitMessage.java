/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.spark.write;

import io.delta.kernel.data.Row;
import io.delta.kernel.spark.utils.SerializableKernelRowWrapper;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * Commit message that carries Delta actions (AddFile) from executor to driver.
 *
 * <p>Each executor task may write multiple files, so this message contains an array of serializable
 * Row wrappers representing AddFile actions.
 */
public class SparkWriterCommitMessage implements WriterCommitMessage {
  private final SerializableKernelRowWrapper[] actionWrappers;

  public SparkWriterCommitMessage(SerializableKernelRowWrapper[] actionWrappers) {
    this.actionWrappers = actionWrappers;
  }

  public Row[] getActions() {
    // Unwrap serialized actions back to Row objects
    Row[] actions = new Row[actionWrappers.length];
    for (int i = 0; i < actionWrappers.length; i++) {
      actions[i] = actionWrappers[i].getRow();
    }
    return actions;
  }
}
