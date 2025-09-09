/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.spark.dsv2

import io.delta.spark.dsv2.table.DeltaKernelTable
import io.delta.spark.dsv2.utils.CCv2Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

class MaybeApplyV2Connector(session: SparkSession)
    extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // scalastyle:off println
    println(plan)
    // scalastyle:on println
    val isReadOnlyPlan = isReadOnly(plan)
    val result = if (isReadOnlyPlan) {
      plan.resolveOperatorsDown {
        case Batch(v2Connector) => v2Connector
        case Streaming(v2Connector) => v2Connector
      }
    } else {
      plan
    }
    // scalastyle:off println
    println(result)
    // scalastyle:on println
    result
  }

  private def isReadOnly(plan: LogicalPlan): Boolean = {
    !plan.containsPattern(COMMAND) && !plan.exists(_.isInstanceOf[InsertIntoStatement])
  }

  object Batch {
    def unapply(dsv2: DataSourceV2Relation): Option[DataSourceV2Relation] = dsv2.table match {
      case d: DeltaTableV2 =>
        Some(dsv2.copy(table = CCv2Utils.convertToV2Connector(d)))
      case _ => None
    }
  }
  object Streaming {
    def unapply(dsv2: StreamingRelationV2): Option[StreamingRelationV2] = dsv2.table match {
      case d: DeltaTableV2 =>
        Some(dsv2.copy(table = CCv2Utils.convertToV2Connector(d)))
      case _ => None
    }

  }
}

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

package io.delta.spark.dsv2
import io.delta.spark.dsv2.table.DeltaKernelTable
import io.delta.spark.dsv2.utils.CCv2Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

class MaybeApplyV2Connector(session: SparkSession)
    extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (isReadOnly(plan)) return plan

    def replaceKernelWithFallback(node: LogicalPlan): LogicalPlan = {
      node.resolveOperatorsDown {
        case Batch(fallback) => fallback
        case Streaming(fallback) => fallback
      }
    }

    plan.resolveOperatorsDown {
      case i @ InsertIntoStatement(table, _, _, _, _, _, _) =>
        val newTable = replaceKernelWithFallback(table)
        i.copy(table = newTable)
      case other => replaceKernelWithFallback(other)
    }
  }

  private def isReadOnly(plan: LogicalPlan): Boolean = {
    !plan.containsPattern(COMMAND) && !plan.exists(_.isInstanceOf[InsertIntoStatement])
  }


  object Batch {
    def unapply(dsv2: DataSourceV2Relation): Option[DataSourceV2Relation] = dsv2.table match {
      case d: DeltaKernelTable if d.toFallback.isPresent =>
       Some(dsv2.copy(table = d.toFallback.get()))
      case _ => None
    }
  }
  object Streaming {
    def unapply(dsv2: StreamingRelationV2): Option[StreamingRelationV2] = dsv2.table match {
      case  d: DeltaKernelTable if d.toFallback.isPresent =>
        Some(dsv2.copy(table = d.toFallback.get()))
      case _ => None
    }

  }
}
