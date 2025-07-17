/*
 * Copyright (2021) The Delta Lake Project Authors.
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
package org.apache.spark.sql.delta
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, Command, DeleteFromTable, LogicalPlan, MergeIntoTable, OverwriteByExpression, UpdateTable, V2WriteCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}

class ResolveDeltaKernelTable(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with DeltaLogging {

  /**
   * Identifies the direct sub-plan of a Command that represents the write target.
   * This version explicitly matches each known command class to correctly extract
   * the target logical plan.
   */
  private def getWriteTarget(command: Command): Option[LogicalPlan] = command match {
    // Specific V2 commands like MERGE, UPDATE, DELETE
    case m: MergeIntoTable => Some(m.targetTable)
    case u: UpdateTable => Some(u.table)
    case d: DeleteFromTable => Some(d.table)

    // V1 commands for backwards compatibility
    case a: AppendData => Some(a.table)
    case o: OverwriteByExpression => Some(o.table)

    // Default case for other/unknown commands
    case _ => None
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // A map used as a set to track relation instances that are write targets.
    // It is defined locally within apply() to ensure thread safety and no state
    // leakage between different query analyses.
    val writeTargets = new java.util.IdentityHashMap[LogicalPlan, java.lang.Boolean]()

    // Phase 1: Mark all write targets.
    // We traverse the plan to build a context of which relations are write targets.
    // The `case _ =>` is critical to prevent MatchErrors on other node types.
    plan.foreach {
      case cmd: Command =>
        getWriteTarget(cmd).foreach { targetPlan =>
          targetPlan.foreach {
            case relation @ (_: DataSourceV2Relation) =>
              writeTargets.put(relation, true)
            case _ => // Safely ignore other nodes within the target plan.
          }
        }
      case _ => // Safely ignore non-Command nodes at the top level.
    }

    plan.resolveOperatorsDown {
      case r @ DataSourceV2Relation(table: DeltaTableV2, _, _, _, _) =>
        if (writeTargets.containsKey(r)) {
          r // It's a write target, leave it unchanged.
        } else {
          // It's a read source, perform the conversion.
          table.kernelTable.map(kernelTbl => r.copy(table = kernelTbl)).getOrElse(r)
        }
      case other =>
        other
    }
  }
}
