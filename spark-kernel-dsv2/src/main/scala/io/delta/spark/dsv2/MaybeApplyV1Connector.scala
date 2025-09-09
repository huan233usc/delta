package io.delta.spark.dsv2

import io.delta.spark.dsv2.table.SparkTable

import org.apache.spark.sql.delta.catalog.DeltaTableV2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

class MaybeApplyV1Connector(session: SparkSession)
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
      case i @ InsertIntoStatement(table, part, cols, query, overwrite, byName, ifNotExists) =>
        val newTable = replaceKernelWithFallback(table)
        i.copy(table = newTable)
      case Batch(fallback) => fallback
      case Streaming(fallback) => fallback
    }
  }

  private def isReadOnly(plan: LogicalPlan): Boolean = {
    !plan.containsPattern(COMMAND) && !plan.exists(_.isInstanceOf[InsertIntoStatement])
  }

  object Batch {
    def unapply(dsv2: DataSourceV2Relation): Option[DataSourceV2Relation] = dsv2.table match {
      case d: SparkTable if d.v1Fallback().isPresent =>
        Some(dsv2.copy(table = d.v1Fallback().get()))
      case _ => None
    }
  }
  object Streaming {
    def unapply(dsv2: StreamingRelationV2): Option[StreamingRelationV2] = dsv2.table match {
      case d: SparkTable if d.v1Fallback().isPresent =>
        Some(dsv2.copy(table = d.v1Fallback().get()))
      case _ => None
    }

  }
}
