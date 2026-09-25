module DBOS.SchemaTest
  ( tests,
  )
where

import DBOS.Prelude
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, (@?=), testCase)

tests :: TestTree
tests =
  testGroup
    "Schema Records"
    [ testCase "records the live Python DBOS table set used by Haskell tests" $
        dbosTables @?= expectedTables,
      testCase "records the workflow_status columns used by Haskell tests" $
        assertBool
          "workflow_status contract columns are present"
          (all (`elem` workflowStatusColumns) requiredWorkflowStatusColumns),
      testCase "records the operation_outputs columns used by Haskell tests" $
        assertBool
          "operation_outputs contract columns are present"
          (all (`elem` operationOutputsColumns) requiredOperationOutputsColumns),
      testCase "records the notifications columns used by Haskell tests" $
        assertBool
          "notifications contract columns are present"
          (all (`elem` notificationsColumns) requiredNotificationsColumns)
    ]

expectedTables :: [String]
expectedTables =
  [ "application_versions",
    "dbos_migrations",
    "event_dispatch_kv",
    "notifications",
    "operation_outputs",
    "queues",
    "streams",
    "workflow_events",
    "workflow_events_history",
    "workflow_schedules",
    "workflow_status"
  ]

dbosTables :: [String]
dbosTables = expectedTables

expectedWorkflowStatusColumns :: [String]
expectedWorkflowStatusColumns =
  [ "workflow_uuid",
    "status",
    "name",
    "config_name",
    "class_name",
    "authenticated_user",
    "authenticated_roles",
    "assumed_role",
    "queue_name",
    "executor_id",
    "created_at",
    "updated_at",
    "application_version",
    "application_id",
    "workflow_deadline_epoch_ms",
    "workflow_timeout_ms",
    "deduplication_id",
    "priority",
    "inputs",
    "queue_partition_key",
    "forked_from",
    "parent_workflow_id",
    "started_at_epoch_ms",
    "serialization",
    "owner_xid",
    "delay_until_epoch_ms",
    "attributes",
    "schedule_name",
    "output",
    "error",
    "request",
    "recovery_attempts",
    "completed_at",
    "dequeued_at",
    "was_forked_from"
  ]

workflowStatusColumns :: [String]
workflowStatusColumns = expectedWorkflowStatusColumns

requiredWorkflowStatusColumns :: [String]
requiredWorkflowStatusColumns =
  [ "workflow_uuid",
    "status",
    "inputs",
    "serialization",
    "request",
    "attributes",
    "parent_workflow_id",
    "recovery_attempts",
    "forked_from"
  ]

expectedOperationOutputsColumns :: [String]
expectedOperationOutputsColumns =
  [ "workflow_uuid",
    "function_id",
    "function_name",
    "output",
    "error",
    "child_workflow_id",
    "started_at_epoch_ms",
    "completed_at_epoch_ms",
    "serialization"
  ]

operationOutputsColumns :: [String]
operationOutputsColumns = expectedOperationOutputsColumns

requiredOperationOutputsColumns :: [String]
requiredOperationOutputsColumns =
  [ "workflow_uuid",
    "function_id",
    "function_name",
    "child_workflow_id",
    "started_at_epoch_ms",
    "completed_at_epoch_ms",
    "serialization"
  ]

expectedNotificationsColumns :: [String]
expectedNotificationsColumns =
  [ "destination_uuid",
    "topic",
    "message",
    "created_at_epoch_ms",
    "message_uuid",
    "serialization",
    "consumed"
  ]

notificationsColumns :: [String]
notificationsColumns = expectedNotificationsColumns

requiredNotificationsColumns :: [String]
requiredNotificationsColumns =
  [ "destination_uuid",
    "topic",
    "message",
    "message_uuid",
    "serialization",
    "consumed"
  ]
