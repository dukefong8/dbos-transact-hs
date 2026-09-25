-- | Acceptance mirror for the starter's workflow, recovery, events, messages,
-- and queues paths. The individual domain test modules remain the watcher
-- toggles; this group is the full L2 public-behavior gate.
module DBOS.StarterTest (tests) where

import DBOS.Prelude
import DBOS.Transact.EventTest qualified as Event
import DBOS.Transact.InstanceTest qualified as Instance
import DBOS.Transact.MessageTest qualified as Message
import DBOS.Transact.QueueTest qualified as Queue
import DBOS.Transact.StepTest qualified as Step
import DBOS.Transact.WorkflowTest qualified as Workflow
import Test.Tasty (TestTree, testGroup)

tests :: TestTree
tests =
  testGroup
    "Starter mirror"
    [ Instance.tests,
      Workflow.tests,
      Step.tests,
      Event.tests,
      Message.tests,
      Queue.tests
    ]
