module DBOS.Transact
  ( -- * Lifecycle (lifecycle-): configure, launch, recover, shut down
  -- lifecycle-config
    ApplicationVersion (..),
    Config (..),
    configNew,
    configFromEnv,
    validateConfig,
    Environment (..),
    DBOS,
    newDBOS,
    Executor,
    isLaunched,
    launch,
    launchWithEnvironment,
    shutdown,
    registerWorkflow,
    registerWorkflowRef,

  -- * Workflow (workflow-): bodies, starts, handles, observations, control
  -- workflow-background: registration identity, starts, children
    WorkflowName (..),
    WorkflowKey (..),
    newWorkflowKey,
    WorkflowRef,
    WorkflowCtx,
    runWorkflow,
    runWorkflowRef,
    startWorkflow,
    startChildWorkflow,
    awaitChild,

  -- error channel: returned by every body (no dedicated ref)
    Error (..),
    EngineOnly,
    DurableError,

  -- workflow-introspection: ids, records, statuses, handles, fan-out
    WorkflowId (..),
    WorkflowStatus (..),
    isTerminal,
    workflowStatusText,
    workflowId,
    retrieveWorkflow,
    listWorkflowSteps,
    listWorkflowStepsInWorkflow,
    listWorkflows,
    listWorkflowsInWorkflow,
    listWorkflowIdsByName,
    fetchWorkflowStatuses,
    getWorkflowStatus,
    StepRecord (..),
    WorkflowHandle (..),
    pollingHandle,
    handleStatus,
    handleResult,
    pendingAwait,
    selectWorkflow,
    joinWorkflows,
    waitForWorkflow,
    waitForFirstWorkflow,
    waitForWorkflows,

  -- workflow-timeout: budgets and deadlines
    Timeout (..),
    RunOptions (..),
    StartOptions (..),
    runOptionsDefault,
    startOptionsDefault,

  -- workflow-control: cancel, resume, delete, fork, attributes
    cancelWorkflow,
    cancelWorkflows,
    cancelWorkflowsInWorkflow,
    resumeWorkflow,
    resumeWorkflows,
    resumeWorkflowsInWorkflow,
    deleteWorkflow,
    deleteWorkflows,
    deleteWorkflowsInWorkflow,
    ForkPoint (..),
    ForkOptions (..),
    defaultForkOptions,
    forkWorkflow,
    forkWorkflows,
    forkWorkflowsInWorkflow,
    forkFrom,
    forkFromInWorkflow,
    RewindOptions (..),
    defaultRewindOptions,
    rewindWorkflow,
    rewindWorkflows,
    rewindWorkflowsInWorkflow,
    updateWorkflowAttributes,
    updateWorkflowAttributesInWorkflow,

  -- durable races (select.rs; arms are pendings, cf. fan-out above; no dedicated ref)
    SelectArm (..),
    Winner (..),
    selectStep,

  -- * Step (step-): durable units of work
  -- step-basics
    StepError (..),
    StepCtx,
    runStep,
    runNestedStep,
    pendingStep,
    pendingStepWith,
    PendingStep,

  -- step-retries, step-timeouts
    runStepWith,
    stepOptionsDefault,
    StepOptions (..),
    stepCtxCancellationToken,

  -- step-transactions (datasource.rs — port's own seam, ADR-0021)
    IsolationLevel (..),
    TransactionConfig (..),
    transactionConfigDefault,
    Tx (..),
    DataSource (..),
    runTxStep,
    runTxOutside,
    AppDataSource,
    acquireAppDataSource,
    acquireAppDataSourceIn,
    acquireAppDataSourceInFromEnv,
    releaseAppDataSource,
    verifyAppDataSource,
    runAppSession,
    toDataSource,
    registerDataSource,

  -- * Queue: concurrency control for workflows
  -- queue-basics
    Queue (..),
    QueueOptions (..),
    defaultQueueOptions,
    registerQueue,
    enqueueWorkflow,

  -- queue-management: inspect and change queues at runtime
    queue,
    listQueues,
    updateQueue,
    QueueChange (..),
    Change (..),
    defaultQueueChange,
    deleteQueue,
    QueueConflict (..),

  -- queue-deduplication, queue-delay: the Enqueue shape
    Enqueue (..),
    DuplicationPolicy (..),
    enqueueNew,
    WorkflowDelay (..),
    setWorkflowDelay,
    setWorkflowDelayInWorkflow,

  -- * Communication: events and messages between workflows
  -- comm-events
    setEvent,
    getEvent,
    pendingGetEvent,
    pendingSetEvent,
    getWorkflowEvent,

  -- comm-messages
    Topic (..),
    IdempotencyKey (..),
    Message (..),
    Forks (..),
    SendOptions (..),
    sendOptionsDefault,
    SendBulkOptions (..),
    sendBulkOptionsDefault,
    SendMessage (..),
    send,
    sendWith,
    sendBulk,
    sendBulkWith,
    sendWorkflowMessage,
    sendWorkflowMessages,
    recv,

  -- * Pattern (pattern-): composed durable shapes
  -- pattern-sleep
    Duration,
    millisDuration,
    secondsDuration,
    sleepStep,
    pendingSleep,
    sleepPlain,

  -- pattern-idempotency: WorkflowId, StartOptions.startWorkflowId (see Workflow)

  -- pattern-debouncing (TypeScript Debouncer; no Rust counterpart, ADR-0028)
    Debouncer (..),
    debouncerNew,
    debounce,
    debounceInWorkflow,

  -- * Durable streams
    StreamReader,
    ReadStreamOptions (..),
    defaultReadStreamOptions,
    readStream,
    readStreamFrom,
    writeStream,
    closeStream,
    toVectorStream,
    foldVectorStream,

  -- * Testing (test-): test-setup describes process, not API — no dedicated facade entries
  -- * Client (client-): external access without launch
  -- client-setup (mirrors the executor surface)
    Client (..),
    ClientConfig (..),
    clientConfigNew,
    clientConfigFromEnv,
    validateClientConfig,
    clientOutcomePollInterval,
    connectClient,
    closeClient,
    withClientTransaction,
    clientRetrieveWorkflow,
    clientWorkflowStatus,
    clientWaitFirst,
    clientSendMessage,
    clientSendMessages,
    clientSendMessageInTx,
    clientSendMessagesInTx,
    clientGetEvent,
    clientCancelWorkflows,
    clientResumeWorkflows,
    clientDeleteWorkflows,
    clientForkWorkflows,
    clientRewindWorkflows,
    clientUpdateWorkflowAttributes,
    clientSetWorkflowDelay,
    clientListQueues,
    clientListQueuedWorkflows,
    clientReadStream,
    clientReadStreamFrom,
    clientRegisterQueue,
    clientRetrieveQueue,
    clientUpdateQueue,
    clientDeleteQueue,
    clientListWorkflows,
    clientListWorkflowSteps,
    clientListApplicationVersions,
    clientLatestApplicationVersion,
    clientPromoteVersion,

  -- client-enqueue
    EnqueueOptions (..),
    enqueueOptionsNew,
    enqueueOptionsOn,
    clientEnqueueWorkflow,
    clientEnqueueWorkflowInTx,
    clientEnqueueWorkflowWith,

  -- Serialization
    CodecError (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    decodeWorkflowValue,
    encodeWorkflowValue,
    Serializer (..),
    serializerName,

  -- domain-event tracing (app use-path: severity-tagged lines through a body's context)
    logDebug,
    logInfo,
    logWarn,
    logError,
  )
where

import DBOS.Transact.Checkpoint (PendingStep)
import DBOS.Transact.Client (Client (..), ClientConfig (..), EnqueueOptions (..), clientCancelWorkflows, clientConfigFromEnv, clientConfigNew, clientDeleteQueue, clientDeleteWorkflows, clientForkWorkflows, clientGetEvent, clientLatestApplicationVersion, clientListApplicationVersions, clientListQueues, clientListQueuedWorkflows, clientListWorkflowSteps, clientListWorkflows, clientOutcomePollInterval, clientPromoteVersion, clientReadStream, clientReadStreamFrom, clientRegisterQueue, clientRetrieveQueue, clientResumeWorkflows, clientRewindWorkflows, clientSetWorkflowDelay, clientSendMessage, clientSendMessageInTx, clientSendMessages, clientSendMessagesInTx, clientUpdateQueue, clientUpdateWorkflowAttributes, closeClient, connectClient, clientEnqueueWorkflow, clientEnqueueWorkflowInTx, clientEnqueueWorkflowWith, enqueueOptionsNew, enqueueOptionsOn, clientRetrieveWorkflow, validateClientConfig, withClientTransaction, clientWorkflowStatus, clientWaitFirst)
import DBOS.Transact.Config (Config (..), Serializer (..), configFromEnv, configNew, serializerName, validateConfig)
import DBOS.Transact.Connection ()
import DBOS.Transact.Context (StepCtx, WorkflowCtx, stepCtxCancellationToken, workflowId)
import DBOS.Transact.Datasource (DataSource (..), IsolationLevel (..), TransactionConfig (..), Tx (..), runTxOutside, runTxStep, transactionConfigDefault)
import DBOS.Transact.Datasource.Postgres (AppDataSource, acquireAppDataSource, acquireAppDataSourceIn, acquireAppDataSourceInFromEnv, releaseAppDataSource, runAppSession, toDataSource, verifyAppDataSource)
import DBOS.Transact.Debouncer (Debouncer (..), debounce, debounceInWorkflow, debouncerNew)
import DBOS.Transact.Error (DurableError, EngineOnly, Error (..))
import DBOS.Transact.Event (getEvent, pendingGetEvent, pendingSetEvent, setEvent)
import DBOS.Transact.Handle (WorkflowHandle (..), awaitChild, handleResult, handleStatus, pendingAwait, pollingHandle)
import DBOS.Transact.Identity (Environment (..))
import DBOS.Transact.Instance (DBOS, Executor, cancelWorkflow, cancelWorkflows, deleteWorkflow, deleteWorkflows, enqueueWorkflow, fetchWorkflowStatuses, forkFrom, forkWorkflow, forkWorkflows, getWorkflowEvent, getWorkflowStatus, isLaunched, launch, launchWithEnvironment, listWorkflowIdsByName, listWorkflowSteps, listWorkflows, newDBOS, registerDataSource, registerWorkflow, registerWorkflowRef, resumeWorkflow, resumeWorkflows, retrieveWorkflow, rewindWorkflow, rewindWorkflows, runWorkflow, runWorkflowRef, sendWorkflowMessage, sendWorkflowMessages, setWorkflowDelay, shutdown, startWorkflow, updateWorkflowAttributes)
import DBOS.Transact.Logger (logDebug, logError, logInfo, logWarn)
import DBOS.Transact.Management (cancelWorkflowsInWorkflow, deleteWorkflowsInWorkflow, forkFromInWorkflow, forkWorkflowsInWorkflow, listWorkflowStepsInWorkflow, listWorkflowsInWorkflow, resumeWorkflowsInWorkflow, rewindWorkflowsInWorkflow, setWorkflowDelayInWorkflow, updateWorkflowAttributesInWorkflow)
import DBOS.Transact.Message (Forks (..), Message (..), SendBulkOptions (..), SendOptions (..), recv, send, sendBulk, sendBulkOptionsDefault, sendBulkWith, sendOptionsDefault, sendWith)
import DBOS.Transact.Queue (Queue (..), QueueChange (..), QueueConflict (..), QueueOptions (..), defaultQueueChange, defaultQueueOptions, deleteQueue, listQueues, queue, registerQueue, updateQueue)
import DBOS.Transact.Registry (WorkflowKey (..), WorkflowRef, newWorkflowKey)
import DBOS.Transact.Select (SelectArm (..), Winner (..), selectStep)
import DBOS.Transact.Serialization (CodecError (..), decodeWorkflowValue, encodeWorkflowValue)
import DBOS.Transact.Sleep (pendingSleep, sleepPlain, sleepStep)
import DBOS.Transact.Step (StepError (..), StepOptions (..), pendingStep, pendingStepWith, runNestedStep, runStep, runStepWith, stepOptionsDefault)
import DBOS.Transact.Stream (ReadStreamOptions (..), StreamReader, closeStream, defaultReadStreamOptions, foldVectorStream, readStream, readStreamFrom, toVectorStream, writeStream)
import DBOS.Transact.Wait (joinWorkflows, selectWorkflow, waitForFirstWorkflow, waitForWorkflow, waitForWorkflows)
import DBOS.Transact.Workflow (DuplicationPolicy (..), Enqueue (..), RunOptions (..), StartOptions (..), Timeout (..), enqueueNew, runOptionsDefault, startChildWorkflow, startOptionsDefault)

import DBOS.SystemDB.Types (ApplicationVersion (..), Change (..), Duration, ForkOptions (..), ForkPoint (..), IdempotencyKey (..), RewindOptions (..), SendMessage (..), Serialization (..), SerializedWorkflowValue (..), StepRecord (..), Topic (..), WorkflowDelay (..), WorkflowId (..), WorkflowName (..), WorkflowStatus (..), defaultForkOptions, defaultRewindOptions, isTerminal, millisDuration, secondsDuration, workflowStatusText)
