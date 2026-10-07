module DBOS.Transact
  ( -- * Workflow executions
    ApplicationVersion (..),
    Duration (..),
    millisDuration,
    secondsDuration,
    WorkflowDelay (..),
    WorkflowId (..),
    WorkflowName (..),
    WorkflowStatus (..),
    isTerminal,
    workflowStatusText,

    -- * Durable steps
    StepError (..),
    runStep,
    runNestedStep,
    pendingStep,
    pendingStepWith,
    runStepWith,
    stepOptionsDefault,
    StepOptions (..),
    sleepStep,
    pendingSleep,
    sleepPlain,
    setEvent,
    getEvent,
    pendingGetEvent,
    pendingSetEvent,
    Message (..),
    Forks (..),
    SendOptions (..),
    sendOptionsDefault,
    SendBulkOptions (..),
    sendBulkOptionsDefault,
    send,
    sendWith,
    sendBulk,
    sendBulkWith,
    recv,

    -- * Transactional steps (datasource.rs — port's own seam, ADR-0021)
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

    -- * Durable value codec
    CodecError (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    decodeWorkflowValue,
    encodeWorkflowValue,

    -- * Domain-event tracing (no logging library)
    Tracer,
    SomeTracer (..),
    nullTracer,
    runTracer,
    LoggerBackend (..),
    acquireLoggerBackend,
    fastLoggerTracer,
    ioTracer,

    -- * Configuration and identity (config.rs, identity.rs)
    Config (..),
    Serializer (..),
    serializerName,
    configNew,
    configFromEnv,
    validateConfig,
    Environment (..),
    -- * Instance lifecycle (instance.rs)
    DBOS,
    newDBOS,
    Executor,
    isLaunched,
    launch,
    launchWithEnvironment,
    shutdown,
    registerDBOSWorkflow,
    registerDBOSWorkflowRef,
    registerDBOSDataSource,
    runDBOSWorkflow,
    startDBOSWorkflowRef,
    runDBOSWorkflowRef,
    enqueueDBOSWorkflow,
    retrieveWorkflow,
    getWorkflowEvent,
    sendWorkflowMessage,
    sendWorkflowMessages,
    listWorkflowIdsByName,
    fetchWorkflowStatuses,
    cancelWorkflows,
    cancelWorkflowsInWorkflow,
    resumeWorkflows,
    resumeWorkflowsInWorkflow,
    setWorkflowDelay,
    deleteWorkflows,
    deleteWorkflowsInWorkflow,
    forkWorkflows,
    forkWorkflowsInWorkflow,
    forkFrom,
    forkFromInWorkflow,
    updateWorkflowAttributes,
    listWorkflows,
    listWorkflowsInWorkflow,
    selectWorkflow,
    joinWorkflows,
    waitForWorkflow,
    waitForFirstWorkflow,
    waitForWorkflows,
    -- * Workflow handles (handle.rs)
    WorkflowHandle (..),
    pollingHandle,
    handleStatus,
    handleResult,
    awaitChild,
    pendingAwait,
    -- * Durable races (select.rs)
    SelectArm (..),
    Winner (..),
    selectStep,
    -- * Client (client.rs)
    Client (..),
    ClientConfig (..),
    clientConfigNew,
    clientConfigFromEnv,
    validateClientConfig,
    clientOutcomePollInterval,
    connectClient,
    closeClient,
    EnqueueOptions (..),
    enqueueOptionsNew,
    enqueueOptionsOn,
    enqueueClientWorkflow,
    enqueueClientWorkflowWith,
    retrieveClientWorkflow,
    workflowStatusClient,
    clientSendMessage,
    clientSendMessages,
    clientGetEvent,
    clientCancelWorkflows,
    clientResumeWorkflows,
    clientDeleteWorkflows,
    clientForkWorkflows,
    clientListApplicationVersions,
    clientLatestApplicationVersion,
    clientPromoteVersion,
    clientListWorkflows,
    -- * Connection (connection.rs): engine-internal, no client entry.
    -- * Scoped workflow contexts
    WorkflowCtx,
    StepCtx,
    workflowId,
    stepCtxCancellationToken,
    -- * Workflow registry and runner
    Error (..),
    EngineOnly,
    DurableError,
    application,
    WorkflowKey (..),
    newWorkflowKey,
    WorkflowRef,
    -- The registry run/enqueue/start primitives take a raw 'Connection'/'Tasks'
    -- and are engine-internal: the facade exposes the 'Executor'/'DBOS'
    -- wrappers above and the 'WorkflowCtx'-taking scoped entries below.
    -- (C5c: no facade entry hands a bare connection to a start/enqueue path.)
    Enqueue (..),
    DuplicationPolicy (..),
    enqueueNew,
    Timeout (..),
    RunOptions (..),
    StartOptions (..),
    runOptionsDefault,
    startOptionsDefault,
    startChildWorkflow,
    -- * Task ownership (workflow.rs @Tasks@)
    Queue (..),
    QueueOptions (..),
    QueueChange (..),
    QueueConflict (..),
    defaultQueueOptions,
    registerQueue,
    queue,
    listQueues,
    updateQueue,
    deleteQueue,

    -- * Workflow messages
    IdempotencyKey (..),
    SendMessage (..),
    Topic (..),
  )
where

import DBOS.Tracer (LoggerBackend (..), SomeTracer (..), Tracer, acquireLoggerBackend, fastLoggerTracer, ioTracer, nullTracer, runTracer)
import DBOS.Transact.Client (Client (..), ClientConfig (..), EnqueueOptions (..), clientCancelWorkflows, clientConfigFromEnv, clientConfigNew, clientDeleteWorkflows, clientForkWorkflows, clientGetEvent, clientLatestApplicationVersion, clientListApplicationVersions, clientListWorkflows, clientOutcomePollInterval, clientPromoteVersion, clientResumeWorkflows, clientSendMessage, clientSendMessages, closeClient, connectClient, enqueueClientWorkflow, enqueueClientWorkflowWith, enqueueOptionsNew, enqueueOptionsOn, retrieveClientWorkflow, validateClientConfig, workflowStatusClient)
import DBOS.Transact.Config (Config (..), Serializer (..), configFromEnv, configNew, serializerName, validateConfig)
import DBOS.Transact.Connection ()
import DBOS.Transact.Context (StepCtx, WorkflowCtx, stepCtxCancellationToken, workflowId)
import DBOS.Transact.Datasource (DataSource (..), IsolationLevel (..), TransactionConfig (..), Tx (..), runTxOutside, runTxStep, transactionConfigDefault)
import DBOS.Transact.Datasource.Postgres (AppDataSource, acquireAppDataSource, acquireAppDataSourceIn, acquireAppDataSourceInFromEnv, releaseAppDataSource, runAppSession, toDataSource, verifyAppDataSource)
import DBOS.Transact.Error (DurableError, EngineOnly, Error (..), application)
import DBOS.Transact.Event (getEvent, pendingGetEvent, pendingSetEvent, setEvent)
import DBOS.Transact.Handle (WorkflowHandle (..), awaitChild, handleResult, handleStatus, pendingAwait, pollingHandle)
import DBOS.Transact.Identity (Environment (..))
import DBOS.Transact.Instance (DBOS, Executor, cancelWorkflows, deleteWorkflows, enqueueDBOSWorkflow, fetchWorkflowStatuses, forkFrom, forkWorkflows, getWorkflowEvent, isLaunched, launch, launchWithEnvironment, listWorkflowIdsByName, listWorkflows, newDBOS, registerDBOSDataSource, registerDBOSWorkflow, registerDBOSWorkflowRef, resumeWorkflows, retrieveWorkflow, runDBOSWorkflow, runDBOSWorkflowRef, sendWorkflowMessage, sendWorkflowMessages, setWorkflowDelay, shutdown, startDBOSWorkflowRef, updateWorkflowAttributes)
import DBOS.Transact.Management (cancelWorkflowsInWorkflow, deleteWorkflowsInWorkflow, forkFromInWorkflow, forkWorkflowsInWorkflow, listWorkflowsInWorkflow, resumeWorkflowsInWorkflow)
import DBOS.Transact.Message (Forks (..), Message (..), SendBulkOptions (..), SendOptions (..), recv, send, sendBulk, sendBulkOptionsDefault, sendBulkWith, sendOptionsDefault, sendWith)
import DBOS.Transact.Queue (Queue (..), QueueChange (..), QueueConflict (..), QueueOptions (..), defaultQueueOptions, deleteQueue, listQueues, queue, registerQueue, updateQueue)
import DBOS.Transact.Registry (WorkflowKey (..), WorkflowRef, newWorkflowKey)
import DBOS.Transact.Select (SelectArm (..), Winner (..), selectStep)
import DBOS.Transact.Serialization (CodecError (..), decodeWorkflowValue, encodeWorkflowValue)
import DBOS.Transact.Sleep (pendingSleep, sleepPlain, sleepStep)
import DBOS.Transact.Step (StepError (..), StepOptions (..), pendingStep, pendingStepWith, runNestedStep, runStep, runStepWith, stepOptionsDefault)
import DBOS.Transact.Wait (joinWorkflows, selectWorkflow, waitForFirstWorkflow, waitForWorkflow, waitForWorkflows)
import DBOS.Transact.Workflow (DuplicationPolicy (..), Enqueue (..), RunOptions (..), StartOptions (..), Timeout (..), enqueueNew, runOptionsDefault, startChildWorkflow, startOptionsDefault)

import DBOS.SystemDB.Types (ApplicationVersion (..), Duration (..), IdempotencyKey (..), SendMessage (..), Serialization (..), SerializedWorkflowValue (..), Topic (..), WorkflowDelay (..), WorkflowId (..), WorkflowName (..), WorkflowStatus (..), isTerminal, millisDuration, secondsDuration, workflowStatusText)
