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

package org.apache.gobblin.temporal.joblauncher;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.temporal.api.enums.v1.WorkflowExecutionStatus;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowStub;
import io.temporal.workflow.Workflow;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.event.ClusterManagerShutdownRequest;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.temporal.cluster.GobblinTemporalTaskRunner;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.workflows.service.ManagedWorkflowServiceStubs;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys.*;
import static org.apache.gobblin.temporal.workflows.client.TemporalWorkflowClientFactory.createClientInstance;
import static org.apache.gobblin.temporal.workflows.client.TemporalWorkflowClientFactory.createServiceInstance;

/**
 * An implementation of {@link JobLauncher} that launches a Gobblin job using the Temporal task framework.
 *
 * <p>
 *   Each {@link WorkUnit} of the job is persisted to the {@link FileSystem} of choice and the path to the file
 *   storing the serialized {@link WorkUnit} is passed to the Temporal task running the {@link WorkUnit} as a
 *   user-defined property {@link GobblinClusterConfigurationKeys#WORK_UNIT_FILE_PATH}. Upon startup, the gobblin
 *   task reads the property for the file path and de-serializes the {@link WorkUnit} from the file.
 * </p>
 *
 * <p>
 *   This class is instantiated by the {@link GobblinTemporalJobScheduler} on every job submission to launch the Gobblin job.
 *   The actual task execution happens in the {@link GobblinTemporalTaskRunner}, usually in a different process.
 * </p>
 */
@Alpha
public abstract class GobblinTemporalJobLauncher extends GobblinJobLauncher {
  private static final Logger log = Workflow.getLogger(GobblinTemporalJobLauncher.class);
  private static final int TERMINATION_TIMEOUT_SECONDS = 3;

  @VisibleForTesting
  static final String WORKFLOW_ID_METADATA_FIELD = "workflowId";
  @VisibleForTesting
  static final String WORKFLOW_STATUS_METADATA_FIELD = "workflowStatus";
  @VisibleForTesting
  static final String FAILURE_REASON_METADATA_FIELD = "failureReason";
  @VisibleForTesting
  static final String AM_TERMINATED_DURING_EXECUTION_REASON = "AM_TERMINATED_DURING_EXECUTION";

  protected ManagedWorkflowServiceStubs managedWorkflowServiceStubs;
  protected WorkflowClient client;
  protected String queueName;
  protected String namespace;
  protected String workflowId;

  private final AtomicBoolean jobCompletionGTEEmitted = new AtomicBoolean(false);

  public GobblinTemporalJobLauncher(Properties jobProps, Path appWorkDir,
                                    List<? extends Tag<?>> metadataTags, ConcurrentHashMap<String, Boolean> runningMap, EventBus eventBus)
          throws Exception {
    super(jobProps, appWorkDir, metadataTags, runningMap, eventBus);
    log.info("GobblinTemporalJobLauncher: appWorkDir {}; jobProps {}", appWorkDir, jobProps);

    String connectionUri = jobProps.getProperty(TEMPORAL_CONNECTION_STRING);
    this.managedWorkflowServiceStubs = createServiceInstance(connectionUri);

    this.namespace = jobProps.getProperty(GOBBLIN_TEMPORAL_NAMESPACE, DEFAULT_GOBBLIN_TEMPORAL_NAMESPACE);
    this.client = createClientInstance(managedWorkflowServiceStubs.getWorkflowServiceStubs(), namespace);

    this.queueName = jobProps.getProperty(GOBBLIN_TEMPORAL_TASK_QUEUE, DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE);

    // non-null value indicates job has been submitted
    this.workflowId = null;
    startCancellationExecutor();
    registerJobCompletionGTEHook();
  }

  /**
   * Register a JVM shutdown hook that, on AM exit, queries Temporal for the workflow's terminal state and
   * emits a single {@link org.apache.gobblin.metrics.GobblinTrackingEvent} capturing the outcome. Mirrors the
   * {@code registerCleanupShutdownHook} pattern in {@link GobblinJobLauncher}. Because in temporal-on-yarn each
   * Yarn application launches exactly one workflow (see {@link #handleLaunchFinalization}), AM termination
   * coincides with job completion, so this hook is the single source of truth for job-completion GTEs.
   */
  private void registerJobCompletionGTEHook() {
    Runtime.getRuntime().addShutdownHook(
        new Thread(this::emitJobCompletionGTE,
            "GobblinTemporalJobLauncher-JobCompletionGTE-" + this.jobContext.getJobId()));
  }

  /**
   * Fetch the workflow's terminal {@link WorkflowExecutionStatus} from Temporal, translate it to the corresponding
   * {@link TimingEvent.LauncherTimings} event name, and submit the GTE via the inherited {@code eventSubmitter}.
   * Idempotency-guarded so multiple shutdown triggers result in a single emission.
   */
  @VisibleForTesting
  void emitJobCompletionGTE() {
    if (!jobCompletionGTEEmitted.compareAndSet(false, true)) {
      return;
    }
    if (this.workflowId == null) {
      // submitJob was never invoked on this launcher; nothing to report.
      return;
    }
    try {
      WorkflowExecutionStatus status = fetchWorkflowStatus();
      String eventName = mapWorkflowStatusToEventName(status);
      Map<String, String> metadata = buildCompletionMetadata(status);
      new TimingEvent(this.eventSubmitter, eventName).stop(metadata);
      log.info("Emitted job completion GTE {} for workflow {} (Temporal status {})",
          eventName, this.workflowId, status);
    } catch (Exception e) {
      log.error("Failed to emit job completion GTE for workflow " + this.workflowId, e);
    }
  }

  /**
   * Query Temporal for the current execution status of {@link #workflowId}. Returns
   * {@code WORKFLOW_EXECUTION_STATUS_UNSPECIFIED} as a safe fallback if the describe call fails, so callers
   * downstream emit a JOB_FAILED rather than swallowing the GTE entirely.
   */
  private WorkflowExecutionStatus fetchWorkflowStatus() {
    try {
      WorkflowStub workflowStub = this.client.newUntypedWorkflowStub(this.workflowId);
      DescribeWorkflowExecutionRequest request = DescribeWorkflowExecutionRequest.newBuilder()
          .setNamespace(this.namespace)
          .setExecution(workflowStub.getExecution())
          .build();
      DescribeWorkflowExecutionResponse response = managedWorkflowServiceStubs.getWorkflowServiceStubs()
          .blockingStub().describeWorkflowExecution(request);
      return response.getWorkflowExecutionInfo().getStatus();
    } catch (Exception e) {
      log.warn("Failed to describe workflow {} for completion GTE; treating as UNSPECIFIED (will emit JOB_FAILED)",
          this.workflowId, e);
      return WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED;
    }
  }

  /**
   * Build the GTE metadata carrying flow/job identifiers (required by {@code KafkaAvroJobStatusMonitor.acceptEvent},
   * which drops events lacking flow group/name/executionId), plus diagnostic fields for the workflow status.
   * When the workflow is still RUNNING at AM shutdown, mark a synthetic failure reason so downstream consumers can
   * distinguish AM-killed-mid-execute from a genuine workflow failure.
   */
  private Map<String, String> buildCompletionMetadata(WorkflowExecutionStatus status) {
    Map<String, String> metadata = new HashMap<>();
    metadata.put(WORKFLOW_ID_METADATA_FIELD, this.workflowId);
    metadata.put(WORKFLOW_STATUS_METADATA_FIELD, status.name());
    addFlowMetadataIfPresent(metadata, TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, ConfigurationKeys.FLOW_GROUP_KEY);
    addFlowMetadataIfPresent(metadata, TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, ConfigurationKeys.FLOW_NAME_KEY);
    addFlowMetadataIfPresent(metadata, TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
    addFlowMetadataIfPresent(metadata, TimingEvent.FlowEventConstants.JOB_NAME_FIELD, ConfigurationKeys.JOB_NAME_KEY);
    addFlowMetadataIfPresent(metadata, TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, ConfigurationKeys.JOB_GROUP_KEY);
    if (isNonTerminal(status)) {
      metadata.put(FAILURE_REASON_METADATA_FIELD, AM_TERMINATED_DURING_EXECUTION_REASON);
    }
    return metadata;
  }

  private void addFlowMetadataIfPresent(Map<String, String> metadata, String metadataKey, String jobPropKey) {
    String value = this.jobProps.getProperty(jobPropKey);
    if (value != null) {
      metadata.put(metadataKey, value);
    }
  }

  /**
   * Map a Temporal {@link WorkflowExecutionStatus} to the {@link TimingEvent.LauncherTimings} event name that
   * {@code KafkaAvroJobStatusMonitor.parseJobStatus} understands. Non-terminal statuses (RUNNING,
   * CONTINUED_AS_NEW, UNSPECIFIED) collapse to JOB_FAILED — the AM is going down, so from the GaaS perspective
   * the job did not complete successfully.
   */
  @VisibleForTesting
  static String mapWorkflowStatusToEventName(WorkflowExecutionStatus status) {
    switch (status) {
      case WORKFLOW_EXECUTION_STATUS_COMPLETED:
        return TimingEvent.LauncherTimings.JOB_SUCCEEDED;
      case WORKFLOW_EXECUTION_STATUS_CANCELED:
        return TimingEvent.LauncherTimings.JOB_CANCEL;
      case WORKFLOW_EXECUTION_STATUS_FAILED:
      case WORKFLOW_EXECUTION_STATUS_TERMINATED:
      case WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
      case WORKFLOW_EXECUTION_STATUS_RUNNING:
      case WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
      case WORKFLOW_EXECUTION_STATUS_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        return TimingEvent.LauncherTimings.JOB_FAILED;
    }
  }

  private static boolean isNonTerminal(WorkflowExecutionStatus status) {
    return status == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING
        || status == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
        || status == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED
        || status == WorkflowExecutionStatus.UNRECOGNIZED;
  }

  /** @return {@link Config} now featuring all overrides rooted at {@link GobblinTemporalConfigurationKeys#GOBBLIN_TEMPORAL_JOB_LAUNCHER_CONFIG_OVERRIDES} */
  protected Config applyJobLauncherOverrides(Config config) {
    Config configOverrides = ConfigUtils.getConfig(config,
        GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_JOB_LAUNCHER_CONFIG_OVERRIDES, ConfigFactory.empty());
    log.info("appying config overrides: {}", configOverrides);
    return configOverrides.withFallback(config);
  }

  @Override
  protected void handleLaunchFinalization() {
    // NOTE: This code only makes sense when there is 1 source / workflow being launched per application for Temporal. This is a stop-gap
    // for achieving batch job behavior. Given the current constraints of yarn applications requiring a static proxy user
    // during application creation, it is not possible to have multiple workflows running in the same application.
    // and so it makes sense to just kill the job after this is completed
    log.info("Requesting the AM to shutdown after the job {} completed", this.jobContext.getJobId());
    eventBus.post(new ClusterManagerShutdownRequest());
  }

  /**
   * Submit a job to run.
   */
  @Override
  abstract protected void submitJob(List<WorkUnit> workUnits) throws Exception;

  @Override
  protected void executeCancellation() {
    if (this.workflowId == null) {
      log.info("Cancellation of temporal workflow attempted without submitting it");
      return;
    }

    log.info("Cancelling temporal workflow {}", this.workflowId);
    try {
      WorkflowStub workflowStub = this.client.newUntypedWorkflowStub(this.workflowId);

      // Describe the workflow execution to get its status
      DescribeWorkflowExecutionRequest request = DescribeWorkflowExecutionRequest.newBuilder()
          .setNamespace(this.namespace)
          .setExecution(workflowStub.getExecution())
          .build();
      DescribeWorkflowExecutionResponse response = managedWorkflowServiceStubs.getWorkflowServiceStubs()
          .blockingStub().describeWorkflowExecution(request);

      WorkflowExecutionStatus status;
      try {
        status = response.getWorkflowExecutionInfo().getStatus();
      } catch (Exception e) {
        log.warn("Exception occurred while getting status of the workflow " + this.workflowId
            + ". We would still attempt the cancellation", e);
        workflowStub.cancel();
        log.info("Temporal workflow {} cancelled successfully", this.workflowId);
        return;
      }

      // Check if the workflow is not finished
      if (status != WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED &&
          status != WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED &&
          status != WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CANCELED &&
          status != WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TERMINATED) {
        workflowStub.cancel();
        try {
          // Check workflow status, if it is cancelled, will throw WorkflowFailedException else TimeoutException
          workflowStub.getResult(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS, String.class, String.class);
        } catch (TimeoutException te) {
          // Workflow is still running, terminate it.
          log.info("Workflow is still running, will attempt termination", te);
          workflowStub.terminate("Job cancel invoked");
        } catch (WorkflowFailedException wfe) {
          // Do nothing as exception is expected.
        }
        log.info("Temporal workflow {} cancelled successfully", this.workflowId);
      } else {
        log.info("Workflow {} is already finished with status {}", this.workflowId, status);
      }
    } catch (Exception e) {
      log.error("Exception occurred while cancelling the workflow " + this.workflowId, e);
    }
  }

  /** No-op: merely logs a warning, since not expected to be invoked */
  @Override
  protected void removeTasksFromCurrentJob(List<String> workUnitIdsToRemove) {
    log.warn("NOT IMPLEMENTED: Temporal removeTasksFromCurrentJob");
  }

  /** No-op: merely logs a warning, since not expected to be invoked */
  @Override
  protected void addTasksToCurrentJob(List<WorkUnit> workUnitsToAdd) {
    log.warn("NOT IMPLEMENTED: Temporal addTasksToCurrentJob");
  }

  @Override
  public void close() throws IOException {
    try {
      // Calling cancel before calling close on serviceStubs as it will shutdown the service which is required during cancellation.
      cancelJob(jobListener);
    } catch (Exception e) {
      log.error("Exception occurred while cancelling job", e);
    } finally {
      managedWorkflowServiceStubs.close();
      super.close();
    }
  }
}
