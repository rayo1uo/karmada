/*
Copyright 2021 The Karmada Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package binding

import (
	"context"
	"strconv"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	configv1alpha1 "github.com/karmada-io/karmada/pkg/apis/config/v1alpha1"
	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/controllers/ctrlutil"
	"github.com/karmada-io/karmada/pkg/features"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/helper"
	"github.com/karmada-io/karmada/pkg/util/names"
	"github.com/karmada-io/karmada/pkg/util/overridemanager"
)

const (
	// requeueIntervalForDirectlyPurge is the requeue interval for binding when there are works in clusters with PurgeMode 'Directly'.
	requeueIntervalForDirectlyPurge = 5 * time.Second
)

/*
1. 获取目标集群列表 (mergeTargetClusters)
  在循环开始前，它会调用 mergeTargetClusters 整合所有需要处理的集群。这包括
  spec.Clusters（调度器分配的主要集群）和
  spec.RequiredBy（因资源依赖关系而必须包含的集群）。

  2. 遍历每一个目标集群
  函数的核心是一个 for 循环，为 targetClusters 列表中的每个集群生成一个专属的
  Work。

  3. 创建资源副本 (workload.DeepCopy())
  这是循环内部至关重要的第一步。它创建了原始资源模板（workload）的一个深拷贝。
  目的：确保对一个集群（如
  cluster-A）的定制化修改（如修改副本数、镜像版本）不会污染原始模板，从而影响到下
  一个集群（如 cluster-B）的处理。每个集群的定制化都是在一个干净的副本上进行的。

  4. 副本数修正 (resourceInterpreter.ReviseReplica)
   * 场景：当 PropagationPolicy 的 replicaScheduling 策略为
     Divided（拆分模式）时，调度器会计算出每个集群应分配的副本数，并记录在
     ResourceBinding 的 spec.clusters[].replicas 字段中。
   * 操作：resourceInterpreter.ReviseReplica 函数会智能地找到 clonedWorkload
     中的副本数字段（如 Deployment 的 spec.replicas），并将其值修改为当前
     targetCluster 被分配到的具体副本数。
   * 这是实现副本数按权重拆分的关键步骤。

  5. 应用差异化策略 (overrideManager.ApplyOverridePolicies)
   * 这是实现集群差异化配置的核心步骤，也是 ensureWork
     中优先级最高的修改步骤（在所有其他修改之后执行）。
   * 操作：overrideManager 会查找所有适用于当前 clonedWorkload 和 targetCluster
     的 OverridePolicy 和 ClusterOverridePolicy。
   * 它会按照策略的优先级，将
     imageOverriders、argsOverriders、commandOverriders、plaintext
     等所有差异化规则应用到 clonedWorkload 上。
   * 例如：如果一个 OverridePolicy 定义了在 cluster-A 中使用 nginx:1.22
     镜像，那么在处理 cluster-A 的循环中，clonedWorkload 的
     spec.template.spec.containers[0].image 字段就会被修改为 nginx:1.22。

  6. 准备 Work 对象的元数据
  在 clonedWorkload 被完全定制化之后，控制器开始准备将要创建的 Work 对象的
  metadata。
   * `mergeLabel`：为 Work 对象打上标签，最重要的是
     ResourceBindingPermanentIDLabel，它像一个“身份证”，将这个 Work 与其父
     ResourceBinding 永久关联起来，便于后续的查询和管理。
   * `mergeAnnotations`：添加注解，例如记录它来自哪个 ResourceBinding。
   * `RecordAppliedOverrides`：将上一步中应用的所有 OverridePolicy 的名称记录在
     Work 的 annotations
     中。这极大地提升了系统的可观测性，让用户能清楚地知道一个最终下发的资源到底受
     到了哪些差异化策略的影响。

  7. 组装并创建/更新 Work 对象 (ctrlutil.CreateOrUpdateWork)
   * 组装：将最终的 clonedWorkload（作为 Work 的                                 ▄
     spec.workload.manifests）和上一步准备的 workMeta 组装成一个完整的 Work      █
     对象。
   * 设置命名空间：Work 对象的 namespace 被设置为
     names.GenerateExecutionSpaceName(targetCluster.Name)，即
     karmada-es-<cluster-name>。这是 Karmada 中 Work
     对象和目标集群绑定的核心约定。
   * 创建/更新：调用 CreateOrUpdateWork 辅助函数，在 Karmada
     控制面中创建或更新这个 Work 对象。
*/

// ensureWork ensure Work to be created or updated.
func ensureWork(
	ctx context.Context, c client.Client, resourceInterpreter resourceinterpreter.ResourceInterpreter, workload *unstructured.Unstructured,
	overrideManager overridemanager.OverrideManager, binding metav1.Object, scope apiextensionsv1.ResourceScope,
) error {
	bindingSpec := getBindingSpec(binding, scope)
	targetClusters := mergeTargetClusters(bindingSpec.Clusters, bindingSpec.RequiredBy)
	var err error
	var errs []error

	var jobCompletions []workv1alpha2.TargetCluster
	if workload.GetKind() == util.JobKind && needReviseJobCompletions(bindingSpec.Replicas, bindingSpec.Placement) {
		jobCompletions, err = divideReplicasByJobCompletions(workload, targetClusters)
		if err != nil {
			return err
		}
	}

	for i := range targetClusters {
		targetCluster := targetClusters[i]
		clonedWorkload := workload.DeepCopy()

		workNamespace := names.GenerateExecutionSpaceName(targetCluster.Name)

		// When syncing workloads to member clusters, the controller MUST strictly adhere to the scheduling results
		// specified in bindingSpec.Clusters for replica allocation, rather than using the replicas declared in the
		// workload's resource template.
		// This rule applies regardless of whether the workload distribution mode is "Divided" or "Duplicated".
		// Failing to do so could allow workloads to bypass the quota checks performed by the scheduler
		// (especially during scale-up operations) or skip queue validation when scheduling is suspended.
		if needReviseReplicas(bindingSpec.Replicas) {
			if resourceInterpreter.HookEnabled(clonedWorkload.GroupVersionKind(), configv1alpha1.InterpreterOperationReviseReplica) {
				clonedWorkload, err = resourceInterpreter.ReviseReplica(clonedWorkload, int64(targetCluster.Replicas))
				if err != nil {
					klog.ErrorS(err, "Failed to revise replica for workload in cluster.", "workloadKind", workload.GetKind(),
						"workloadNamespace", workload.GetNamespace(), "workloadName", workload.GetName(), "cluster", targetCluster.Name)
					errs = append(errs, err)
					continue
				}
			}
		}

		// jobSpec.Completions specifies the desired number of successfully finished pods the job should be run with.
		// When the replica scheduling policy is set to "divided", jobSpec.Completions should also be divided accordingly.
		// The weight assigned to each cluster roughly equals that cluster's jobSpec.Parallelism value. This approach helps
		// balance the execution time of the job across member clusters.
		if len(jobCompletions) > 0 {
			// Set allocated completions for Job only when the '.spec.completions' field not omitted from resource template.
			// For jobs running with a 'work queue' usually leaves '.spec.completions' unset, in that case we skip
			// setting this field as well.
			// Refer to: https://kubernetes.io/docs/concepts/workloads/controllers/job/#parallel-jobs.
			if err = helper.ApplyReplica(clonedWorkload, int64(jobCompletions[i].Replicas), util.CompletionsField); err != nil {
				klog.ErrorS(err, "Failed to apply Completions for workload in cluster.",
					"workloadKind", clonedWorkload.GetKind(), "workloadNamespace", clonedWorkload.GetNamespace(),
					"workloadName", clonedWorkload.GetName(), "cluster", targetCluster.Name)
				errs = append(errs, err)
				continue
			}
		}

		// We should call ApplyOverridePolicies last, as override rules have the highest priority
		cops, ops, err := overrideManager.ApplyOverridePolicies(clonedWorkload, targetCluster.Name)
		if err != nil {
			klog.ErrorS(err, "Failed to apply overrides for workload in cluster.",
				"workloadKind", clonedWorkload.GetKind(), "workloadNamespace", clonedWorkload.GetNamespace(),
				"workloadName", clonedWorkload.GetName(), "cluster", targetCluster.Name)
			errs = append(errs, err)
			continue
		}
		workLabel := mergeLabel(clonedWorkload, binding, scope)

		annotations := mergeAnnotations(clonedWorkload, binding, scope)
		annotations = mergeConflictResolution(clonedWorkload, bindingSpec.ConflictResolution, annotations)
		annotations, err = RecordAppliedOverrides(cops, ops, annotations)
		if err != nil {
			klog.ErrorS(err, "Failed to record appliedOverrides in cluster.", "cluster", targetCluster.Name)
			errs = append(errs, err)
			continue
		}

		if features.FeatureGate.Enabled(features.StatefulFailoverInjection) {
			// we need to figure out if the targetCluster is in the cluster we are going to migrate application to.
			// If yes, we have to inject the preserved label state to the clonedWorkload.
			clonedWorkload = injectReservedLabelState(bindingSpec, targetCluster, clonedWorkload, len(targetClusters))
		}

		workMeta := metav1.ObjectMeta{
			Name:        names.GenerateWorkName(clonedWorkload.GetKind(), clonedWorkload.GetName(), clonedWorkload.GetNamespace()),
			Namespace:   workNamespace,
			Finalizers:  []string{util.ExecutionControllerFinalizer},
			Labels:      workLabel,
			Annotations: annotations,
		}

		if err = ctrlutil.CreateOrUpdateWork(
			ctx,
			c,
			workMeta,
			clonedWorkload,
			ctrlutil.WithSuspendDispatching(shouldSuspendDispatching(bindingSpec.Suspension, targetCluster)),
			ctrlutil.WithPreserveResourcesOnDeletion(ptr.Deref(bindingSpec.PreserveResourcesOnDeletion, false)),
		); err != nil {
			errs = append(errs, err)
			continue
		}
	}

	return errors.NewAggregate(errs)
}

func getBindingSpec(binding metav1.Object, scope apiextensionsv1.ResourceScope) workv1alpha2.ResourceBindingSpec {
	var bindingSpec workv1alpha2.ResourceBindingSpec
	switch scope {
	case apiextensionsv1.NamespaceScoped:
		bindingObj := binding.(*workv1alpha2.ResourceBinding)
		bindingSpec = bindingObj.Spec
	case apiextensionsv1.ClusterScoped:
		bindingObj := binding.(*workv1alpha2.ClusterResourceBinding)
		bindingSpec = bindingObj.Spec
	}
	return bindingSpec
}

// injectReservedLabelState injects the reservedLabelState in to the failover to cluster.
// We have the following restrictions on whether to perform injection operations:
//  1. Only the scenario where an application is deployed in one cluster and migrated to
//     another cluster is considered.
//  2. If consecutive failovers occur, for example, an application is migrated form clusterA
//     to clusterB and then to clusterC, the PreservedLabelState before the last failover is
//     used for injection. If the PreservedLabelState is empty, the injection is skipped.
//  3. The injection operation is performed only when PurgeMode is set to Immediately or Directly.
func injectReservedLabelState(bindingSpec workv1alpha2.ResourceBindingSpec, moveToCluster workv1alpha2.TargetCluster, workload *unstructured.Unstructured, clustersLen int) *unstructured.Unstructured {
	if clustersLen > 1 {
		return workload
	}

	if len(bindingSpec.GracefulEvictionTasks) == 0 {
		return workload
	}
	targetEvictionTask := bindingSpec.GracefulEvictionTasks[len(bindingSpec.GracefulEvictionTasks)-1]

	//nolint:staticcheck
	// disable `deprecation` check for backward compatibility.
	if targetEvictionTask.PurgeMode != policyv1alpha1.Immediately &&
		targetEvictionTask.PurgeMode != policyv1alpha1.PurgeModeDirectly {
		return workload
	}

	clustersBeforeFailover := sets.NewString(targetEvictionTask.ClustersBeforeFailover...)
	if clustersBeforeFailover.Has(moveToCluster.Name) {
		return workload
	}

	for key, value := range targetEvictionTask.PreservedLabelState {
		util.MergeLabel(workload, key, value)
	}

	return workload
}

func mergeTargetClusters(targetClusters []workv1alpha2.TargetCluster, requiredByBindingSnapshot []workv1alpha2.BindingSnapshot) []workv1alpha2.TargetCluster {
	if len(requiredByBindingSnapshot) == 0 {
		return targetClusters
	}

	scheduledClusterNames := util.ConvertToClusterNames(targetClusters)

	for _, requiredByBinding := range requiredByBindingSnapshot {
		for _, targetCluster := range requiredByBinding.Clusters {
			if !scheduledClusterNames.Has(targetCluster.Name) {
				scheduledClusterNames.Insert(targetCluster.Name)
				targetClusters = append(targetClusters, targetCluster)
			}
		}
	}

	return targetClusters
}

func mergeLabel(workload *unstructured.Unstructured, binding metav1.Object, scope apiextensionsv1.ResourceScope) map[string]string {
	var workLabel = make(map[string]string)
	if scope == apiextensionsv1.NamespaceScoped {
		bindingID := util.GetLabelValue(binding.GetLabels(), workv1alpha2.ResourceBindingPermanentIDLabel)
		util.MergeLabel(workload, workv1alpha2.ResourceBindingPermanentIDLabel, bindingID)
		workLabel[workv1alpha2.ResourceBindingPermanentIDLabel] = bindingID
	} else {
		bindingID := util.GetLabelValue(binding.GetLabels(), workv1alpha2.ClusterResourceBindingPermanentIDLabel)
		util.MergeLabel(workload, workv1alpha2.ClusterResourceBindingPermanentIDLabel, bindingID)
		workLabel[workv1alpha2.ClusterResourceBindingPermanentIDLabel] = bindingID
	}
	return workLabel
}

func mergeAnnotations(workload *unstructured.Unstructured, binding metav1.Object, scope apiextensionsv1.ResourceScope) map[string]string {
	annotations := make(map[string]string)
	if workload.GetGeneration() > 0 {
		util.MergeAnnotation(workload, workv1alpha2.ResourceTemplateGenerationAnnotationKey, strconv.FormatInt(workload.GetGeneration(), 10))
	}

	if scope == apiextensionsv1.NamespaceScoped {
		util.MergeAnnotation(workload, workv1alpha2.ResourceBindingNamespaceAnnotationKey, binding.GetNamespace())
		util.MergeAnnotation(workload, workv1alpha2.ResourceBindingNameAnnotationKey, binding.GetName())
		annotations[workv1alpha2.ResourceBindingNamespaceAnnotationKey] = binding.GetNamespace()
		annotations[workv1alpha2.ResourceBindingNameAnnotationKey] = binding.GetName()
	} else {
		util.MergeAnnotation(workload, workv1alpha2.ClusterResourceBindingAnnotationKey, binding.GetName())
		annotations[workv1alpha2.ClusterResourceBindingAnnotationKey] = binding.GetName()
	}

	return annotations
}

// RecordAppliedOverrides record applied (cluster) overrides to annotations
func RecordAppliedOverrides(cops *overridemanager.AppliedOverrides, ops *overridemanager.AppliedOverrides,
	annotations map[string]string) (map[string]string, error) {
	if annotations == nil {
		annotations = make(map[string]string)
	}

	if cops != nil {
		appliedBytes, err := cops.MarshalJSON()
		if err != nil {
			return nil, err
		}
		if appliedBytes != nil {
			annotations[util.AppliedClusterOverrides] = string(appliedBytes)
		}
	}

	if ops != nil {
		appliedBytes, err := ops.MarshalJSON()
		if err != nil {
			return nil, err
		}
		if appliedBytes != nil {
			annotations[util.AppliedOverrides] = string(appliedBytes)
		}
	}

	return annotations, nil
}

// mergeConflictResolution determine the conflictResolution annotation of Work: preferentially inherit from RT, then RB
func mergeConflictResolution(workload *unstructured.Unstructured, conflictResolutionInBinding policyv1alpha1.ConflictResolution,
	annotations map[string]string) map[string]string {
	// conflictResolutionInRT refer to the annotation in ResourceTemplate
	conflictResolutionInRT := util.GetAnnotationValue(workload.GetAnnotations(), workv1alpha2.ResourceConflictResolutionAnnotation)

	// the final conflictResolution annotation value of Work inherit from RT preferentially
	// so if conflictResolution annotation is defined in RT already, just copy the value and return
	if conflictResolutionInRT == workv1alpha2.ResourceConflictResolutionOverwrite || conflictResolutionInRT == workv1alpha2.ResourceConflictResolutionAbort {
		annotations[workv1alpha2.ResourceConflictResolutionAnnotation] = conflictResolutionInRT
		return annotations
	} else if conflictResolutionInRT != "" {
		// ignore its value and add logs if conflictResolutionInRT is neither abort nor overwrite.
		klog.Warningf("Ignore the invalid conflict-resolution annotation in ResourceTemplate %s/%s/%s: %s",
			workload.GetKind(), workload.GetNamespace(), workload.GetName(), conflictResolutionInRT)
	}

	if conflictResolutionInBinding == policyv1alpha1.ConflictOverwrite {
		annotations[workv1alpha2.ResourceConflictResolutionAnnotation] = workv1alpha2.ResourceConflictResolutionOverwrite
		return annotations
	}

	annotations[workv1alpha2.ResourceConflictResolutionAnnotation] = workv1alpha2.ResourceConflictResolutionAbort
	return annotations
}

func divideReplicasByJobCompletions(workload *unstructured.Unstructured, clusters []workv1alpha2.TargetCluster) ([]workv1alpha2.TargetCluster, error) {
	var targetClusters []workv1alpha2.TargetCluster
	completions, found, err := unstructured.NestedInt64(workload.Object, util.SpecField, util.CompletionsField)
	if err != nil {
		return nil, err
	}

	if found {
		targetClusters = helper.SpreadReplicasByTargetClusters(int32(completions), clusters, nil, workload.GetUID()) // #nosec G115: integer overflow conversion int64 -> int32
	}

	return targetClusters, nil
}

func needReviseReplicas(replicas int32) bool {
	return replicas > 0
}

func needReviseJobCompletions(replicas int32, placement *policyv1alpha1.Placement) bool {
	return replicas > 0 && placement != nil && placement.ReplicaSchedulingType() == policyv1alpha1.ReplicaSchedulingTypeDivided
}

func shouldSuspendDispatching(suspension *workv1alpha2.Suspension, targetCluster workv1alpha2.TargetCluster) bool {
	if suspension == nil {
		return false
	}

	suspendDispatching := ptr.Deref(suspension.Dispatching, false)

	if !suspendDispatching && suspension.DispatchingOnClusters != nil {
		for _, cluster := range suspension.DispatchingOnClusters.ClusterNames {
			if cluster == targetCluster.Name {
				suspendDispatching = true
				break
			}
		}
	}
	return suspendDispatching
}
