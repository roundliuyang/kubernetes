/*
Copyright 2016 The Kubernetes Authors.

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

// Package app implements a server that runs a set of active
// components.  This includes replication controllers, service endpoints and
// nodes.
package app

import (
	"fmt"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/kubernetes/pkg/controller/daemon"
	"k8s.io/kubernetes/pkg/controller/deployment"
	"k8s.io/kubernetes/pkg/controller/replicaset"
	"k8s.io/kubernetes/pkg/controller/statefulset"
)

func startDaemonSetController(ctx ControllerContext) (http.Handler, bool, error) {
	if !ctx.AvailableResources[schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "daemonsets"}] {
		return nil, false, nil
	}
	dsc, err := daemon.NewDaemonSetsController(
		ctx.InformerFactory.Apps().V1().DaemonSets(),
		ctx.InformerFactory.Apps().V1().ControllerRevisions(),
		ctx.InformerFactory.Core().V1().Pods(),
		ctx.InformerFactory.Core().V1().Nodes(),
		ctx.ClientBuilder.ClientOrDie("daemon-set-controller"),
		flowcontrol.NewBackOff(1*time.Second, 15*time.Minute),
	)
	if err != nil {
		return nil, true, fmt.Errorf("error creating DaemonSets controller: %v", err)
	}
	go dsc.Run(int(ctx.ComponentConfig.DaemonSetController.ConcurrentDaemonSetSyncs), ctx.Stop)
	return nil, true, nil
}

func startStatefulSetController(ctx ControllerContext) (http.Handler, bool, error) {
	if !ctx.AvailableResources[schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}] {
		return nil, false, nil
	}
	go statefulset.NewStatefulSetController(
		ctx.InformerFactory.Core().V1().Pods(),
		ctx.InformerFactory.Apps().V1().StatefulSets(),
		ctx.InformerFactory.Core().V1().PersistentVolumeClaims(),
		ctx.InformerFactory.Apps().V1().ControllerRevisions(),
		ctx.ClientBuilder.ClientOrDie("statefulset-controller"),
	).Run(int(ctx.ComponentConfig.StatefulSetController.ConcurrentStatefulSetSyncs), ctx.Stop)
	return nil, true, nil
}

/*
	启动 Kubernetes 的 ReplicaSet 控制器，用于确保实际运行的 Pod 数量与 ReplicaSet 中声明的副本数一致。
	它是 kube-controller-manager 启动控制器流程中的一环，由 StartControllers 调用
	控制器核心职责
	• 每当 ReplicaSet 或 Pod 状态发生变化时，控制器会响应：
		• ReplicaSet 更新： 如果 .spec.replicas 有变化 → 创建或删除 Pod
		• Pod 状态变化： 比如被杀掉或意外挂掉 → 尝试补充新 Pod
*/
func startReplicaSetController(ctx ControllerContext) (http.Handler, bool, error) {
	// 检查是否支持 ReplicaSet 资源（防止 API Server 不支持），如果不支持，控制器不启动，返回 false
	if !ctx.AvailableResources[schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}] {
		return nil, false, nil
	}
	// 启动 ReplicaSet 控制器
	// 核心逻辑：
	// • 使用两个 Informer：
	//     • ReplicaSet informer：监控期望状态
	//     • Pod informer：监控实际状态
	// • Run() 内部启动多个 goroutine（由 ConcurrentRSSyncs 决定并发度）
	// • 控制器监听变更事件、维护队列、执行 reconcile 操作
	go replicaset.NewReplicaSetController(
		ctx.InformerFactory.Apps().V1().ReplicaSets(),          // 监听 ReplicaSet
		ctx.InformerFactory.Core().V1().Pods(),                 // 监听 Pod（必须，控制器需知道实际 Pod 状态）
		ctx.ClientBuilder.ClientOrDie("replicaset-controller"), // 构造用于访问 API Server 的 client
		replicaset.BurstReplicas,                               // 每次调整副本数时最多 burst 创建多少个 Pod（用于限制）
	).Run(int(ctx.ComponentConfig.ReplicaSetController.ConcurrentRSSyncs), ctx.Stop)
	return nil, true, nil
}

func startDeploymentController(ctx ControllerContext) (http.Handler, bool, error) {
	if !ctx.AvailableResources[schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}] {
		return nil, false, nil
	}
	dc, err := deployment.NewDeploymentController(
		ctx.InformerFactory.Apps().V1().Deployments(),
		ctx.InformerFactory.Apps().V1().ReplicaSets(),
		ctx.InformerFactory.Core().V1().Pods(),
		ctx.ClientBuilder.ClientOrDie("deployment-controller"),
	)
	if err != nil {
		return nil, true, fmt.Errorf("error creating Deployment controller: %v", err)
	}
	go dc.Run(int(ctx.ComponentConfig.DeploymentController.ConcurrentDeploymentSyncs), ctx.Stop)
	return nil, true, nil
}
