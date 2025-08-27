package dynamo

import (
	"context"
	"fmt"

	grovev1alpha1 "github.com/NVIDIA/grove/operator/api/core/v1alpha1"
	commonconsts "github.com/ai-dynamo/dynamo/deploy/cloud/operator/internal/consts"
	"github.com/ai-dynamo/dynamo/deploy/cloud/operator/internal/controller_common"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type GroveMultinodeDeployer struct {
	MultinodeDeployer
}

func (d *GroveMultinodeDeployer) GetLeaderHostname(serviceName string) string {
	return fmt.Sprintf("${GROVE_PCSG_NAME}-${GROVE_PCSG_INDEX}-%s-%s-0.${GROVE_HEADLESS_SERVICE}", serviceName, commonconsts.GroveRoleSuffixLeader)
}

func (d *GroveMultinodeDeployer) GetNodeRank() string {
	return "$((GROVE_PCLQ_POD_INDEX + 1))"
}

func (d *GroveMultinodeDeployer) GetHostNames(serviceName string, numberOfNodes int32) []string {
	hostnames := make([]string, 0, numberOfNodes)
	leaderHostname := d.GetLeaderHostname(serviceName)
	hostnames = append(hostnames, leaderHostname)
	// Add worker hostnames
	for i := int32(0); i < numberOfNodes-1; i++ {
		workerHostname := fmt.Sprintf("${GROVE_PCSG_NAME}-${GROVE_PCSG_INDEX}-%s-%s-%d.${GROVE_HEADLESS_SERVICE}",
			serviceName, commonconsts.GroveRoleSuffixWorker, i)
		hostnames = append(hostnames, workerHostname)
	}
	return hostnames
}

// resolveKaiSchedulerQueueName extracts the queue name from annotations or returns default
// This is the shared logic between DetermineKaiSchedulerQueue and ResolveKaiSchedulerQueue
func resolveKaiSchedulerQueueName(annotations map[string]string) string {
	queueName := commonconsts.DefaultKaiSchedulerQueue
	if annotations != nil {
		if annotationQueue, exists := annotations[commonconsts.KubeAnnotationKaiSchedulerQueue]; exists && annotationQueue != "" {
			queueName = annotationQueue
		}
	}
	return queueName
}

// ensureQueueExists validates that a Queue resource with the given name exists in the cluster
// Returns an error if the queue doesn't exist or if validation fails
func ensureQueueExists(ctx context.Context, dynamicClient dynamic.Interface, queueName string) error {
	logger := log.FromContext(ctx)

	// Define the Queue resource schema (scheduling.run.ai/v2)
	queueGVR := schema.GroupVersionResource{
		Group:    "scheduling.run.ai",
		Version:  "v2",
		Resource: "queues",
	}

	// Try to get the queue resource
	_, err := dynamicClient.Resource(queueGVR).Get(ctx, queueName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Error(err, "Queue not found", "queueName", queueName)
			return fmt.Errorf("queue '%s' not found in cluster. Ensure the queue exists before using kai-scheduler", queueName)
		}
		logger.Error(err, "Failed to validate queue", "queueName", queueName)
		return fmt.Errorf("failed to validate queue '%s': %w", queueName, err)
	}

	logger.Info("Queue validation successful", "queueName", queueName)
	return nil
}

// DetermineKaiSchedulerQueue determines the queue name for kai-scheduler from deployment annotations or returns default
// Also validates that the queue exists in the cluster
func DetermineKaiSchedulerQueue(ctx context.Context, annotations map[string]string) (string, error) {
	// Get the queue name from annotation or use default
	queueName := resolveKaiSchedulerQueueName(annotations)

	// Create a dynamic client for CRD validation (Queue CRD might not be in the standard client scheme)
	cfg, err := ctrl.GetConfig()
	if err != nil {
		return "", fmt.Errorf("failed to get kubernetes config for queue validation: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return "", fmt.Errorf("failed to create dynamic client for queue validation: %w", err)
	}

	// Validate that the queue exists
	if err := ensureQueueExists(ctx, dynamicClient, queueName); err != nil {
		return "", fmt.Errorf("kai-scheduler queue validation failed: %w", err)
	}

	return queueName, nil
}

// ResolveKaiSchedulerQueue determines the queue name for kai-scheduler from deployment annotations or returns default
// Does NOT validate - use DetermineKaiSchedulerQueue for validation
func ResolveKaiSchedulerQueue(annotations map[string]string) string {
	return resolveKaiSchedulerQueueName(annotations)
}

// injectKaiSchedulerIfEnabled injects kai-scheduler settings into a clique if kai-scheduler is enabled and grove is enabled
func injectKaiSchedulerIfEnabled(
	clique *grovev1alpha1.PodCliqueTemplateSpec,
	controllerConfig controller_common.Config,
	validatedQueueName string,
) {
	// Only proceed if grove is enabled, kai-scheduler is enabled, and no manual schedulerName is set
	if !controllerConfig.Grove.Enabled || !controllerConfig.KaiScheduler.Enabled {
		return
	}

	// Check if user has manually set schedulerName - if so, respect their choice
	if clique.Spec.PodSpec.SchedulerName != "" {
		return
	}

	// Use the pre-validated queue name
	queueName := validatedQueueName

	// Inject schedulerName
	clique.Spec.PodSpec.SchedulerName = commonconsts.KaiSchedulerName

	// Inject queue label
	if clique.Labels == nil {
		clique.Labels = make(map[string]string)
	}
	clique.Labels[commonconsts.KubeLabelKaiSchedulerQueue] = queueName
}
