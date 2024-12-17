package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	clientv3 "go.etcd.io/etcd/client/v3"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	ContainerEnergyQueryTemplate = "max(max_over_time(irate(kepler_container_joules_total{pod_name=~\".*%s.*\"}[1m])[1d:]))"
	WattToMicrowatt              = 1e+6
	TimeElapsed                  = 60.0
	DefaultKeyPath               = "/etc/kubernetes/pki/etcd/"
	DefaultEtcdHost              = "https://localhost:2379"
	DefaultPrometheusURL         = "http://prometheus-k8s.monitoring.svc.cluster.local:9090"
	EtcdKeyPrefix                = "/registry/kcas-energy/pods/"
)

type UsageEntry struct {
	Timestamp      string `json:"timestamp"`
	MaxEnergyUsage int64  `json:"max_energy_usage"`
}

type PodUsage struct {
	UsageHistory []UsageEntry `json:"usage_history"`
}

type EtcdData struct {
	Pods map[string]PodUsage `json:"pods"`
}

func main() {
	// Initialize clients
	etcdClient, err := initializeEtcdClient()
	handleFatalError("Error initializing etcd client", err)
	defer etcdClient.Close()

	promClient, err := initializePrometheusClient()
	handleFatalError("Error initializing Prometheus client", err)

	kubeClient, err := initializeKubernetesClient()
	handleFatalError("Error initializing Kubernetes client", err)

	// Fetch Kubernetes pod list
	pods, err := fetchPodList(kubeClient)
	handleFatalError("Error fetching pod list", err)

	// Initialize Etcd data
	initializeEtcdData(context.Background(), etcdClient, EtcdKeyPrefix)

	// Process pods
	processPods(pods, etcdClient, promClient)
}

// Initializes the etcd client with secure communication.
func initializeEtcdClient() (*clientv3.Client, error) {
	caCert, cert, err := loadCerts()
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		RootCAs:      caCert,
		Certificates: []tls.Certificate{cert},
	}

	etcdHost := getEnvOrDefault("ETCD_HOST", DefaultEtcdHost)
	etcdConfig := clientv3.Config{
		Endpoints:   []string{etcdHost},
		DialTimeout: 5 * time.Second,
		TLS:         tlsConfig,
	}

	return clientv3.New(etcdConfig)
}

// Initializes the Prometheus client.
func initializePrometheusClient() (promv1.API, error) {
	prometheusURL := getEnvOrDefault("PROMETHEUS_URL", DefaultPrometheusURL)
	client, err := api.NewClient(api.Config{Address: prometheusURL})
	if err != nil {
		return nil, err
	}
	return promv1.NewAPI(client), nil
}

// Initializes the Kubernetes client.
func initializeKubernetesClient() (*kubernetes.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}

// Fetches the list of pods from the Kubernetes cluster.
func fetchPodList(clientset *kubernetes.Clientset) (*v1.PodList, error) {
	return clientset.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{})
}

// Processes each pod, calculates energy usage, and updates etcd.
func processPods(pods *v1.PodList, etcdClient *clientv3.Client, promClient promv1.API) {
	for _, pod := range pods.Items {
		podBaseName := pod.GetLabels()["app.kcas/name"]
		if podBaseName == "" {
			continue
		}

		energyUsage, err := calculatePodEnergyUsage(podBaseName, promClient)
		if err != nil {
			log.Printf("Error calculating energy usage for pod %s: %v", podBaseName, err)
			continue
		}

		updatePodUsage(context.Background(), etcdClient, podBaseName, energyUsage)
	}
}

// Calculates energy usage for a pod based on Prometheus metrics.
func calculatePodEnergyUsage(podName string, promClient promv1.API) (int64, error) {
	queryString := fmt.Sprintf(ContainerEnergyQueryTemplate, podName)
	results, warnings, err := promClient.Query(context.Background(), queryString, time.Now())
	if err != nil {
		return 0, err
	}
	if len(warnings) > 0 {
		log.Printf("Prometheus warnings for pod %s: %v", podName, warnings)
	}

	vector, ok := results.(model.Vector)
	if !ok || len(vector) == 0 {
		return 0, fmt.Errorf("unexpected Prometheus response for pod %s", podName)
	}

	return int64(WattToMicrowatt * float64(vector[0].Value) / TimeElapsed), nil
}

// Updates the pod's energy usage data in etcd.
func updatePodUsage(ctx context.Context, etcdClient *clientv3.Client, podName string, energyUsage int64) {
	key := EtcdKeyPrefix 
	usageEntry := UsageEntry{
		Timestamp:      time.Now().Format(time.RFC3339),
		MaxEnergyUsage: energyUsage,
	}

	resp, err := etcdClient.Get(ctx, key)
	if err != nil {
		log.Printf("Error fetching etcd data for key %s: %v", key, err)
		return
	}

	var etcdData EtcdData
	if len(resp.Kvs) > 0 {
		if err := json.Unmarshal(resp.Kvs[0].Value, &etcdData); err != nil {
			log.Printf("Error unmarshalling etcd data for key %s: %v", key, err)
			return
		}
	} else {
		etcdData = EtcdData{Pods: make(map[string]PodUsage)}
	}

	podUsage := etcdData.Pods[podName]
	
	// Checks if an entry already exists within the last 5 minutes
	if hasRecentEntry(podUsage.UsageHistory) {
		log.Printf("Skipping update for pod %s: entry already exists within 5 minutes", podName)
		return
	}
	podUsage.UsageHistory = append(podUsage.UsageHistory, usageEntry)
	etcdData.Pods[podName] = podUsage

	saveEtcdData(ctx, etcdClient, key, etcdData)
}

// Saves pod usage data to etcd.
func saveEtcdData(ctx context.Context, etcdClient *clientv3.Client, key string, data EtcdData) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error marshalling etcd data for key %s: %v", key, err)
		return
	}

	if _, err := etcdClient.Put(ctx, key, string(jsonData)); err != nil {
		log.Printf("Error writing data to etcd for key %s: %v", key, err)
	}
}

// Checks if an entry has been added in the last 5 minutes
func hasRecentEntry(usageHistory []UsageEntry) bool {
	now := time.Now()
	threshold := now.Add(-5 * time.Minute)

	for _, entry := range usageHistory {
		entryTime, err := time.Parse(time.RFC3339, entry.Timestamp)
		if err != nil {
			log.Printf("Error parsing timestamp %s: %v", entry.Timestamp, err)
			continue
		}
		if entryTime.After(threshold) {
			return true // A recent entry exists
		}
	}

	return false
}

// Utility functions
func getEnvOrDefault(envKey, defaultValue string) string {
	if value := os.Getenv(envKey); value != "" {
		return value
	}
	return defaultValue
}

func loadCerts() (*x509.CertPool, tls.Certificate, error) {
	keyPath := getEnvOrDefault("KEY_PATH", DefaultKeyPath)
	caCert, err := os.ReadFile(keyPath + "ca.crt")
	if err != nil {
		return nil, tls.Certificate{}, fmt.Errorf("error reading CA certificate: %w", err)
	}

	cert, err := tls.LoadX509KeyPair(keyPath+"server.crt", keyPath+"server.key")
	if err != nil {
		return nil, tls.Certificate{}, fmt.Errorf("error loading client certificate: %w", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	return caCertPool, cert, nil
}

// Initializes Etcd data if the key is not present.
func initializeEtcdData(ctx context.Context, etcdClient *clientv3.Client, key string) {
	resp, err := etcdClient.Get(ctx, key)
	if err != nil || len(resp.Kvs) == 0 {
		etcdData := EtcdData{Pods: make(map[string]PodUsage)}
		saveEtcdData(ctx, etcdClient, key, etcdData)
	}
}

func handleFatalError(message string, err error) {
	if err != nil {
		log.Fatalf("%s: %v", message, err)
	}
}
