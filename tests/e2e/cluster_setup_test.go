/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
*/

package e2e

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/specs"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/utils"
	"github.com/cloudnative-pg/cloudnative-pg/tests"
	"github.com/cloudnative-pg/cloudnative-pg/tests/utils/clusterutils"
	"github.com/cloudnative-pg/cloudnative-pg/tests/utils/postgres"
	"github.com/cloudnative-pg/cloudnative-pg/tests/utils/timeouts"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cluster setup", Label(tests.LabelSmoke, tests.LabelBasic), func() {
	const (
		sampleFile  = fixturesDir + "/base/cluster-storage-class.yaml.template"
		clusterName = "postgresql-storage-class"
		level       = tests.Highest
	)

	var namespace string

	BeforeEach(func() {
		if testLevelEnv.Depth < int(level) {
			Skip("Test depth is lower than the amount requested for this test")
		}
	})

	It("sets up a cluster", func(_ SpecContext) {
		const namespacePrefix = "cluster-storageclass-e2e"
		var err error

		// Create a cluster in a namespace we'll delete after the test
		namespace, err = env.CreateUniqueTestNamespace(env.Ctx, env.Client, namespacePrefix)
		Expect(err).ToNot(HaveOccurred())

		AssertCreateCluster(namespace, clusterName, sampleFile, env)

		By("having three PostgreSQL pods with status ready", func() {
			podList, err := clusterutils.ListPods(env.Ctx, env.Client, namespace, clusterName)
			Expect(utils.CountReadyPods(podList.Items), err).Should(BeEquivalentTo(3))
		})

		By("being able to restart a killed pod without losing it", func() {
			commandTimeout := time.Second * 10
			timeout := 120
			podName := clusterName + "-1"
			pod := &corev1.Pod{}
			namespacedName := types.NamespacedName{
				Namespace: namespace,
				Name:      podName,
			}
			err := env.Client.Get(env.Ctx, namespacedName, pod)
			Expect(err).ToNot(HaveOccurred())

			forward, conn, err := postgres.ForwardPSQLConnection(
				env.Ctx,
				env.Client,
				env.Interface,
				env.RestClientConfig,
				namespace,
				clusterName,
				postgres.AppDBName,
				apiv1.ApplicationUserSecretSuffix,
			)
			Expect(err).NotTo(HaveOccurred())

			query := "CREATE TABLE IF NOT EXISTS test (id bigserial PRIMARY KEY, t text);"
			_, err = conn.Exec(query)
			Expect(err).NotTo(HaveOccurred())

			// Here we need to close the connection and close the forward, if we don't do both steps
			// the PostgreSQL connection will be there and PostgreSQL will not restart in time because
			// of the connection that wasn't close and stays idle
			_ = conn.Close()
			forward.Close()

			// We kill the pid 1 process.
			// The pod should be restarted and the count of the restarts
			// should increase by one
			restart := int32(-1)
			for _, data := range pod.Status.ContainerStatuses {
				if data.Name == specs.PostgresContainerName {
					restart = data.RestartCount
				}
			}
			_, _, err = env.EventuallyExecCommand(env.Ctx, *pod, specs.PostgresContainerName, &commandTimeout,
				"sh", "-c", "kill 1")
			Expect(err).ToNot(HaveOccurred())
			Eventually(func() (int32, error) {
				pod := &corev1.Pod{}
				if err := env.Client.Get(env.Ctx, namespacedName, pod); err != nil {
					return 0, err
				}

				for _, data := range pod.Status.ContainerStatuses {
					if data.Name == specs.PostgresContainerName {
						return data.RestartCount, nil
					}
				}

				return int32(-1), nil
			}, timeout).Should(BeEquivalentTo(restart + 1))

			AssertClusterIsReady(namespace, clusterName, testTimeouts[timeouts.ClusterIsReady], env)

			forward, conn, err = postgres.ForwardPSQLConnection(
				env.Ctx,
				env.Client,
				env.Interface,
				env.RestClientConfig,
				namespace,
				clusterName,
				postgres.AppDBName,
				apiv1.ApplicationUserSecretSuffix,
			)
			defer func() {
				_ = conn.Close()
				forward.Close()
			}()
			Expect(err).NotTo(HaveOccurred())

			_, err = conn.Exec("SELECT * FROM test")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	It("tests cluster readiness conditions work", func() {
		const namespacePrefix = "cluster-conditions"

		var err error
		namespace, err = env.CreateUniqueTestNamespace(env.Ctx, env.Client, namespacePrefix)
		Expect(err).ToNot(HaveOccurred())

		By(fmt.Sprintf("having a %v namespace", namespace), func() {
			// Creating a namespace should be quick
			timeout := 20
			namespacedName := types.NamespacedName{
				Namespace: namespace,
				Name:      namespace,
			}
			Eventually(func() (string, error) {
				namespaceResource := &corev1.Namespace{}
				err := env.Client.Get(env.Ctx, namespacedName, namespaceResource)
				return namespaceResource.GetName(), err
			}, timeout).Should(BeEquivalentTo(namespace))
		})

		By(fmt.Sprintf("creating a Cluster in the %v namespace", namespace), func() {
			CreateResourceFromFile(namespace, sampleFile)
		})

		By("verifying cluster reaches ready condition", func() {
			AssertClusterReadinessStatusIsReached(namespace, clusterName, apiv1.ConditionTrue, 600, env)
		})

		// scale up the cluster to verify if the cluster remains in Ready
		By("scaling up the cluster size", func() {
			err := clusterutils.ScaleSize(env.Ctx, env.Client, namespace, clusterName, 5)
			Expect(err).ToNot(HaveOccurred())
		})

		By("verifying cluster readiness condition is false just after scale-up", func() {
			// Just after scale up the cluster, the condition status set to be `False` and cluster is not ready state.
			AssertClusterReadinessStatusIsReached(namespace, clusterName, apiv1.ConditionFalse, 180, env)
		})

		By("verifying cluster reaches ready condition after additional waiting", func() {
			AssertClusterReadinessStatusIsReached(namespace, clusterName, apiv1.ConditionTrue, 180, env)
		})
	})
})
