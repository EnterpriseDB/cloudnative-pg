/*
Copyright The CloudNativePG Contributors

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

package e2e

import (
	"os"
	"strings"
	"time"

	"github.com/cloudnative-pg/cloudnative-pg/pkg/certs"
	"github.com/cloudnative-pg/cloudnative-pg/tests"
	testUtils "github.com/cloudnative-pg/cloudnative-pg/tests/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Test case for validating volume snapshots
// with different storage providers in different k8s environments
var _ = Describe("Verify Volume Snapshot",
	Label(tests.LabelBackupRestore, tests.LabelStorage, tests.LabelSnapshot), func() {
		// Initializing a global namespace variable to be used in each test case
		var namespace string
		// Gathering the default volumeSnapshot class for the current environment
		volumeSnapshotClassName := os.Getenv("E2E_DEFAULT_VOLUMESNAPSHOT_CLASS")

		Context("Can create a Volume Snapshot", Ordered, func() {
			// test env constants
			const (
				sampleFile      = fixturesDir + "/volume_snapshot/cluster-volume-snapshot.yaml.template"
				namespacePrefix = "volume-snapshot"
				level           = tests.High
				snapshotSuffix  = "test"
			)

			var clusterName string
			BeforeAll(func() {
				if testLevelEnv.Depth < int(level) {
					Skip("Test depth is lower than the amount requested for this test")
				}
				var err error
				clusterName, err = env.GetResourceNameFromYAML(sampleFile)
				Expect(err).ToNot(HaveOccurred())

				// Initializing namespace variable to be used in test case
				namespace, err = env.CreateUniqueNamespace(namespacePrefix)
				Expect(err).ToNot(HaveOccurred())

				DeferCleanup(func() error {
					if CurrentSpecReport().Failed() {
						env.DumpNamespaceObjects(namespace, "out/"+CurrentSpecReport().LeafNodeText+".log")
					}
					return env.DeleteNamespace(namespace)
				})

				// Creating a cluster with three nodes
				AssertCreateCluster(namespace, clusterName, sampleFile, env)
			})

			It("using the kubectl cnpg plugin", func() {
				err := testUtils.CreateVolumeSnapshotBackup(
					volumeSnapshotClassName,
					namespace,
					clusterName,
					"",
				)
				Expect(err).ToNot(HaveOccurred())

				Eventually(func(g Gomega) {
					snapshotList, _ := env.GetSnapshotList(namespace)
					for _, snapshot := range snapshotList.Items {
						if strings.Contains(snapshot.Name, snapshotSuffix) {
							continue
						}
						g.Expect(snapshot.Name).To(ContainSubstring(clusterName))
					}
				}).Should(Succeed())
			})

			It("using the kubectl cnpg plugin with a custom suffix", func() {
				err := testUtils.CreateVolumeSnapshotBackup(
					volumeSnapshotClassName,
					namespace,
					clusterName,
					snapshotSuffix,
				)
				Expect(err).ToNot(HaveOccurred())

				Eventually(func(g Gomega) {
					snapshotList, _ := env.GetSnapshotList(namespace)
					for _, snapshot := range snapshotList.Items {
						if strings.Contains(snapshot.Name, snapshotSuffix) {
							g.Expect(snapshot.Name).To(ContainSubstring(clusterName))
						}
					}
				}).Should(Succeed())
			})
		})

		Context("Can restore from a Volume Snapshot", Ordered, func() {
			// test env constants
			const (
				namespacePrefix = "volume-snapshot-recovery"
				level           = tests.High
				filesDir        = fixturesDir + "/volume_snapshot"
			)
			// minio constants
			const (
				minioCaSecName  = "minio-server-ca-secret"
				minioTLSSecName = "minio-server-tls-secret"
			)
			// file constants
			const (
				clusterToSnapshot          = filesDir + "/cluster-pvc-snapshot.yaml.template"
				clusterSnapshotRestoreFile = filesDir + "/cluster-pvc-snapshot-restore.yaml.template"
			)
			// database constants
			const (
				tableName = "test"
			)

			var clusterToSnapshotName string
			BeforeAll(func() {
				if testLevelEnv.Depth < int(level) {
					Skip("Test depth is lower than the amount requested for this test")
				}

				if !(IsLocal() || IsGKE()) {
					Skip("This test is only executed on gke, openshift and local")
				}

				var err error
				clusterToSnapshotName, err = env.GetResourceNameFromYAML(clusterToSnapshot)
				Expect(err).ToNot(HaveOccurred())

				namespace, err = env.CreateUniqueNamespace(namespacePrefix)
				Expect(err).ToNot(HaveOccurred())

				DeferCleanup(func() error {
					if CurrentSpecReport().Failed() {
						env.DumpNamespaceObjects(namespace, "out/"+CurrentSpecReport().LeafNodeText+".log")
					}
					return env.DeleteNamespace(namespace)
				})

				By("creating ca and tls certificate secrets", func() {
					// create CA certificates
					_, caPair, err := testUtils.CreateSecretCA(namespace, clusterToSnapshotName, minioCaSecName, true, env)
					Expect(err).ToNot(HaveOccurred())

					// sign and create secret using CA certificate and key
					serverPair, err := caPair.CreateAndSignPair("minio-service", certs.CertTypeServer,
						[]string{"minio-service.internal.mydomain.net, minio-service.default.svc, minio-service.default,"},
					)
					Expect(err).ToNot(HaveOccurred())
					serverSecret := serverPair.GenerateCertificateSecret(namespace, minioTLSSecName)
					err = env.Client.Create(env.Ctx, serverSecret)
					Expect(err).ToNot(HaveOccurred())
				})

				By("creating the credentials for minio", func() {
					AssertStorageCredentialsAreCreated(namespace, "backup-storage-creds", "minio", "minio123")
				})

				By("setting up minio", func() {
					setup, err := testUtils.MinioSSLSetup(namespace)
					Expect(err).ToNot(HaveOccurred())
					err = testUtils.InstallMinio(env, setup, uint(testTimeouts[testUtils.MinioInstallation]))
					Expect(err).ToNot(HaveOccurred())
				})

				// Create the minio client pod and wait for it to be ready.
				// We'll use it to check if everything is archived correctly
				By("setting up minio client pod", func() {
					minioClient := testUtils.MinioSSLClient(namespace)
					err := testUtils.PodCreateAndWaitForReady(env, &minioClient, 240)
					Expect(err).ToNot(HaveOccurred())
				})
			})

			It("correctly executes PITR with a cold snapshot", func() {
				DeferCleanup(func() error {
					return os.Unsetenv("SNAPSHOT_PITR")
				})

				By("creating the cluster to snapshot", func() {
					AssertCreateCluster(namespace, clusterToSnapshotName, clusterToSnapshot, env)
				})

				By("verify test connectivity to minio using barman-cloud-wal-archive script", func() {
					primaryPod, err := env.GetClusterPrimary(namespace, clusterToSnapshotName)
					Expect(err).ToNot(HaveOccurred())
					Eventually(func() (bool, error) {
						connectionStatus, err := testUtils.MinioTestConnectivityUsingBarmanCloudWalArchive(
							namespace, clusterToSnapshotName, primaryPod.GetName(), "minio", "minio123")
						if err != nil {
							return false, err
						}
						return connectionStatus, nil
					}, 60).Should(BeTrue())
				})

				By("creating the snapshot", func() {
					const suffix = "test-pitr"
					err := testUtils.CreateVolumeSnapshotBackup(
						volumeSnapshotClassName,
						namespace,
						clusterToSnapshotName,
						suffix,
					)
					Expect(err).ToNot(HaveOccurred())
				})

				By("inserting test data and creating WALs on the cluster to be snapshotted", func() {
					// Create a "test" table with values 1,2
					AssertCreateTestData(namespace, clusterToSnapshotName, tableName, psqlClientPod)

					// Because GetCurrentTimestamp() rounds down to the second and is executed
					// right after the creation of the test data, we wait for 1s to avoid not
					// including the newly created data within the recovery_target_time
					time.Sleep(1 * time.Second)
					// Get the recovery_target_time and pass it to the template engine
					recoveryTargetTime, err := testUtils.GetCurrentTimestamp(namespace, clusterToSnapshotName, env, psqlClientPod)
					Expect(err).ToNot(HaveOccurred())
					err = os.Setenv("SNAPSHOT_PITR", recoveryTargetTime)
					Expect(err).ToNot(HaveOccurred())

					// Insert 2 more rows which we expect not to be present at the end of the recovery
					insertRecordIntoTable(namespace, clusterToSnapshotName, tableName, 3, psqlClientPod)
					insertRecordIntoTable(namespace, clusterToSnapshotName, tableName, 4, psqlClientPod)

					// Close and archive the current WAL file
					AssertArchiveWalOnMinio(namespace, clusterToSnapshotName, clusterToSnapshotName)
				})

				clusterToRestoreName, err := env.GetResourceNameFromYAML(clusterSnapshotRestoreFile)
				Expect(err).ToNot(HaveOccurred())

				By("creating the cluster to be restored through snapshot and PITR", func() {
					AssertCreateCluster(namespace, clusterToRestoreName, clusterSnapshotRestoreFile, env)
					AssertClusterIsReady(namespace, clusterToRestoreName, testTimeouts[testUtils.ClusterIsReadySlow], env)
				})

				By("verifying the correct data exists in the restored cluster", func() {
					restoredPrimary, err := env.GetClusterPrimary(namespace, clusterToRestoreName)
					Expect(err).ToNot(HaveOccurred())
					AssertDataExpectedCount(namespace, clusterToRestoreName, tableName, 2, restoredPrimary)
				})
			})
		})
	})
