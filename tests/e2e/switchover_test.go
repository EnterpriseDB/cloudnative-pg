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
	"github.com/cloudnative-pg/cloudnative-pg/tests"
	"github.com/cloudnative-pg/cloudnative-pg/tests/utils/yaml"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Switchover", Serial, Label(tests.LabelSelfHealing), func() {
	const (
		sampleFileWithoutReplicationSlots = fixturesDir + "/switchover/cluster-switchover.yaml.template"
		sampleFileWithReplicationSlots    = fixturesDir + "/switchover/cluster-switchover-with-rep-slots.yaml.template"
		level                             = tests.Medium
	)
	var namespace string
	BeforeEach(func() {
		if testLevelEnv.Depth < int(level) {
			Skip("Test depth is lower than the amount requested for this test")
		}
	})

	Context("with HA Replication slots", func() {
		It("reacts to switchover requests", func() {
			// Create a cluster in a namespace we'll delete after the test
			const namespacePrefix = "switchover-e2e-with-slots"
			var err error
			namespace, err = env.CreateUniqueTestNamespace(env.Ctx, env.Client, namespacePrefix)
			Expect(err).ToNot(HaveOccurred())
			clusterName, err := yaml.GetResourceNameFromYAML(env.Scheme, sampleFileWithReplicationSlots)
			Expect(err).ToNot(HaveOccurred())

			AssertCreateCluster(namespace, clusterName, sampleFileWithReplicationSlots, env)
			AssertSwitchover(namespace, clusterName, env)
			AssertPvcHasLabels(namespace, clusterName)
			AssertClusterHAReplicationSlots(namespace, clusterName)
		})
	})
	Context("without HA Replication slots", func() {
		It("reacts to switchover requests", func() {
			// Create a cluster in a namespace we'll delete after the test
			const namespacePrefix = "switchover-e2e"
			var err error
			namespace, err = env.CreateUniqueTestNamespace(env.Ctx, env.Client, namespacePrefix)
			Expect(err).ToNot(HaveOccurred())
			clusterName, err := yaml.GetResourceNameFromYAML(env.Scheme, sampleFileWithoutReplicationSlots)
			Expect(err).ToNot(HaveOccurred())

			AssertCreateCluster(namespace, clusterName, sampleFileWithoutReplicationSlots, env)
			AssertSwitchover(namespace, clusterName, env)
			AssertPvcHasLabels(namespace, clusterName)
		})
	})
})
