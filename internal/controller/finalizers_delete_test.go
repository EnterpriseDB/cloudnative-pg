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

package controller

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/cloudnative-pg/cloudnative-pg/api/v1"
	schemeBuilder "github.com/cloudnative-pg/cloudnative-pg/internal/scheme"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Database CRD finalizers", func() {
	var (
		r              ClusterReconciler
		scheme         *runtime.Scheme
		namespacedName types.NamespacedName
	)

	BeforeEach(func() {
		scheme = schemeBuilder.BuildWithAllKnownScheme()
		r = ClusterReconciler{
			Scheme: scheme,
		}
		namespacedName = types.NamespacedName{
			Namespace: "test",
			Name:      "cluster",
		}
	})

	It("should be deleted when the cluster name matches with the database cluster ref name",
		func(ctx SpecContext) {
			databaseList := &v1.DatabaseList{
				Items: []v1.Database{
					{
						ObjectMeta: metav1.ObjectMeta{
							Finalizers: []string{
								utils.DatabaseFinalizerName,
							},
							Name:      "db-1",
							Namespace: "test"},
						Spec: v1.DatabaseSpec{
							Name: "db-test",
							ClusterRef: corev1.LocalObjectReference{
								Name: "cluster",
							},
						},
					},
				}}

			cli := fake.NewClientBuilder().WithScheme(scheme).WithLists(databaseList).Build()
			r.Client = cli
			err := r.deleteDatabaseFinalizers(ctx, namespacedName)
			Expect(err).ToNot(HaveOccurred())

			database := &v1.Database{}
			err = cli.Get(ctx, client.ObjectKeyFromObject(&databaseList.Items[0]), database)
			Expect(err).ToNot(HaveOccurred())
			Expect(database.Finalizers).To(BeZero())
		})

	It("should not be deleted when the cluster name does not  match with the database cluster ref name",
		func(ctx SpecContext) {
			databaseList := &v1.DatabaseList{
				Items: []v1.Database{
					{
						ObjectMeta: metav1.ObjectMeta{
							Finalizers: []string{
								utils.DatabaseFinalizerName,
							},
							Name:      "db-1",
							Namespace: "test"},
						Spec: v1.DatabaseSpec{
							Name: "db-test",
							ClusterRef: corev1.LocalObjectReference{
								Name: "wrong-cluster",
							},
						},
					},
				}}

			cli := fake.NewClientBuilder().WithScheme(scheme).WithLists(databaseList).Build()
			r.Client = cli
			err := r.deleteDatabaseFinalizers(ctx, namespacedName)
			Expect(err).ToNot(HaveOccurred())

			database := &v1.Database{}
			err = cli.Get(ctx, client.ObjectKeyFromObject(&databaseList.Items[0]), database)
			Expect(err).ToNot(HaveOccurred())
			Expect(database.Finalizers).To(BeEquivalentTo([]string{utils.DatabaseFinalizerName}))
		})
})
