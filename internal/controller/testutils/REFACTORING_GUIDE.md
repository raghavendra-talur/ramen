# Test Refactoring Guide

This guide explains how to refactor existing Ginkgo-based tests to follow the new pattern where utility functions do not use Ginkgo/Gomega directly.

## Principles

1. **Utility functions return errors** - They should not call `Expect()`, `Eventually()`, or any Ginkgo/Gomega functions
2. **Tests use Expect()** - Only the actual test code (inside `It()`, `BeforeEach()`, etc.) should use Ginkgo/Gomega
3. **Builders for fixtures** - Use builder pattern for creating test objects
4. **Explicit error handling** - Makes tests clearer and debugging easier

## Before: Old Pattern (Anti-pattern)

```go
// ❌ BAD: Utility function uses Expect()
func ensureManagedCluster(k8sClient client.Client, cluster string) {
    mc := ocmv1.ManagedCluster{
        ObjectMeta: metav1.ObjectMeta{Name: cluster},
        Spec:       ocmv1.ManagedClusterSpec{HubAcceptsClient: true},
    }
    Expect(k8sClient.Create(context.TODO(), &mc)).To(Succeed())  // ❌ Ginkgo in utility
    updateManagedClusterStatus(k8sClient, &mc)
}

// Test uses the utility
var _ = Describe("DRCluster", func() {
    It("should work", func() {
        ensureManagedCluster(k8sClient, "cluster1")  // Hard to know what failed
    })
})
```

## After: New Pattern

```go
// ✅ GOOD: Utility function returns error
func CreateManagedClusterWithStatus(
    ctx context.Context,
    k8sClient client.Client,
    name string,
    opts ManagedClusterOptions,
) (*ocmv1.ManagedCluster, error) {
    mc := &ocmv1.ManagedCluster{
        ObjectMeta: metav1.ObjectMeta{Name: name},
        Spec:       ocmv1.ManagedClusterSpec{HubAcceptsClient: opts.HubAcceptsClient},
    }
    if err := k8sClient.Create(ctx, mc); err != nil {
        return nil, fmt.Errorf("failed to create ManagedCluster: %w", err)
    }
    if err := UpdateManagedClusterStatus(ctx, k8sClient, mc, opts.ClusterID); err != nil {
        return nil, err
    }
    return mc, nil
}

// Test handles errors explicitly
var _ = Describe("DRCluster", func() {
    It("should work", func() {
        mc, err := testutils.CreateManagedClusterWithStatus(ctx, k8sClient, "cluster1",
            testutils.DefaultManagedClusterOptions())
        Expect(err).NotTo(HaveOccurred())  // ✅ Explicit error check in test
        Expect(mc.Name).To(Equal("cluster1"))
    })
})
```

## Using Builders

```go
// Create test fixtures with builders
drcluster := testutils.NewDRClusterBuilder("test-cluster").
    WithS3Profile("profile1").
    WithCIDRs([]string{"10.0.0.0/8"}).
    Build()

Expect(k8sClient.Create(ctx, drcluster)).To(Succeed())
```

## Waiting for Conditions

```go
// ❌ BAD: Eventually inside utility
func drclusterConditionExpect(drcluster *ramen.DRCluster, status metav1.ConditionStatus) {
    Eventually(func() []metav1.Condition {
        Expect(apiReader.Get(...)).To(Succeed())  // ❌ Nested Expect
        return drcluster.Status.Conditions
    }, timeout, interval).Should(...)
}

// ✅ GOOD: Utility returns result, test uses Eventually
func WaitForDRClusterCondition(ctx context.Context, reader client.Reader, 
    name string, matcher ConditionMatcher, opts WaitOptions) (*ramen.DRCluster, error) {
    var drcluster *ramen.DRCluster
    err := wait.PollUntilContextTimeout(ctx, opts.Interval, opts.Timeout, true,
        func(ctx context.Context) (bool, error) {
            drcluster = &ramen.DRCluster{}
            if err := reader.Get(ctx, types.NamespacedName{Name: name}, drcluster); err != nil {
                return false, err
            }
            if err := CheckDRClusterCondition(drcluster, matcher); err != nil {
                return false, nil // Keep waiting
            }
            return true, nil
        })
    return drcluster, err
}

// Test code
It("should validate DRCluster", func() {
    drcluster, err := testutils.WaitForDRClusterConditionMatch(ctx, apiReader, "test",
        testutils.ConditionMatcher{
            Type:   ramen.DRClusterValidated,
            Status: metav1.ConditionTrue,
        },
        testutils.DefaultWaitOptions())
    Expect(err).NotTo(HaveOccurred())
    Expect(drcluster.Status.Phase).To(Equal(ramen.Available))
})
```

## Package Structure

```
internal/controller/testutils/
├── doc.go                    # Package documentation
├── ginkgo.go                 # Only Ginkgo configuration helper
├── fixtures.go               # Builder patterns for test objects
├── fixtures_test.go          # Standard Go tests for fixtures
├── k8s_helpers.go            # K8s client operations (return errors)
├── conditions.go             # Condition checking utilities
├── conditions_test.go        # Standard Go tests for conditions
└── REFACTORING_GUIDE.md      # This guide
```

## Migration Steps

1. Identify utility functions that use Expect()
2. Refactor to return errors instead
3. Update test files to handle errors explicitly
4. Move utilities to appropriate file in testutils package
5. Run tests to verify behavior unchanged

## Benefits

- **Better error messages**: Know exactly which operation failed
- **Reusable utilities**: Can be used in standard Go tests too
- **Cleaner separation**: Test logic separate from utilities
- **Easier debugging**: Stack traces point to actual failures
- **Testable utilities**: Utilities can be tested independently
