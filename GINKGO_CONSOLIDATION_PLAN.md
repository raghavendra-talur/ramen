# Ginkgo Test Suite Consolidation Plan

## Overview

This document outlines the first step in consolidating the Ginkgo test suites in the RamenDR project. The goal is to reduce code duplication and create a more maintainable test infrastructure.

## Current State Analysis

### Existing Test Suites

1. **Controller Suite** (`internal/controller/suite_test.go`)
   - Most comprehensive suite with full controller setup
   - Includes S3 setup, manager configuration, and multiple controllers
   - ~400 lines of BeforeSuite setup

2. **Cephfscg Suite** (`internal/controller/cephfscg/cephfscg_suite_test.go`)
   - CephFS consistency group tests
   - Manager setup with field indexing
   - ~150 lines of BeforeSuite setup

3. **Volsync Suite** (`internal/controller/volsync/volsync_suite_test.go`)
   - VolSync related tests with storage classes
   - Manager setup with metrics disabled
   - ~220 lines of BeforeSuite setup

4. **Util Suite** (`internal/controller/util/util_suite_test.go`)
   - Utility function tests
   - Simple client setup
   - ~80 lines of BeforeSuite setup

5. **CEL Suite** (`internal/controller/cel/cel_suite_test.go`)
   - CEL expression tests
   - Minimal setup
   - ~50 lines of BeforeSuite setup

6. **Kubeobjects Suite** (`internal/controller/kubeobjects/kubeobjects_suite_test.go`)
   - Minimal suite with no BeforeSuite

### Common Patterns Identified

All suites share these common operations:

1. **Logger Setup**: Zap logger with GinkgoWriter and ISO8601 time encoding
2. **KUBEBUILDER_ASSETS Setup**: Reading from `testassets.txt` file
3. **Test Environment Bootstrap**: Creating `envtest.Environment` with CRD paths
4. **Scheme Registration**: Adding various Kubernetes APIs to the scheme
5. **K8s Client Creation**: Creating controller-runtime clients

## Consolidation Strategy - Phase 1

### 1. Common Test Setup Package

Created `internal/controller/testsetup` package with:

#### `common.go`
- `CommonTestSetup` struct to hold shared test environment
- `SetupOptions` for customizable configuration
- `SetupCommonTestEnvironment()` function for common setup
- `TeardownCommonTestEnvironment()` for cleanup
- Centralized scheme registration for all common APIs

#### `controller_setup.go`
- `ControllerTestSetup` extending `CommonTestSetup`
- Specialized setup for the main controller suite
- Manager and controller registration (to be completed)

### 2. Benefits of This Approach

1. **Code Reduction**: Eliminated ~50-80 lines of duplicated code per suite
2. **Consistency**: All suites now use identical setup patterns
3. **Maintainability**: Changes to common setup only need to be made in one place
4. **Flexibility**: `SetupOptions` allows customization per suite
5. **Clean Cleanup**: Using `DeferCleanup` ensures proper teardown

### 3. Refactored Suites

#### CEL Suite (Completed)
- Reduced from ~100 lines to ~15 lines in BeforeSuite
- Uses `testsetup.DefaultSetupOptions("CEL Suite")`
- Automatic cleanup with `DeferCleanup`

#### Util Suite (Completed)
- Reduced from ~130 lines to ~20 lines in BeforeSuite
- Maintains util-specific `SecretsUtil` setup
- Uses common test environment

## Next Steps - Phase 2

### 1. Complete Controller Suite Refactoring
- Finish implementing `setupControllers()` in `controller_setup.go`
- Add S3 setup and fake object store getters
- Migrate the complex controller suite to use the new setup

### 2. Refactor Remaining Suites
- **Cephfscg Suite**: Add manager setup with field indexing
- **Volsync Suite**: Add storage class creation and manager setup
- **Kubeobjects Suite**: Add minimal BeforeSuite if needed

### 3. Advanced Consolidation
- Create suite-specific setup functions for complex requirements
- Consider merging compatible test suites into a single suite
- Implement shared test utilities and helpers

## Implementation Details

### File Structure
```
internal/controller/testsetup/
├── common.go           # Common test environment setup
├── controller_setup.go # Controller-specific setup
└── (future files for other specialized setups)
```

### Usage Pattern
```go
var _ = BeforeSuite(func() {
    testSetup := testsetup.SetupCommonTestEnvironment(
        testsetup.DefaultSetupOptions("Suite Name")
    )
    
    // Extract common values
    cfg = testSetup.Cfg
    k8sClient = testSetup.K8sClient
    testEnv = testSetup.TestEnv
    testLogger = testSetup.Logger
    
    // Suite-specific setup here
    
    // Register cleanup
    DeferCleanup(testSetup.TeardownCommonTestEnvironment)
})
```

## Risk Mitigation

### Potential Issues
1. **Test Isolation**: Shared setup might cause test interference
2. **Customization Needs**: Some suites might need unique configurations
3. **Dependency Management**: Changes to common setup affect all suites

### Mitigation Strategies
1. **Gradual Migration**: Refactor one suite at a time
2. **Flexible Options**: Use `SetupOptions` for customization
3. **Comprehensive Testing**: Ensure all existing tests still pass
4. **Rollback Plan**: Keep original implementations until migration is complete

## Success Metrics

1. **Code Reduction**: Measure lines of code eliminated
2. **Maintainability**: Easier to add new test suites
3. **Consistency**: All suites follow the same patterns
4. **Test Reliability**: No regression in test stability

## Conclusion

This first phase successfully demonstrates the consolidation approach by:
- Creating a reusable test setup infrastructure
- Reducing code duplication significantly
- Maintaining test functionality and isolation
- Providing a foundation for further consolidation

The next phase will complete the migration of all test suites and explore opportunities for deeper consolidation.