// Package errs defines the public error values returned by the LSM API.
//
// Errors are stable contract points for callers. Prefer errors.Is when
// comparing, since some errors may be wrapped by higher-level operations.
package errs
