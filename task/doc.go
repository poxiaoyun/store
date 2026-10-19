// Package task defines the contracts used to submit, execute, and inspect
// asynchronous tasks.
//
// A task is an imperative command: every accepted task remains eligible for
// execution until it succeeds, is canceled, or exhausts its retry policy.
// Implementations provide at-least-once execution, so a Handler may observe the
// same task more than once and must make its side effects idempotent.
//
// Submitter, Worker, and Manager are separate interfaces deliberately. Most
// application code only needs Submitter, while process startup wires a Worker
// and operational endpoints may additionally use Manager. An adapter may
// implement any combination of these interfaces.
//
// Durability is an adapter property. Production adapters should preserve an
// accepted task across process restarts; an in-memory adapter may only preserve
// it for the lifetime of the process and should document that limitation.
package task
