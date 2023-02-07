// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

// Package workerpool implements a concurrency limiting worker pool.
// Worker routines are spawned on demand as tasks are submitted; up to the
// configured limit of concurrent workers.
//
// When the limit of concurrently running workers is reached, submitting a task
// blocks until a worker is able to pick it up. This behavior is intentional as
// it prevents from accumulating tasks which could grow unbounded. Therefore,
// it is the responsibility of the caller to queue up tasks if that's the
// intended behavior.
//
// One caveat is that while the number of concurrently running workers is
// limited, task results are not and they accumulate until they are collected.
// Therefore, if a large number of tasks can be expected, the workerpool should
// be periodically drained (e.g. every 10k tasks).
package workerpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrDraining is returned when an operation is not possible because
	// draining is in progress.
	ErrDraining = errors.New("drain operation in progress")
	// ErrClosed is returned when operations are attempted after a call to Close.
	ErrClosed = errors.New("worker pool is closed")
)

// WorkerPool spawns, on demand, a number of worker routines to process
// submitted tasks concurrently. The number of concurrent routines never
// exceeds the specified limit.
type WorkerPool struct {
	workerSemaphore chan struct{}
	tasks           chan *task
	results         []Task
	wg              sync.WaitGroup

	mu       sync.Mutex
	draining bool
	closed   bool
}

// New creates a new pool of workers where at most n workers process submitted
// tasks concurrently. New panics if n ≤ 0.
func New(n int) *WorkerPool {
	if n <= 0 {
		panic(fmt.Sprintf("workerpool.New: n must be > 0, got %d", n))
	}
	wp := &WorkerPool{
		workerSemaphore: make(chan struct{}, n),
		tasks:           make(chan *task),
	}
	return wp
}

// Cap returns the concurrent workers capacity, see New().
func (wp *WorkerPool) Cap() int {
	return cap(wp.workerSemaphore)
}

// Len returns the count of concurrent workers currently running.
func (wp *WorkerPool) Len() int {
	return len(wp.workerSemaphore)
}

// Submit submits f for processing by a worker. The given id is useful for
// identifying the task once it is completed. The task f must return when the
// context ctx is cancelled. The context passed to task f is cancelled when
// Close is called.
//
// Submit blocks until a routine start processing the task.
//
// If a drain operation is in progress, ErrDraining is returned and the task
// is not submitted for processing.
// If the worker pool is closed, ErrClosed is returned and the task is not
// submitted for processing.
func (wp *WorkerPool) Submit(ctx context.Context, id string, f func(ctx context.Context) error) error {
	wp.mu.Lock()
	if wp.closed {
		wp.mu.Unlock()
		return ErrClosed
	}
	if wp.draining {
		wp.mu.Unlock()
		return ErrDraining
	}
	wp.wg.Add(1)
	wp.mu.Unlock()
	wp.tasks <- &task{
		id:  id,
		run: f,
	}
	return nil
}

// Drain waits until all tasks are completed. This operation prevents
// submitting new tasks to the worker pool. Drain returns the results of the
// tasks that have been processed.
// If a drain operation is already in progress, ErrDraining is returned.
// If the worker pool is closed, ErrClosed is returned.
func (wp *WorkerPool) Drain(ctx context.Context) ([]Task, error) {
	wp.mu.Lock()
	if wp.closed {
		wp.mu.Unlock()
		return nil, ErrClosed
	}
	if wp.draining {
		wp.mu.Unlock()
		return nil, ErrDraining
	}
	wp.draining = true
	wp.mu.Unlock()

	done := make(chan struct{})
	go func() {
		// TODO(chance): this go routine may leak if a task never returns
		wp.wg.Wait()
		close(done)
	}()

	// wait for context cancellation or for the go routine above to return,
	// indicating all tasks are finished
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-done:
	}

	// NOTE: It's not necessary to hold a lock when reading or writing
	// wp.results as no other routine is running at this point besides the
	// "run" routine which should be waiting on the tasks channel.
	res := wp.results
	wp.results = nil

	wp.mu.Lock()
	wp.draining = false
	wp.mu.Unlock()

	return res, nil
}

// Close closes the worker pool, rendering it unable to process new tasks.
// Close sends the cancellation signal to any running task and waits for all
// workers, if any, to return.
// Close will return ErrClosed if it has already been called.
func (wp *WorkerPool) Close(ctx context.Context) error {
	wp.mu.Lock()
	if wp.closed {
		wp.mu.Unlock()
		return ErrClosed
	}
	wp.closed = true
	wp.mu.Unlock()

	// Signal to Run that we should stop starting new tasks. This triggers Run()
	// to cancel the task context of each task.
	close(wp.tasks)

	done := make(chan struct{})
	go func() {
		// TODO(chance): this go routine may leak if a task never returns
		wp.wg.Wait()
		close(done)
	}()

	// wait for context cancellation or for the go routine above to return,
	// indicating all tasks are finished
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

// Run starts processing pending tasks.
func (wp *WorkerPool) Run(ctx context.Context) error {
	defer close(wp.workerSemaphore)

	// If run returns, we should cancel the context for the running tasks
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for t := range wp.tasks {
		t := t
		result := taskResult{id: t.id}
		wp.results = append(wp.results, &result)

		// check for context cancellation while trying to obtain the worker
		// semaphore
		select {
		case wp.workerSemaphore <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}

		go func() {
			defer wp.wg.Done()
			if t.run != nil {
				result.err = t.run(ctx)
			}
			<-wp.workerSemaphore
		}()
	}
	return nil
}
