package com.enernoc.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public interface FairExecutorService extends ExecutorService {

	void newContext();
	void clearContext();
	long getCurrentContextId();

	FairExecutorService wrapperWithNewContext();
	FairExecutorService wrapperWithCurrentContext();

	@Override
	default CompletableFuture<?> submit(Runnable task) {
		return CompletableFuture.runAsync(task, this);
	}

	@Override
	default <T> CompletableFuture<T> submit(Runnable task, T result) {
		return CompletableFuture.supplyAsync(() -> {
			task.run();
			return result;
		}, this);
	}

	@Override
	default <T> CompletableFuture<T> submit(Callable<T> task) {
		return CompletableFuture.supplyAsync(() -> {
			try {
				return task.call();
			}
			catch (Exception t) {
				// Since Supplier doesn't declare any checked exceptions, we
				// need a way to rethrow exceptions thrown from Callable.call.
				// Fortunately, supplyAsync(Supplier<T>) catches Throwable
				// from the call to Supplier.get. Here we exploit generics to
				// sneakily rethrow checked exceptions without wrapping them.
				throw Throwables.sneakyThrow(t);
			}
		}, this);
	}

}
