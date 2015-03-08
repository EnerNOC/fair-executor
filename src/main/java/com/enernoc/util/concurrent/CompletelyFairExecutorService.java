package com.enernoc.util.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PreDestroy;

public final class CompletelyFairExecutorService extends ThreadPoolExecutor implements FairExecutorService {

	private static final Logger LOG = LoggerFactory.getLogger(CompletelyFairExecutorService.class);

	private final int corePoolSize;
	private final int maxPoolSize;
	private final int leasesPerThread;
	private final int maxLeasesPerThread;
	private final long keepAliveTime;
	private final TimeUnit timeUnit;
	private final CompletelyFairWorkQueue workQueue;
	private final ThreadFactory threadFactory;

	public static Builder builder() {
		return new Builder();
	}

	private CompletelyFairExecutorService(
			int corePoolSize,
			int maxPoolSize,
			int leasesPerThread,
			int maxLeasesPerThread,
			long keepAliveTime,
			TimeUnit timeUnit,
			ThreadFactory threadFactory) {

		this(corePoolSize, maxPoolSize,
				leasesPerThread, maxLeasesPerThread,
				keepAliveTime, timeUnit,
				new CompletelyFairWorkQueue(leasesPerThread, maxLeasesPerThread),
				threadFactory);
	}

	private CompletelyFairExecutorService(
			int corePoolSize,
			int maxPoolSize,
			int leasesPerThread,
			int maxLeasesPerThread,
			long keepAliveTime,
			TimeUnit timeUnit,
			CompletelyFairWorkQueue workQueue,
			ThreadFactory threadFactory) {

		super(corePoolSize, maxPoolSize, keepAliveTime, timeUnit, workQueue, threadFactory);

		this.corePoolSize = corePoolSize;
		this.maxPoolSize = maxPoolSize;
		this.leasesPerThread = leasesPerThread;
		this.maxLeasesPerThread = maxLeasesPerThread;
		this.keepAliveTime = keepAliveTime;
		this.timeUnit = timeUnit;
		this.workQueue = workQueue;
		this.threadFactory = threadFactory;

		init();
	}

	public void init() {
		prestartAllCoreThreads();
	}

	@Override
	public void allowCoreThreadTimeOut(boolean value) {
		if (value) {
			throw new UnsupportedOperationException("Core thread time out is not allowable");
		}
	}

	@PreDestroy
	public void shutdownAndAwaitTermination() {
		shutdown(); // Disable new tasks from being submitted
		try {
			// Wait a while for existing tasks to terminate
			if (!awaitTermination(60, TimeUnit.SECONDS)) {
				shutdownNow(); // Cancel currently executing tasks
				// Wait a while for tasks to respond to being cancelled
				if (!awaitTermination(60, TimeUnit.SECONDS))
					LOG.error("Failed to shut down in a timely manner");
			}
		}
		catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public void newContext() {
		workQueue.newContext();
	}

	@Override
	public void clearContext() {
		workQueue.clearContext();
	}

	@Override
	public long getCurrentContextId() {
		return workQueue.getCurrentContextId();
	}

	@Override
	public FairExecutorService wrapperWithCurrentContext() {
		return new View(workQueue.wrapperWithCurrentContext());
	}

	@Override
	public FairExecutorService wrapperWithNewContext() {
		return new View(workQueue.wrapperWithNewContext());
	}

	@Override
	public CompletableFuture<?> submit(Runnable task) {
		return FairExecutorService.super.submit(task);
	}

	@Override
	public <T> CompletableFuture<T> submit(Runnable task, T result) {
		return FairExecutorService.super.submit(task, result);
	}

	@Override
	public <T> CompletableFuture<T> submit(Callable<T> task) {
		return FairExecutorService.super.submit(task);
	}

	@Override
	public void execute(Runnable command) {
		super.execute(wrapCommand(command));
	}

	protected Runnable wrapCommand(Runnable command) {
		Map<String, String> mdc = MDC.getCopyOfContextMap();
		if (mdc == null || mdc.isEmpty()) {
			// MDC is empty, no need to wrap the command
			return command;
		}

		return () -> {
			MDC.setContextMap(mdc);
			try {
				command.run();
			}
			finally {
				MDC.clear();
			}
		};
	}

	private class View extends AbstractExecutorService implements FairExecutorService {

		private final FairWorkQueue queue;

		private View(FairWorkQueue queue) {
			this.queue = queue;
		}

		@Override
		public void shutdown() {
			throw new UnsupportedOperationException("CompletelyFairExecutorService.View is write only");
		}

		@Override
		public List<Runnable> shutdownNow() {
			throw new UnsupportedOperationException("CompletelyFairExecutorService.View is write only");
		}

		@Override
		public boolean isShutdown() {
			return CompletelyFairExecutorService.this.isShutdown();
		}

		@Override
		public boolean isTerminated() {
			return CompletelyFairExecutorService.this.isTerminated();
		}

		@Override
		public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
			return CompletelyFairExecutorService.this.awaitTermination(timeout, unit);
		}

		@Override
		public void execute(Runnable command) {
			if (isShutdown()) {
				throw new RejectedExecutionException("Task " + command.toString() + " rejected from " + CompletelyFairExecutorService.this.toString() + ". Thread pool is shutdown.");
			}

			queue.offer(wrapCommand(command));
		}

		@Override
		public void newContext() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clearContext() {
			throw new UnsupportedOperationException();
		}

		@Override
		public long getCurrentContextId() {
			return queue.getCurrentContextId();
		}

		@Override
		public FairExecutorService wrapperWithNewContext() {
			return CompletelyFairExecutorService.this.wrapperWithNewContext();
		}

		@Override
		public FairExecutorService wrapperWithCurrentContext() {
			return this;
		}

		@Override
		public CompletableFuture<?> submit(Runnable task) {
			return FairExecutorService.super.submit(task);
		}

		@Override
		public <T> CompletableFuture<T> submit(Runnable task, T result) {
			return FairExecutorService.super.submit(task, result);
		}

		@Override
		public <T> CompletableFuture<T> submit(Callable<T> task) {
			return FairExecutorService.super.submit(task);
		}

	}

	public static final class Builder {

		private int corePoolSize = -1;
		private int maxPoolSize = -1;
		private int leasesPerThread = -1;
		private int maxLeasesPerThread = -1;
		private ThreadFactory threadFactory = null;
		private long keepAliveTime = Long.MAX_VALUE;
		private TimeUnit timeUnit = TimeUnit.NANOSECONDS;

		private final AtomicBoolean used = new AtomicBoolean(false);

		private Builder() {
		}

		public void setCorePoolSize(int corePoolSize) {
			this.corePoolSize = corePoolSize;
		}

		public Builder withCorePoolSize(int corePoolSize) {
			setCorePoolSize(corePoolSize);
			return this;
		}

		public void setMaxPoolSize(int maxPoolSize) {
			this.maxPoolSize = maxPoolSize;
		}

		public Builder withMaxPoolSize(int maxPoolSize) {
			setMaxPoolSize(maxPoolSize);
			return this;
		}

		public void setLeasesPerThread(int leasesPerThread) {
			this.leasesPerThread = leasesPerThread;
		}

		public Builder withLeasesPerThread(int leasesPerThread) {
			setLeasesPerThread(leasesPerThread);
			return this;
		}

		public void setMaxLeasesPerThread(int maxLeasesPerThread) {
			this.maxLeasesPerThread = maxLeasesPerThread;
		}

		public Builder withMaxLeasesPerThread(int maxLeasesPerThread) {
			setMaxLeasesPerThread(maxLeasesPerThread);
			return this;
		}

		public void setKeepAliveTime(long keepAliveTime) {
			this.keepAliveTime = keepAliveTime;
		}

		public void setKeepAliveTime(long keepAliveTime, TimeUnit unit) {
			this.keepAliveTime = keepAliveTime;
			this.timeUnit = unit;
		}

		public Builder withKeepAliveTime(long keepAliveTime) {
			setKeepAliveTime(keepAliveTime);
			return this;
		}

		public Builder withKeepAliveTime(long keepAliveTime, TimeUnit unit) {
			setKeepAliveTime(keepAliveTime, unit);
			return this;
		}

		public void setTimeUnit(TimeUnit timeUnit) {
			this.timeUnit = timeUnit;
		}

		public Builder withTimeUnit(TimeUnit timeUnit) {
			this.timeUnit = timeUnit;
			return this;
		}

		public void setThreadFactory(ThreadFactory threadFactory) {
			this.threadFactory = threadFactory;
		}

		public Builder withThreadFactory(ThreadFactory threadFactory) {
			setThreadFactory(threadFactory);
			return this;
		}

		public void setThreadNameFormat(String threadNameFormat) {
			if (threadFactory != null) {
				throw new IllegalStateException("Can't set thread name format when specifying a ThreadFactory");
			}

			this.threadFactory = new NamedThreadFactory(threadNameFormat);
		}

		public Builder withThreadNameFormat(String threadNameFormat) {
			setThreadNameFormat(threadNameFormat);
			return this;
		}

		public CompletelyFairExecutorService build() {
			if (maxPoolSize == -1) {
				maxPoolSize = corePoolSize;
			}

			if (leasesPerThread == -1) {
				leasesPerThread = corePoolSize;
			}
			if (leasesPerThread <= 0) {
				throw new IllegalArgumentException("leasesPerThread");
			}

			if (maxLeasesPerThread == -1) {
				maxLeasesPerThread = leasesPerThread;
			}
			if (maxLeasesPerThread < leasesPerThread) {
				throw new IllegalArgumentException("maxLeasesPerThread");
			}

			if (!used.compareAndSet(false, true)) {
				throw new IllegalStateException("used");
			}
			if (threadFactory == null) {
				threadFactory = Executors.defaultThreadFactory();
			}

			return new CompletelyFairExecutorService(corePoolSize, maxPoolSize, leasesPerThread, maxLeasesPerThread,
					keepAliveTime, timeUnit, threadFactory);
		}

	}

}
