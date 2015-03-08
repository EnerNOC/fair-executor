package com.enernoc.util.concurrent;

import com.enernoc.util.concurrent.CompletelyFairExecutorService;
import com.enernoc.util.concurrent.FairExecutorService;
import com.enernoc.util.concurrent.NamedThreadFactory;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CompletelyFairExecutorServiceTest {

	private static final Logger LOG = LoggerFactory.getLogger(CompletelyFairExecutorServiceTest.class);

	private enum Fairness {
			UNFAIR, FAIR
	}

	@Test(expected = Exception.class)
	public void testSneakyThrow() throws Throwable {
		FairExecutorService executorService = CompletelyFairExecutorService.builder()
			.withCorePoolSize(5)
			.withLeasesPerThread(2)
			.withThreadNameFormat("fair-executor-thread-%s")
			.build();

		boolean test = true;
		CompletableFuture<String> future = executorService.submit(() -> {
			if (test) {
				throw new Exception("Checked exception");
			}
			return "";
		});

		try {
			future.join();
		}
		catch (CompletionException e) {
			throw e.getCause();
		}
	}

	@Test
	public void test() throws InterruptedException {
		Params.Builder builder = Params.builder()
				.withPoolSize(10)
				.withLeasesPerThread(2)
				.withMaxLeasesPerThread(5)
				.withGeneratorThreads(5)
				.withJobsPerThread(10)
				.withThreadDelay(20)
				.withJobTime(100);

		Result unfairResult = test(builder.withFairness(Fairness.UNFAIR).build());
		Result fairResult = test(builder.withFairness(Fairness.FAIR).build());

		System.out.println("\n\n\n\nResults:\n");
		unfairResult.print();
		fairResult.print();
	}

	@Test
	public void testView() throws InterruptedException {
		FairExecutorService executorService = CompletelyFairExecutorService.builder()
			.withCorePoolSize(5)
			.withLeasesPerThread(2)
			.withThreadNameFormat("fair-executor-thread-%s")
			.build();

		FairExecutorService executorServiceView = executorService.wrapperWithCurrentContext();

		Params params = Params.builder()
				.withGeneratorThreads(5)
				.withJobsPerThread(10)
				.withThreadDelay(20)
				.withJobTime(100)
				.build();

		ThreadFactory generatorThreadFactory = new NamedThreadFactory("generator-thread-%d");
		CountDownLatch latch = new CountDownLatch(params.getGeneratorThreads() * params.getJobsPerThread() * 2);
		for (int i = 0; i < params.getGeneratorThreads(); i++) {
			int thread = i;
			generatorThreadFactory.newThread(() -> {
				sleep(thread * params.getThreadDelay());
				LOG.trace("Generator thread {} has started", thread + 1);
				long enqueued = System.currentTimeMillis();
				for (int j = 0; j < params.getJobsPerThread(); j++) {
					int job = j;

					executorService.execute(() -> {
						long diff = System.currentTimeMillis() - enqueued;
						LOG.trace("Thread {}, job {} (normal) running in thread {} context {}, waited {} ms to execute", thread + 1, job + 1, Thread.currentThread().getName(), executorService.getCurrentContextId(), diff);
						sleep(params.getJobTime());
						long contextId = executorService.getCurrentContextId();
						executorService.execute(() -> { LOG.trace("Sub task (normal) running in thread {} context {} (parent context {})", Thread.currentThread().getName(), executorService.getCurrentContextId(), contextId); });
						latch.countDown();
					});

					executorServiceView.execute(() -> {
						long diff = System.currentTimeMillis() - enqueued;
						LOG.trace("Thread {}, job {} (view) running in thread {} context {}, waited {} ms to execute", thread + 1, job + 1, Thread.currentThread().getName(), executorServiceView.getCurrentContextId(), diff);
						sleep(params.getJobTime());
						long contextId = executorServiceView.getCurrentContextId();
						executorServiceView.execute(() -> { LOG.trace("Sub task (fair) running in thread {} context {} (parent context {})", Thread.currentThread().getName(), executorServiceView.getCurrentContextId(), contextId); });
						latch.countDown();
					});
				}
				LOG.trace("Generator thread {} has finished, ran in {} ms", thread + 1, System.currentTimeMillis() - enqueued);
			}).start();
		}

		latch.await();

		sleep(100);
	}

	public Result test(Params params) throws InterruptedException {
		LOG.debug("Running test with params: " + params.toString().replace("Params [", "").replace("]", ""));
		ExecutorService executorService;
		switch (params.getFairness()) {
			case UNFAIR:
				executorService = new ThreadPoolExecutor(
						params.getPoolSize(), params.getPoolSize(),
						Long.MAX_VALUE, TimeUnit.NANOSECONDS,
						new LinkedBlockingQueue<>(),
						new ThreadFactoryBuilder().setNameFormat("fair-executor-thread-%s").build());

				((ThreadPoolExecutor) executorService).prestartAllCoreThreads();
				break;
			case FAIR:
				executorService = CompletelyFairExecutorService.builder()
					.withCorePoolSize(params.getPoolSize())
					.withLeasesPerThread(params.getLeasesPerThread())
					.withMaxLeasesPerThread(params.getMaxLeasesPerThread())
					.withThreadNameFormat("fair-executor-thread-%s")
					.build();
				break;
			default:
				throw new IllegalStateException("fairness");
		}

		ThreadFactory generatorThreadFactory = new NamedThreadFactory("generator-thread-%d");
		Map<Integer, Long> timeToExecute = Collections.synchronizedMap(new TreeMap<>());
		CountDownLatch latch = new CountDownLatch(params.getGeneratorThreads() * params.getJobsPerThread());
		long started = System.currentTimeMillis();
		for (int i = 0; i < params.getGeneratorThreads(); i++) {
			int thread = i;
			generatorThreadFactory.newThread(() -> {
				sleep(thread * params.getThreadDelay());
				LOG.trace("Generator thread {} has started", thread + 1);
				long enqueued = System.currentTimeMillis();
				for (int j = 0; j < params.getJobsPerThread(); j++) {
					int job = j;
					executorService.execute(() -> {
						long diff = System.currentTimeMillis() - enqueued;
						timeToExecute.merge(thread, diff, (a, b) -> a + b);
						LOG.trace("Thread {}, job {} running in thread {}, waited {} ms to execute", thread + 1, job + 1, Thread.currentThread().getName(), diff);
						sleep(params.getJobTime());
						latch.countDown();
					});
				}
				LOG.trace("Generator thread {} has finished, ran in {} ms", thread + 1, System.currentTimeMillis() - enqueued);
			}).start();
		}

		latch.await();

		long diff = System.currentTimeMillis() - started;

		MoreExecutors.shutdownAndAwaitTermination(executorService, 1, TimeUnit.SECONDS);

		return new Result(params, timeToExecute, diff);
	}

	private static final void sleep(long ms) {
		if (ms == 0) {
			return;
		}
		try {
			Thread.sleep(ms);
		}
		catch (InterruptedException ex) {
			Assert.fail("sleep interrupted");
		}
	}

	private static class Params {

		private final Fairness fairness;
		private final int poolSize;
		private final int leasesPerThread;
		private final int maxLeasesPerThread;
		private final int generatorThreads;
		private final int jobsPerThread;
		private final long threadDelay;
		private final long jobTime;

		public static Builder builder() {
			return new Builder();
		}

		private Params(Fairness fairness, int poolSize, int leasesPerThread, int maxLeasesPerThread, int generatorThreads, int jobsPerThread, long threadDelay,
				long jobTime) {
			this.fairness = fairness;
			this.poolSize = poolSize;
			this.leasesPerThread = leasesPerThread;
			this.maxLeasesPerThread = maxLeasesPerThread;
			this.generatorThreads = generatorThreads;
			this.jobsPerThread = jobsPerThread;
			this.threadDelay = threadDelay;
			this.jobTime = jobTime;
		}

		public Fairness getFairness() {
			return fairness;
		}

		public int getPoolSize() {
			return poolSize;
		}

		public int getLeasesPerThread() {
			return leasesPerThread;
		}

		public int getMaxLeasesPerThread() {
			return maxLeasesPerThread;
		}

		public int getGeneratorThreads() {
			return generatorThreads;
		}

		public int getJobsPerThread() {
			return jobsPerThread;
		}

		public long getThreadDelay() {
			return threadDelay;
		}

		public long getJobTime() {
			return jobTime;
		}

		public static class Builder {

			private Fairness fairness;
			private int poolSize;
			private int leasesPerThread;
			private int maxLeasesPerThread;
			private int generatorThreads;
			private int jobsPerThread;
			private long threadDelay;
			private long jobTime;

			private Builder() {
			}

			public Params build() {
				return new Params(fairness, poolSize, leasesPerThread, maxLeasesPerThread, generatorThreads, jobsPerThread, threadDelay, jobTime);
			}

			public void setFairness(Fairness fairness) {
				this.fairness = fairness;
			}

			public Builder withFairness(Fairness fairness) {
				this.fairness = fairness;
				return this;
			}

			public void setPoolSize(int poolSize) {
				this.poolSize = poolSize;
			}

			public Builder withPoolSize(int poolSize) {
				this.poolSize = poolSize;
				return this;
			}

			public void setLeasesPerThread(int leasesPerThread) {
				this.leasesPerThread = leasesPerThread;
			}

			public Builder withLeasesPerThread(int leasesPerThread) {
				this.leasesPerThread = leasesPerThread;
				return this;
			}

			public void setMaxLeasesPerThread(int maxLeasesPerThread) {
				this.maxLeasesPerThread = maxLeasesPerThread;
			}

			public Builder withMaxLeasesPerThread(int maxLeasesPerThread) {
				this.maxLeasesPerThread = maxLeasesPerThread;
				return this;
			}

			public void setGeneratorThreads(int generatorThreads) {
				this.generatorThreads = generatorThreads;
			}

			public Builder withGeneratorThreads(int generatorThreads) {
				this.generatorThreads = generatorThreads;
				return this;
			}

			public void setJobsPerThread(int jobsPerThread) {
				this.jobsPerThread = jobsPerThread;
			}

			public Builder withJobsPerThread(int jobsPerThread) {
				this.jobsPerThread = jobsPerThread;
				return this;
			}

			public void setThreadDelay(long threadDelay) {
				this.threadDelay = threadDelay;
			}

			public Builder withThreadDelay(long threadDelay) {
				this.threadDelay = threadDelay;
				return this;
			}

			public void setJobTime(long jobTime) {
				this.jobTime = jobTime;
			}

			public Builder withJobTime(long jobTime) {
				this.jobTime = jobTime;
				return this;
			}

		}

		// CHECKSTYLE:OFF

		@Override
		public String toString() {
			return "Params ["
					+ (fairness != null ? "fairness=" + fairness + ", " : "")
					+ "poolSize=" + poolSize
					+ ", leasesPerThread=" + leasesPerThread
					+ ", maxLeasesPerThread=" + maxLeasesPerThread
					+ ", generatorThreads=" + generatorThreads
					+ ", jobsPerThread=" + jobsPerThread
					+ ", threadDelay=" + threadDelay
					+ ", jobTime=" + jobTime
					+ "]";
		}

		// CHECKSTYLE:ON

	}

	private static class Result {

		private final Params params;
		private final Map<Integer, Long> times;
		private final long total;

		public Result(Params params, Map<Integer, Long> times, long total) {
			this.params = params;
			this.times = times;
			this.total = total;
		}

		public Params getParams() {
			return params;
		}

		public Map<Integer, Long> getTimes() {
			return times;
		}

		public long getTotal() {
			return total;
		}

		public void print() {
			System.out.println("Fairness: " + params.getFairness());
			System.out.println("\t" + Joiner.on("\n\t").withKeyValueSeparator(" => ").join(times));
			StandardDeviation stddev = new StandardDeviation();
			times.values().stream().forEach(stddev::increment);
			System.out.println("\tStddev: " + stddev.getResult());
			System.out.println("\tTotal: " + total);

			double expected = (params.getGeneratorThreads() - 1) * params.getThreadDelay()
					+	((double) (params.getGeneratorThreads() * params.getJobsPerThread() * params.getJobTime()) / (double) params.getPoolSize());
			System.out.println(String.format("\tEfficiency: %.1f%%", (total / expected) * 100));
		}

		// CHECKSTYLE:OFF

		@Override
		public String toString() {
			final int maxLen = 5;
			return "Result ["
					+ (params != null ? "params=" + params + ", " : "")
					+ (times != null ? "times=" + toString(times.entrySet(), maxLen) + ", " : "")
					+ "total=" + total
					+ "]";
		}

		private String toString(Collection<?> collection, int maxLen) {
			StringBuilder builder = new StringBuilder();
			builder.append("[");
			int i = 0;
			for (Iterator<?> iterator = collection.iterator(); iterator.hasNext() && i < maxLen; i++) {
				if (i > 0) {
					builder.append(", ");
				}
				builder.append(iterator.next());
			}
			builder.append("]");
			return builder.toString();
		}

		// CHECKSTYLE:ON

	}

}
