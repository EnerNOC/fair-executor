package com.enernoc.util.concurrent;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

class NamedThreadFactory implements ThreadFactory {

	private final String format;
	private final ThreadFactory backingThreadFactory;
	private final AtomicLong count = new AtomicLong();

	public NamedThreadFactory(String format) {
		this(format, Executors.defaultThreadFactory());
	}

	public NamedThreadFactory(String format, ThreadFactory backingThreadFactory) {
		this.format = format;
		this.backingThreadFactory = backingThreadFactory;

		// Fail fast, borrowed from ThreadFactoryBuilder
		String.format(format, 0);
	}

	@Override
	public Thread newThread(Runnable runnable) {
		Thread thread = backingThreadFactory.newThread(runnable);
		thread.setName(String.format(format, count.getAndIncrement()));
		return thread;
	}

}
