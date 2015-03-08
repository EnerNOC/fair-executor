package com.enernoc.util.concurrent;

import org.springframework.beans.factory.config.AbstractFactoryBean;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public final class FairExecutorServiceFactoryBean extends AbstractFactoryBean<CompletelyFairExecutorService> {

	private CompletelyFairExecutorService.Builder builder = CompletelyFairExecutorService.builder();

	@Override
	public Class<?> getObjectType() {
		return CompletelyFairExecutorService.class;
	}

	@Override
	protected CompletelyFairExecutorService createInstance() throws Exception {
		return builder.build();
	}

	public void setCorePoolSize(int corePoolSize) {
		builder.setCorePoolSize(corePoolSize);
	}

	public void setMaxPoolSize(int maxPoolSize) {
		builder.setMaxPoolSize(maxPoolSize);
	}

	public void setLeasesPerThread(int leasesPerThread) {
		builder.setLeasesPerThread(leasesPerThread);
	}

	public void setMaxLeasesPerThread(int maxLeasesPerThread) {
		builder.setMaxLeasesPerThread(maxLeasesPerThread);
	}

	public void setKeepAliveTime(long keepAliveTime) {
		builder.setKeepAliveTime(keepAliveTime);
	}

	public void setTimeUnit(TimeUnit timeUnit) {
		builder.setTimeUnit(timeUnit);
	}

	public void setThreadFactory(ThreadFactory threadFactory) {
		builder.setThreadFactory(threadFactory);
	}

	public void setThreadNameFormat(String threadNameFormat) {
		builder.setThreadNameFormat(threadNameFormat);
	}

}
