package com.enernoc.util.concurrent;

import java.util.Queue;

public interface FairWorkQueue extends Queue<Runnable> {

	void newContext();
	void clearContext();
	long getCurrentContextId();

	FairWorkQueue wrapperWithNewContext();
	FairWorkQueue wrapperWithCurrentContext();

}
