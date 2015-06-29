package com.enernoc.util.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public final class CompletelyFairWorkQueue extends AbstractQueue<Runnable> implements FairWorkQueue, BlockingQueue<Runnable> {

	private static final long serialVersionUID = 1087694850619920832L;

	private static final Logger LOG = LoggerFactory.getLogger(CompletelyFairWorkQueue.class);

	private static final AtomicLong HANDLE_ID = new AtomicLong();

	private final int leasesPerThread;
	private final int maxLeasesPerThread;
	private final ThreadLocal<Handle> handles;
	private final ThreadLocal<Handle> currentHandle = new ThreadLocal<>();

	private final ReentrantLock lock = new ReentrantLock();
	private final Condition notEmpty = lock.newCondition();

	private final AtomicInteger count = new AtomicInteger();
	private final LinkedList<Handle> subqueue = new LinkedList<>();
	private final Set<Handle> activeHandles = new HashSet<>();

	public CompletelyFairWorkQueue(int leasesPerThread) {
		this(leasesPerThread, leasesPerThread);
	}

	public CompletelyFairWorkQueue(int leasesPerThread, int maxLeasesPerThread) {
		if (leasesPerThread <= 0 || maxLeasesPerThread <= 0 || maxLeasesPerThread < leasesPerThread) {
			throw new IllegalArgumentException();
		}

		this.leasesPerThread = leasesPerThread;
		this.maxLeasesPerThread = maxLeasesPerThread;

		this.handles = ThreadLocal.withInitial(() -> new Handle(Thread.currentThread().getName()));
	}

	@Override
	public Runnable poll() {
		final AtomicInteger count = this.count;
		final ReentrantLock lock = this.lock;

		Runnable runnable = null;

		lock.lock();
		try {
			runnable = dequeue();

			if (runnable != null) {
				int c = count.getAndDecrement();
				if (c > 1) {
					notEmpty.signal();
				}
			}
		}
		finally {
			lock.unlock();
		}

		return runnable;
	}

	@Override
	public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
		final AtomicInteger count = this.count;
		final ReentrantLock lock = this.lock;

		long nanos = unit.toNanos(timeout);
		Runnable runnable = null;

		lock.lockInterruptibly();
		try {
			while (count.get() == 0) {
				if (nanos <= 0) {
					return null;
				}
				nanos = notEmpty.awaitNanos(nanos);
			}

			runnable = dequeue();

			if (runnable != null) {
				int c = count.getAndDecrement();
				if (c > 1) {
					notEmpty.signal();
				}
			}
		}
		finally {
			lock.unlock();
		}

		return runnable;
	}

	@Override
	public Runnable take() throws InterruptedException {
		final AtomicInteger count = this.count;
		final ReentrantLock lock = this.lock;

		Runnable runnable = null;

		lock.lockInterruptibly();
		try {
			while (count.get() == 0) {
				notEmpty.await();
			}

			runnable = dequeue();

			if (runnable != null) {
				int c = count.getAndDecrement();
				if (c > 1) {
					notEmpty.signal();
				}
			}
		}
		finally {
			lock.unlock();
		}

		return runnable;
	}

	private Runnable dequeue() {
		return dequeue(false);
	}

	private Runnable dequeue(boolean allowSurplus) {
		if (subqueue.isEmpty()) {
			return null;
		}

		Runnable runnable = null;
		Iterator<Handle> iterator = null;
		do {
			// Common case is likely that the first item on the queue is valid, this way we avoid creating an iterator every time
			// As the saying goes: "Premature optimization is awesome and has never caused any problems ever."
			Handle handle = (iterator == null) ? subqueue.peek() : iterator.next();

			handle.lock();
			try {

				// Though the handle shouldn't be empty here, test if it is and discard.
				if (handle.isEmpty()) {
					if (iterator == null) {
						subqueue.remove(handle);

						// In this case we need to create the iterator here, otherwise
						// we call subqueue.listIterator(1) futher down, and we skip a handle
						if (!subqueue.isEmpty()) {
							iterator = subqueue.listIterator();
						}
					}
					else {
						iterator.remove();
					}

					continue;
				}

				// Is there capacity to do the work?
				if (handle.tryAcquire(allowSurplus)) {
					runnable = handle.poll();

					// Remove the handle from the queue.
					if (iterator == null) {
						subqueue.remove(handle);
					}
					else {
						iterator.remove();
					}

					// Is there more work to be done after this?
					if (handle.isEmpty()) {
						// This handle is no longer in active rotation.
						activeHandles.remove(handle);
					}
					else {
						// Put it back at the end of the queue.
						subqueue.offer(handle);
					}

					break;
				}

				// Now create the iterator
				if (iterator == null && subqueue.size() > 1) {
					iterator = subqueue.listIterator(1);
				}
			}
			finally {
				handle.unlock();
			}
		}
		while (iterator != null && iterator.hasNext());

		if (runnable == null && !allowSurplus) {
			runnable = dequeue(true);
		}

		return runnable;
	}

	@Override
	public Runnable peek() {
		throw new UnsupportedOperationException("peek");
	}

	@Override
	public boolean offer(Runnable runnable) {
		return _offer(runnable, getHandle());
	}

	private boolean _offer(Runnable runnable, Handle handle) {
		if (runnable == null) {
			throw new NullPointerException();
		}

		final ReentrantLock putLock = this.lock;
		final AtomicInteger count = this.count;
		int c = -1;

		putLock.lock();
		try {
			enqueue(runnable, handle);

			c = count.getAndIncrement();
		}
		finally {
			putLock.unlock();
		}

		if (c == 0) {
			signalNotEmpty();
		}

		return c >= 0;
	}

	@Override
	public boolean offer(Runnable runnable, long timeout, TimeUnit unit) throws InterruptedException {
		return _put(runnable);
	}

	@Override
	public void put(Runnable runnable) throws InterruptedException {
		_put(runnable);
	}

	private boolean _put(Runnable runnable) throws InterruptedException {
		if (runnable == null) {
			throw new NullPointerException();
		}

		final ReentrantLock putLock = this.lock;
		final AtomicInteger count = this.count;
		int c = -1;

		putLock.lockInterruptibly();
		try {
			enqueue(runnable, getHandle());

			c = count.getAndIncrement();
		}
		finally {
			putLock.unlock();
		}

		if (c == 0) {
			signalNotEmpty();
		}

		return c >= 0;
	}

	private boolean enqueue(Runnable runnable, Handle handle) {
		handle.lock();
		try {
			if (activeHandles.add(handle)) {
				subqueue.offer(handle);
			}

			handle.offer(runnable);
		}
		finally {
			handle.unlock();
		}

		return true;
	}

	@Override
	public int size() {
		return count.get();
	}

	@Override
	public int remainingCapacity() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void newContext() {
		setCurrentHandle(new Handle());
	}

	@Override
	public void clearContext() {
		clearCurrentHandle();
	}

	@Override
	public long getCurrentContextId() {
		return getHandle().getId();
	}

	@Override
	public FairWorkQueue wrapperWithNewContext() {
		return new View(new Handle());
	}

	@Override
	public FairWorkQueue wrapperWithCurrentContext() {
		return new View(getHandle());
	}

	private final Handle getHandle() {
		Handle handle = currentHandle.get();
		if (handle == null) {
			handle = handles.get();
		}
		return handle;
	}

	private final void setCurrentHandle(Handle handle) {
		currentHandle.set(handle);
	}

	private final void clearCurrentHandle() {
		currentHandle.remove();
	}

	private final void signalNotEmpty() {
		final ReentrantLock lock = this.lock;

		lock.lock();
		try {
			notEmpty.signal();
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public int drainTo(Collection<? super Runnable> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int drainTo(Collection<? super Runnable> c, int maxElements) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<Runnable> iterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean contains(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object[] toArray() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Runnable remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return "[CompletelyFairWorkQueue size=" + size() + "]";
	}

	private final class Handle {

		private final long id;
		private final String name;
		private final Queue<Runnable> queue = new LinkedList<>();
		private final ReentrantLock handleLock = new ReentrantLock();
		private final Semaphore leaseSemaphore = new Semaphore(leasesPerThread);
		private final Semaphore surplusLeaseSemaphore = new Semaphore(maxLeasesPerThread - leasesPerThread);
		private final AtomicInteger surplusLeasesCount = new AtomicInteger();
		private final AtomicInteger runningCount = new AtomicInteger();

		public Handle() {
			this(null);
		}

		public Handle(String name) {
			this.id = HANDLE_ID.getAndIncrement();

			if (name == null) {
				this.name = "Handle-" + this.id;
			}
			else {
				this.name = name;
			}
		}

		public long getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public boolean isEmpty() {
			return queue.isEmpty();
		}

		public void lock() {
			handleLock.lock();
		}

		public void unlock() {
			handleLock.unlock();
		}

		public boolean tryAcquire() {
			return tryAcquire(false);
		}

		public boolean tryAcquire(boolean allowSurplus) {
			// Called used when trying to obtain a permit to execute
			// head of this queue, so we don't care about fairness.
			// (tryAcquire() is non-fair, tryAcquire(int, TimeUnit) is)
			boolean acquired = leaseSemaphore.tryAcquire();
			if (acquired) {
				LOG.trace("Handle {} acquired a normal lease", name);
			}
			else if (allowSurplus) {
				acquired = surplusLeaseSemaphore.tryAcquire();
				if (acquired) {
					int count = surplusLeasesCount.incrementAndGet();
					LOG.trace("Handle {} acquired a surplus lease, {} now outstanding", name, count);
				}
			}
			return acquired;
		}

		private void release() {
			int surplusLeases = this.surplusLeasesCount.get();
			if (surplusLeases > 0) {
				for (;;) {
					surplusLeases = this.surplusLeasesCount.get();
					if (surplusLeases == 0) {
						// If surplus lease count is 0 we need to release a standard lease instead
						LOG.trace("Handle {} attempted to release a surplus lease, instead returning a normal lease", name);
						break;
					}
					else if (this.surplusLeasesCount.compareAndSet(surplusLeases, surplusLeases - 1)) {
						surplusLeaseSemaphore.release();
						LOG.trace("Handle {} released a surplus lease, {} now outstanding", name, surplusLeases - 1);
						return;
					}
				}
			}

			leaseSemaphore.release();
			LOG.trace("Handle {} released a normal lease, {} now available", name, leaseSemaphore.availablePermits());
		}

		public Runnable poll() {
			final Runnable runnable = queue.poll();

			return new Runnable() {

				@Override
				public void run() {
					try {
						setCurrentHandle(Handle.this);
						runnable.run();
					}
					finally {
						clearCurrentHandle();
						release();
					}
				}

			};
		}

		public boolean offer(Runnable runnable) {
			return queue.offer(runnable);
		}

		@Override
		public String toString() {
			return name;
		}

	}

	private final class View extends AbstractQueue<Runnable> implements FairWorkQueue {

		private final Handle handle;

		private View(Handle handle) {
			this.handle = handle;
		}

		@Override
		public boolean offer(Runnable runnable) {
			return _offer(runnable, handle);
		}

		@Override
		public Runnable poll() {
			throw new UnsupportedOperationException("CompletelyFairWorkQueue.View is write only");
		}

		@Override
		public Runnable peek() {
			throw new UnsupportedOperationException("CompletelyFairWorkQueue.View is write only");
		}

		@Override
		public Iterator<Runnable> iterator() {
			throw new UnsupportedOperationException("CompletelyFairWorkQueue.View is write only");
		}

		@Override
		public int size() {
			return 0;
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
			return handle.getId();
		}

		@Override
		public FairWorkQueue wrapperWithNewContext() {
			return CompletelyFairWorkQueue.this.wrapperWithNewContext();
		}

		@Override
		public FairWorkQueue wrapperWithCurrentContext() {
			return this;
		}

	}

}