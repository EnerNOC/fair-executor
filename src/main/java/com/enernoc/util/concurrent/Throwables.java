package com.enernoc.util.concurrent;

final class Throwables {

	private Throwables() {
	}

	@SuppressWarnings("unchecked")
  public static <T extends Throwable> RuntimeException sneakyThrow(Throwable t) throws T {
    throw (T) t;
  }

}
