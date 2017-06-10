package com.opencsv;

import com.lmax.disruptor.*;


public abstract  interface RowHandler<T> extends EventHandler<T>,LifecycleAware {
}
