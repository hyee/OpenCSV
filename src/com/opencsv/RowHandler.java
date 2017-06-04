package com.opencsv;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;

/**
 * Created by Will on 2017/5/27.
 */
public interface RowHandler<T> extends EventHandler<T>, LifecycleAware {
}
