package com.opencsv;

/**
 * Created by Will on 2015/8/30.
 */
interface RowCallback {
    void execute(Object[] row) throws Exception;
}
