/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

/**
 * An MBean exposing various region index manager ops and info.
 */
public interface IdxRegionIndexManagerMBean {
    /**
     * The number of keys in the index which is equivalent to the number of top
     * level rows in this region.
     *
     * @return the number of keys in the index.
     */
    int getNumberOfIndexedKeys();

    /**
     * The total heap size, in bytes, used by the indexes and their overhead.
     *
     * @return the total index heap size in bytes.
     */
    long getIndexesTotalHeapSize();
}
