/**
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

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.JmxHelper;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.idx.IdxColumnDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxIndexDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxScan;
import org.apache.hadoop.hbase.client.idx.exp.Expression;
import org.apache.hadoop.hbase.regionserver.idx.support.IdxClassSize;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.ObjectArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSet;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Manages the indexes for a single region.
 */
public class IdxRegionIndexManager extends IndexManager {
    private static final Logger LOG = Logger.getLogger(IdxRegionIndexManager.class);

    static final long FIXED_SIZE =
            ClassSize.align(ClassSize.OBJECT + 4 * ClassSize.REFERENCE +
                    IdxClassSize.HASHMAP + IdxClassSize.OBJECT_ARRAY_LIST +
                    Bytes.SIZEOF_LONG + ClassSize.REENTRANT_LOCK);


    private static final int DEFAULT_INITIAL_INDEX_SIZE = 1000;

    /**
     * The wrapping region.
     */
    private IdxRegion region;

    private final IdxExpressionEvaluator expressionEvaluator;

    /**
     * The index map. Each pair holds the column and qualifier.
     */
    private volatile Map<Pair<byte[], byte[]>, IdxIndex> indexMap;
    /**
     * The keys ordered by their id. The IntSet in the {@link IdxIndex} have
     * members corresponding to the indices of this array.
     */
    private volatile ObjectArrayList<KeyValue> keys;
    /**
     * The heap size.
     */
    private long heapSize;

    private static final double INDEX_SIZE_GROWTH_FACTOR = 1.1;
    private static final double BYTES_IN_MB = 1024D * 1024D;

    public IdxRegionIndexManager() {
        expressionEvaluator = new IdxExpressionEvaluator();
    }

  /**
     * Create and initialize a new index manager.
     *
     * @param region the region to connect to
     */
    @Override
    public void initialize(IdxRegion region) {
        this.region = region;
        heapSize = FIXED_SIZE;

        JmxHelper.registerMBean(
                IdxRegionIndexManagerMBeanImpl.generateObjectName(region.getRegionInfo()),
                IdxRegionIndexManagerMBeanImpl.newIdxRegionIndexManagerMBeanImpl(this));
    }

    @Override
    public void cleanup() {
        MBeanUtil.unregisterMBean(
                IdxRegionMBeanImpl.generateObjectName(region.getRegionInfo()));

    }

  /**
     * Get the list of keys for this index manager.
     *
     * @return the list of keys
     */
    public ObjectArrayList<KeyValue> getKeys() {
        return keys;
    }

    public Map<Pair<byte[], byte[]>, IdxIndex> getIndexMap() {
        return indexMap;
    }

    /**
     * Creates and populates all indexes. Bruteforce scan fetching everything
     * into memory, creating indexes out of that.
     *
     * @return total time in millis to rebuild the indexes
     * @throws IOException in case scan throws
     */
    @Override
    public Pair<Long, Callable<Void>> rebuildIndexes() throws IOException {
        long startMillis = System.currentTimeMillis();
        if (LOG.isInfoEnabled()) {
            LOG.info(String.format("Initializing index manager for region: %s", region.toString()));
        }
        heapSize = FIXED_SIZE;
        Map<Pair<byte[], byte[]>, CompleteIndexBuilder> builderTable = initIndexTable();
        // if the region is closing/closed then a fillIndex method will throw a
        // NotServingRegion exection when an attempt to obtain a scanner is made
        // NOTE: when the region is being created isClosing() returns true
        Callable<Void> work = null;
        if (!(region.isClosing() || region.isClosed()) && !builderTable.isEmpty()) {
            try {
                final ObjectArrayList<KeyValue> newKeys = fillIndex(builderTable);
                final Map<Pair<byte[], byte[]>, IdxIndex> newIndexMap = finalizeIndex(builderTable, newKeys);
                work = new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        switchIndex(newKeys, newIndexMap);
                        return null;
                    }
                };
            } catch (NotServingRegionException e) {
                // the not serving exception may also be thrown during the scan if
                // the region was closed during the scan
                LOG.warn("Aborted index initialization", e);
            }
        } else {
            work = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    switchIndex(new ObjectArrayList<KeyValue>(), Collections.<Pair<byte[], byte[]>, IdxIndex>emptyMap());
                    return null;
                }
            };
        }
        return new Pair<Long, Callable<Void>>(System.currentTimeMillis() - startMillis,
                work);
    }

    private void switchIndex(final ObjectArrayList<KeyValue> newKeys, final Map<Pair<byte[], byte[]>, IdxIndex> newIndexMap) {
        // There is no lock here because switchIndex is called in the context of a newScannerLock
        // and one scanners are created they keep a ref to the old index (which never changes).
        this.keys = newKeys;
        this.indexMap = newIndexMap;
    }

    /**
     * Initiate the index table. Read the column desciprtors, extract the index
     * descriptors from them and instantiate index builders for those columns.
     *
     * @return the initiated map of builders keyed by column:qualifer pair
     * @throws IOException thrown by {@link IdxColumnDescriptor#getIndexDescriptors(org.apache.hadoop.hbase.HColumnDescriptor)}
     */
    private Map<Pair<byte[], byte[]>, CompleteIndexBuilder> initIndexTable()
            throws IOException {
        Map<Pair<byte[], byte[]>, CompleteIndexBuilder> indexBuilders =
                new HashMap<Pair<byte[], byte[]>, CompleteIndexBuilder>();
        for (HColumnDescriptor columnDescriptor : region.getRegionInfo().getTableDesc().getColumnFamilies()) {
            Collection<IdxIndexDescriptor> indexDescriptors = IdxColumnDescriptor.getIndexDescriptors(columnDescriptor).values();

            for (IdxIndexDescriptor indexDescriptor : indexDescriptors) {
                LOG.info(String.format("Adding index for region: '%s' index: %s", region.getRegionNameAsString(), indexDescriptor.toString()));
                Pair<byte[], byte[]> key = Pair.of(columnDescriptor.getName(), indexDescriptor.getQualifierName());
                IdxIndex currentIndex = indexMap != null ? indexMap.get(key) : null;
                int initialSize = currentIndex == null ? DEFAULT_INITIAL_INDEX_SIZE : (int) Math.round(currentIndex.size() * INDEX_SIZE_GROWTH_FACTOR);
                indexBuilders.put(key, new CompleteIndexBuilder(columnDescriptor, indexDescriptor, initialSize));
            }
        }
        return indexBuilders;
    }

    /**
     * Fills the index. Scans the region for latest rows and sends key values
     * to the matching index builder
     *
     * @param builders the map of builders keyed by column:qualifer pair
     * @return the keyset (a fresh set)
     * @throws IOException may be thrown by the scan
     */
    private ObjectArrayList<KeyValue> fillIndex(Map<Pair<byte[], byte[]>,
            CompleteIndexBuilder> builders) throws IOException {
        ObjectArrayList<KeyValue> newKeys = this.keys == null ?
                new ObjectArrayList<KeyValue>() :
                // in case we already have keys in the store try to guess the new size
                new ObjectArrayList<KeyValue>(this.keys.size() + this.region.averageNumberOfMemStoreSKeys() * 2);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        InternalScanner scanner = region.getScanner(createScan(builders.keySet()));
        try {
            boolean moreRows;
            int id = 0;
            do {
                List<KeyValue> nextRow = new ArrayList<KeyValue>();
                moreRows = scanner.next(nextRow);
                if (nextRow.size() > 0) {
                    KeyValue firstOnRow = KeyValue.createFirstOnRow(nextRow.get(0).getRow());
                    newKeys.add(firstOnRow);
                    // add keyvalue to the heapsize
                    heapSize += firstOnRow.heapSize();
                    for (KeyValue keyValue : nextRow) {
                        try {
                            CompleteIndexBuilder idx = builders.get(Pair.of(keyValue.getFamily(), keyValue.getQualifier()));
                            // we must have an index since we've limited the
                            // scan to include only indexed columns
                            assert idx != null;
                            if (LOG.isTraceEnabled()) {
                                LOG.trace("About to add kv: [" + keyValue + "] with id " + id);
                            }
                            idx.addKeyValue(keyValue, id);
                        } catch (Exception e) {
                            LOG.error("Failed to add " + keyValue + " to the index", e);
                        }
                    }
                    id++;
                }
            } while (moreRows);
            stopWatch.stop();
            LOG.info("Filled indices for region: '" + region.getRegionNameAsString() + "' with " + id + " entries in " + stopWatch.toString());
            return newKeys;
        } finally {
            scanner.close();
        }
    }

    private Scan createScan(Set<Pair<byte[], byte[]>> columns) {
        Scan scan = new Scan();
        for (Pair<byte[], byte[]> column : columns) {
            scan.addColumn(column.getFirst(), column.getSecond());
        }
        return scan;
    }

    /**
     * Converts the map of builders into complete indexes, calling
     * {@link CompleteIndexBuilder#finalizeIndex(int)} on each builder.
     *
     * @param builders the map of builders
     * @param newKeys  the set of keys for the new index to be finalized
     * @return the new index map
     */
    private Map<Pair<byte[], byte[]>, IdxIndex> finalizeIndex(Map<Pair<byte[], byte[]>,
            CompleteIndexBuilder> builders, ObjectArrayList<KeyValue> newKeys) {
        Map<Pair<byte[], byte[]>, IdxIndex> newIndexes = new HashMap<Pair<byte[], byte[]>, IdxIndex>();
        for (Map.Entry<Pair<byte[], byte[]>, CompleteIndexBuilder> indexEntry : builders.entrySet()) {
            final IdxIndex index = indexEntry.getValue().finalizeIndex(newKeys.size());
            final Pair<byte[], byte[]> key = indexEntry.getKey();
            newIndexes.put(key, index);
            // adjust the heapsize
            long indexSize = ClassSize.align(ClassSize.MAP_ENTRY + ClassSize.align(ClassSize.OBJECT + 2 * ClassSize.ARRAY +
                    key.getFirst().length + key.getSecond().length) + index.heapSize());
            LOG.info(String.format("Final index size: %f mb for region: '%s' index: %s", toMb(indexSize), Bytes.toString(key.getFirst()), Bytes.toString(key.getSecond())));
            heapSize += indexSize;
        }
        LOG.info(String.format("Total index heap overhead: %f mb for region: '%s'", toMb(heapSize), region.getRegionNameAsString()));
        return newIndexes;
    }

    private double toMb(long bytes) {
        return bytes / BYTES_IN_MB;
    }

    /**
     * Create a new search context.
     *
     * @return the new search context.
     */
    public IdxSearchContext newSearchContext() {
        return new IdxSearchContext(keys, indexMap);
    }

    @Override
    public long heapSize() {
        return heapSize + expressionEvaluator.heapSize();
    }

    /**
     * Exposes the number of keys in the index manager.
     *
     * @return the number of keys.
     */
    public int getNumberOfKeys() {
        return keys.size();
    }

    /**
     * A monitoring operation which returns the byte size of a given index.
     *
     * @param columnName in [family]:[qualifier] format
     * @return the byte size of the index
     */
    public long getIndexHeapSize(String columnName) {
        String[] familyAndQualifier = columnName.split(":");
        if (familyAndQualifier != null && familyAndQualifier.length == 2) {
            Pair fqPair = Pair.of(Bytes.toBytes(familyAndQualifier[0]), Bytes.toBytes(familyAndQualifier[1]));
            IdxIndex idx = indexMap.get(fqPair);
            if (idx != null) {
                return idx.heapSize();
            }
        }
        throw new IllegalArgumentException("No index for " + columnName);
    }

    IdxRegion getRegion() {
      return region;
    }

    @Override
    public IndexScannerContext newIndexScannerContext(Scan scan) throws IOException {
      // use the expression evaluator to determine the final set of ints
      Expression expression = IdxScan.getExpression(scan);

      IdxSearchContext idxSearchContext = newSearchContext();

      IntSet matchedExpression;
      try {
          matchedExpression = expressionEvaluator.evaluate(
                  idxSearchContext, expression
          );
      } catch (RuntimeException e) {
          throw new DoNotRetryIOException(e.getMessage(), e);
      }
      if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("%s rows matched the index expression",
                  matchedExpression.size()));
      }

      return new IdxScannerContext(idxSearchContext, matchedExpression);
    }
}
