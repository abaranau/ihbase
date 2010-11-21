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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.idx.IdxColumnDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxIndexDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import javax.management.AttributeNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.StandardMBean;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * A delegate MBean to get stats / poke an index manager implementation.
 */
public class IdxRegionIndexManagerMBeanImpl extends StandardMBean
        implements IdxRegionIndexManagerMBean {

    private static final HashMap<String, String> ATTRIBUTE_DESCRIPTIONS =
            new HashMap<String, String>() {{
                put("NumberOfIndexedKeys", "The number of keys in the index " +
                        "which is equivalent to the number of top-level rows in this region");
                put("IndexesTotalHeapSize", "The total heap size, in bytes, used by " +
                        "the indexes and their overhead");
            }};


    /**
     * Instantiate an new IdxRegionIndexManager MBean. this is a convenience method which
     * allows the caller to avoid catching the
     * {@link javax.management.NotCompliantMBeanException}.
     *
     * @param idxRegionIndexManager the index manager to wrap
     * @return a new instance of IdxRegionIndexManagerMBeanImpl
     */
    static IdxRegionIndexManagerMBeanImpl newIdxRegionIndexManagerMBeanImpl(IdxRegionIndexManager idxRegionIndexManager) {
        try {
            return new IdxRegionIndexManagerMBeanImpl(idxRegionIndexManager);
        } catch (NotCompliantMBeanException e) {
            throw new IllegalStateException("Could not instantiate mbean", e);
        }
    }

    /**
     * Generate object name from the index manager info.
     *
     * @param regionInfo the region info to create the object name from.
     * @return an valid object name.
     * @throws IllegalStateException if an error occurs while generating the
     *                               object name
     */
    static ObjectName generateObjectName(HRegionInfo regionInfo)
            throws IllegalStateException {
        StringBuilder builder =
                new StringBuilder(IdxRegionIndexManagerMBeanImpl.class.getPackage().getName());
        builder.append(':');
        builder.append("table=");
        // according to HTableDescriptor.isLegalTableName() the table name
        // will never contain invalid parameters
        builder.append(regionInfo.getTableDesc().getNameAsString());
        builder.append(',');

        builder.append("id=");
        builder.append(regionInfo.getRegionId());
        builder.append(',');
        builder.append("type=IdxRegionIndexManager");
        try {
            return ObjectName.getInstance(builder.toString());
        } catch (MalformedObjectNameException e) {
            throw new IllegalStateException("Failed to create a legal object name " +
                    "for JMX console.  Generated name was [" + builder.toString() + "]",
                    e
            );
        }
    }

    /**
     * Using a weak reference to the idx region.
     * This way a failure to clear this MBean from the mbean server won't prevent
     * the index manager to be garbage collected.
     */
    private WeakReference<IdxRegionIndexManager> idxRegionIndexManagerRef;

    private IdxRegionIndexManagerMBeanImpl(IdxRegionIndexManager idxRegionIndexManager)
            throws NotCompliantMBeanException {
        super(IdxRegionIndexManagerMBean.class);
        idxRegionIndexManagerRef = new WeakReference<IdxRegionIndexManager>(idxRegionIndexManager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfIndexedKeys() {
        IdxRegionIndexManager indexManager = idxRegionIndexManagerRef.get();
        if (indexManager != null) {
            return indexManager.getNumberOfKeys();
        } else {
            return -1;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getIndexesTotalHeapSize() {
        IdxRegionIndexManager indexManager = idxRegionIndexManagerRef.get();
        if (indexManager != null) {
            return indexManager.heapSize();
        } else {
            return -1;
        }
    }

/* StandardMBean hooks and overrides */

    @Override
    protected String getDescription(MBeanAttributeInfo info) {
        String description = ATTRIBUTE_DESCRIPTIONS.get(info.getName());
        return description == null ? super.getDescription(info) : description;
    }

    @Override
    public Object getAttribute(String attribute)
            throws AttributeNotFoundException, MBeanException, ReflectionException {
        if (attribute.endsWith(".heapSize")) {
            String columnName = attribute.substring(0, attribute.indexOf('.'));
            return getIndexHeapSize(columnName);
        }
        return super.getAttribute(attribute);
    }

    @Override
    protected void cacheMBeanInfo(MBeanInfo info) {
        IdxRegionIndexManager indexManager = idxRegionIndexManagerRef.get();
        if (indexManager != null) {
            Set<String> columnNames;
            try {
                columnNames = extractIndexedColumnNames(indexManager.getRegion().getRegionInfo());
            } catch (IOException e) {
                throw new IllegalStateException("Invalid region info for " + indexManager.getRegion());
            }
            final MBeanAttributeInfo[] existingInfos = info.getAttributes();
            for (MBeanAttributeInfo attributeInfo : existingInfos) {
                String name = attributeInfo.getName();
                if (name.indexOf('.') >= 0) {
                    columnNames.remove(name.substring(0, name.indexOf('.')));
                }
            }
            MBeanAttributeInfo[] attributeInfos = new
                    MBeanAttributeInfo[columnNames.size() + existingInfos.length];
            System.arraycopy(existingInfos, 0, attributeInfos, 0,
                    existingInfos.length);
            Iterator<String> columnNameIterator = columnNames.iterator();
            for (int i = existingInfos.length; i < attributeInfos.length; i++) {
                String name = columnNameIterator.next() + ".heapSize";
                attributeInfos[i] = new MBeanAttributeInfo(name,
                        "long", "The amount of heap space occupied by this index", true,
                        false, false);
            }
            info = new MBeanInfo(info.getClassName(), info.getDescription(),
                    attributeInfos, info.getConstructors(), info.getOperations(),
                    info.getNotifications(), info.getDescriptor());
        }
        super.cacheMBeanInfo(info);
    }

    private static Set<String> extractIndexedColumnNames(HRegionInfo regionInfo)
            throws IOException {
        Set<String> idxColumns = new HashSet<String>();
        for (HColumnDescriptor columnDescriptor :
                regionInfo.getTableDesc().getColumnFamilies()) {
            Collection<IdxIndexDescriptor> indexDescriptors =
                    IdxColumnDescriptor.getIndexDescriptors(columnDescriptor).values();
            for (IdxIndexDescriptor indexDescriptor : indexDescriptors) {
                idxColumns.add(columnDescriptor.getNameAsString() + ":" +
                        Bytes.toString(indexDescriptor.getQualifierName()));
            }
        }
        return idxColumns;
    }

    private long getIndexHeapSize(String columnName) {
        IdxRegionIndexManager indexManager = idxRegionIndexManagerRef.get();
        if (indexManager != null) {
            return indexManager.getIndexHeapSize(columnName);
        } else {
            return -1L;
        }
    }
}
