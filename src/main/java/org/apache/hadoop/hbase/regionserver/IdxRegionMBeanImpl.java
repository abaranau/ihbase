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

import org.apache.hadoop.hbase.HRegionInfo;

import javax.management.MBeanAttributeInfo;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import java.lang.ref.WeakReference;
import java.util.HashMap;

/**
 * A delegate MBean to get stats / poke an idx region.
 */
public class IdxRegionMBeanImpl extends StandardMBean
        implements IdxRegionMBean {

    private static final HashMap<String, String> ATTRIBUTE_DESCRIPTIONS =
            new HashMap<String, String>() {{
                put("Valid", "Indicates whether the region being exposed by " +
                        "this MBean is still alive");
            }};


    /**
     * Instantiate an new IdxRegion MBean. this is a convenience method which
     * allows the caller to avoid catching the
     * {@link javax.management.NotCompliantMBeanException}.
     *
     * @param idxRegion the region to wrap
     * @return a new instance of IdxRegionMBeanImpl
     */
    static IdxRegionMBeanImpl newIdxRegionMBeanImpl(IdxRegion idxRegion) {
        try {
            return new IdxRegionMBeanImpl(idxRegion);
        } catch (NotCompliantMBeanException e) {
            throw new IllegalStateException("Could not instantiate mbean", e);
        }
    }

    /**
     * Generate object name from the hregion info.
     *
     * @param regionInfo the region info to create the object name from.
     * @return an valid object name.
     * @throws IllegalStateException if an error occurs while generating the
     *                               object name
     */
    static ObjectName generateObjectName(HRegionInfo regionInfo)
            throws IllegalStateException {
        StringBuilder builder =
                new StringBuilder(IdxRegionMBeanImpl.class.getPackage().getName());
        builder.append(':');
        builder.append("table=");
        // according to HTableDescriptor.isLegalTableName() the table name
        // will never contain invalid parameters
        builder.append(regionInfo.getTableDesc().getNameAsString());
        builder.append(',');

        builder.append("id=");
        builder.append(regionInfo.getRegionId());
        builder.append(',');
        builder.append("type=IdxRegion");
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
     * the idx region to be garbage collected.
     */
    private WeakReference<IdxRegion> idxRegionRef;

    private IdxRegionMBeanImpl(IdxRegion idxRegion)
            throws NotCompliantMBeanException {
        super(IdxRegionMBean.class);
        idxRegionRef = new WeakReference<IdxRegion>(idxRegion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid() {
        IdxRegion region = idxRegionRef.get();
        if (region != null) {
            return !(region.isClosed() || region.isClosing());
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTotalIndexedScans() {
        IdxRegion region = idxRegionRef.get();
        if (region != null) {
            return region.getTotalIndexedScans();
        } else {
            return -1L;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long resetTotalIndexedScans() {
        IdxRegion region = idxRegionRef.get();
        if (region != null) {
            return region.resetTotalIndexedScans();
        } else {
            return -1L;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTotalNonIndexedScans() {
        IdxRegion region = idxRegionRef.get();
        if (region != null) {
            return region.getTotalNonIndexedScans();
        } else {
            return -1L;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long resetTotalNonIndexedScans() {
        IdxRegion region = idxRegionRef.get();
        if (region != null) {
            return region.resetTotalNonIndexedScans();
        } else {
            return -1L;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getNumberOfOngoingIndexedScans() {
        IdxRegion region = idxRegionRef.get();
        if (region != null) {
            return region.getNumberOfOngoingIndexedScans();
        } else {
            return -1L;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIndexBuildTimes() {
        IdxRegion region = idxRegionRef.get();
        if (region != null) {
            return toIndexBuildTimesString(region.getIndexBuildTimes());
        } else {
            return "";
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String resetIndexBuildTimes() {
        IdxRegion region = idxRegionRef.get();
        if (region != null) {
            return toIndexBuildTimesString(region.resetIndexBuildTimes());
        } else {
            return "";
        }
    }

    private String toIndexBuildTimesString(long[] buildTimes) {
        StringBuilder builder = new StringBuilder();
        for (long indexBuildTime : buildTimes) {
            if (indexBuildTime >= 0) {
                builder.append(indexBuildTime);
                builder.append(", ");
            }
        }
        return builder.length() > 2 ? builder.substring(0, builder.length() - 2) :
                "";
    }

/* StandardMBean hooks and overrides */

    @Override
    protected String getDescription(MBeanAttributeInfo info) {
        String description = ATTRIBUTE_DESCRIPTIONS.get(info.getName());
        return description == null ? super.getDescription(info) : description;
    }
}
