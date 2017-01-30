/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.database.freelist;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 *
 */
public class DataPageList {
    static {
        try {
            headOffset = GridUnsafe.objectFieldOffset(DataPageList.class.getDeclaredField("head"));
        }
        catch (NoSuchFieldException e) {
            throw new NoSuchFieldError();
        }
    }

    /** */
    private volatile Head head = new Head(0);

    /** */
    private static final long headOffset;

    /** */
    private final PageMemory pageMem;

    /** */
    private final DataPageIO io;

    public volatile boolean needCompact;

    /**
     * @param pageMem Page memory.
     */
    public DataPageList(PageMemory pageMem) {
        this.pageMem = pageMem;

        io = DataPageIO.VERSIONS.latest();
    }

    public void put(Page page, int bucket, int stripe) throws IgniteCheckedException {
        long pageAddr = page.pageAddress();

        io.setBucket(pageAddr, bucket);
        io.setStripe(pageAddr, stripe);

        while (true) {
            Head head = this.head;

            Head newHead = new Head(page.id());

            io.setNextPageId(pageAddr, head.pageId);

            if (GridUnsafe.compareAndSwapObject(this, headOffset, head, newHead))
                break;
        }
    }

    public Page take(int cacheId) throws IgniteCheckedException {
        while (true) {
            Head head = this.head;

            if (head.pageId == 0L)
                return null;

            Page page = pageMem.page(cacheId, head.pageId);

            long pageAddr = page.pageAddress();

            long nextPageId = io.getNextPageId(pageAddr);

            Head newHead = new Head(nextPageId);

            if (GridUnsafe.compareAndSwapObject(this, headOffset, head, newHead))
                return page;
        }
    }

    public void dumpState(int cacheId, IgniteLogger log) throws IgniteCheckedException {
        Head head = this.head;

        long pageId = head.pageId;

        if (pageId == 0) {
            log.info("        Empty");

            return;
        }

        while (pageId != 0) {
            Page page = pageMem.page(cacheId, pageId);

            long pageAddr = page.pageAddress();

            log.info("        Page [id=" + pageId + ", free=" + io.getFreeSpace(pageAddr) + ']');

            pageId = io.getNextPageId(pageAddr);
        }
    }

    /**
     *
     */
    private static class Head {
        /** */
        final long pageId;

        /**
         * @param pageId Page ID.
         */
        public Head(long pageId) {
            this.pageId = pageId;
        }
    }
}
