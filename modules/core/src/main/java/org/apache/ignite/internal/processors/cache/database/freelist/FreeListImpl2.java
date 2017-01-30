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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.DataStructure;
import org.apache.ignite.internal.processors.cache.database.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseListImpl;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jsr166.LongAdder8;

import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.writePage;

/**
 */
public class FreeListImpl2 extends DataStructure implements FreeList, ReuseList {
    /** */
    private static final int BUCKETS = 256; // Must be power of 2.

    /** */
    private static final Integer COMPLETE = Integer.MAX_VALUE;

    /** */
    private static final Integer FAIL_I = Integer.MIN_VALUE;

    /** */
    private static final Long FAIL_L = Long.MAX_VALUE;

    /** */
    private static final int MIN_PAGE_FREE_SPACE = 8;

    /** */
    private final int shift;

    /** */
    private final int MIN_SIZE_FOR_DATA_PAGE;

    private final PageHandler<CacheDataRow, Boolean> updateRow = new UpdateRow();

    /**
     *
     */
    private class UpdateRow extends PageHandler<CacheDataRow, Boolean> {
            @Override public Boolean run(Page page, PageIO iox, long pageAddr, CacheDataRow row, int itemId)
                throws IgniteCheckedException {
                DataPageIO io = (DataPageIO)iox;

                int rowSize = getRowSize(row);

                boolean updated = io.updateRow(pageAddr, itemId, pageSize(), null, row, rowSize);

                return updated;
            }
        };

    /** */
    private final PageHandler<Void, Boolean> compact = new Compact();

    /**
     *
     */
    private class Compact extends PageHandler<Void, Boolean> {
        @Override public Boolean run(Page page, PageIO iox, long pageAddr, Void row, int itemId)
            throws IgniteCheckedException {
            DataPageIO io = (DataPageIO)iox;

            int freeSpace = io.getFreeSpace(pageAddr);

            int newFreeSpace = io.compact(pageAddr, freeSpace, pageSize());

            assert freeSpace == newFreeSpace;

            if (newFreeSpace > MIN_PAGE_FREE_SPACE) {
                int newBucket = bucket(newFreeSpace);

//                    System.out.println("End compact [freeSpace=" + freeSpace +
//                        ", newSpace=" + newFreeSpace +
//                        ", b=" + newBucket + ']');

                putInBucket(newBucket, page);
            }
//                else
//                    System.out.println("End compact, no reuse [freeSpace=" + freeSpace +
//                        ", newSpace=" + newFreeSpace + ']');

            return Boolean.TRUE;
        }
    };

    /** */
    private final PageHandler<Integer, Compact2Res> compact2 = new Compact2();

    /**
     *
     */
    private class Compact2 extends PageHandler<Integer, Compact2Res> {
        @Override public Compact2Res run(Page page, PageIO iox, long pageAddr, Integer b, int reqSpace)
            throws IgniteCheckedException {
            DataPageIO io = (DataPageIO)iox;

            int freeSpace = io.getFreeSpace(pageAddr);

            int newBucket = bucket(freeSpace);

            if (newBucket == b)
                return Compact2Res.BUCKET_NOT_CHANGED;

            int newFreeSpace = io.compact(pageAddr, freeSpace, pageSize());

            assert freeSpace == newFreeSpace;

            if (newFreeSpace > MIN_PAGE_FREE_SPACE) {
                if (newFreeSpace >= reqSpace)
                    return Compact2Res.FOUND;

                putInBucket(newBucket, page);
            }

            return Compact2Res.MOVED;
        }
    };

    /** */
    private final PageHandler<CacheDataRow, Integer> writeRow = new WriteRow();

    /**
     *
     */
    private class WriteRow extends PageHandler<CacheDataRow, Integer> {
        @Override public Integer run(Page page, PageIO iox, long pageAddr, CacheDataRow row, int written)
            throws IgniteCheckedException {
            DataPageIO io = (DataPageIO)iox;

            int rowSize = getRowSize(row);
            int oldFreeSpace = io.getFreeSpace(pageAddr);

            assert oldFreeSpace > 0 : oldFreeSpace;

            // If the full row does not fit into this page write only a fragment.
            written = (written == 0 && oldFreeSpace >= rowSize) ? addRow(page, pageAddr, io, row, rowSize):
                addRowFragment(page, pageAddr, io, row, written, rowSize);

            // Reread free space after update.
            //int newFreeSpace = io.getFreeSpace(pageAddr);
            int newFreeSpace = Math.max(io.getFreeSpace(pageAddr), io.getFreeSpace2(pageAddr));

            if (newFreeSpace > MIN_PAGE_FREE_SPACE) {
                int bucket = bucket(newFreeSpace);

                putInBucket(bucket, page);
            }

            // Avoid boxing with garbage generation for usual case.
            return written == rowSize ? COMPLETE : written;
        }

            /**
             * @param page Page.
             * @param pageAddr Page address.
             * @param io IO.
             * @param row Row.
             * @param rowSize Row size.
             * @return Written size which is always equal to row size here.
             * @throws IgniteCheckedException If failed.
             */
            private int addRow(
                Page page,
                long pageAddr,
                DataPageIO io,
                CacheDataRow row,
                int rowSize
            ) throws IgniteCheckedException {
                io.addRow(pageAddr, row, rowSize, pageSize());

                return rowSize;
            }

            /**
             * @param page Page.
             * @param pageAddr Page address.
             * @param io IO.
             * @param row Row.
             * @param written Written size.
             * @param rowSize Row size.
             * @return Updated written size.
             * @throws IgniteCheckedException If failed.
             */
            private int addRowFragment(
                Page page,
                long pageAddr,
                DataPageIO io,
                CacheDataRow row,
                int written,
                int rowSize
            ) throws IgniteCheckedException {
                int payloadSize = io.addRowFragment(pageMem, pageAddr, row, written, rowSize, pageSize());

                assert payloadSize > 0 : payloadSize;

                return written + payloadSize;
            }
        };

    /** */
    private final PageHandler<Void, Long> rmvRow = new RemoveRow();

    /**
     *
     */
    private class RemoveRow extends PageHandler<Void, Long> {
        @Override public Long run(Page page, PageIO iox, long pageAddr, Void arg, int itemId)
            throws IgniteCheckedException {
            DataPageIO io = (DataPageIO)iox;

            int oldFreeSpace = io.getFreeSpace(pageAddr);

            assert oldFreeSpace >= 0 : oldFreeSpace;

            long nextLink = io.removeRow(pageAddr, itemId, pageSize());

            int newFreeSpace = io.getFreeSpace(pageAddr);

            //assert newFreeSpace > oldFreeSpace : "n=" + newFreeSpace + ", o=" + oldFreeSpace;

            if (newFreeSpace > MIN_PAGE_FREE_SPACE) {
                int curBucket = io.getBucket(pageAddr);

                assert curBucket == -1 || (curBucket >= 0 && curBucket < BUCKETS) : curBucket;

                if (curBucket >= 0) {
                    int newBucket = bucket(newFreeSpace);

                    if ((newBucket != curBucket)) {
                        int stripe = io.getStripe(pageAddr);

                        assert stripe >= 0 && stripe < STACKS_PER_BUCKET : stripe;

                        DataPageList list = (DataPageList)buckets[curBucket].get(stripe);

                        if (list != null)
                            list.needCompact = true;
                    }
                }
            }

            // For common case boxed 0L will be cached inside of Long, so no garbage will be produced.
            return nextLink;
        }
    };

    /** */
    private final int STACKS_PER_BUCKET = 5;

    /** */
    private final AtomicReferenceArray[] buckets = new AtomicReferenceArray[BUCKETS];

    final Thread compacter;

    private final ReuseListImpl reuseList;

    private LongAdder8 scanCnt = new LongAdder8();
    private LongAdder8 scanCntSuccess = new LongAdder8();
    private LongAdder8 scanBuckets = new LongAdder8();

    private LongAdder8 syncClearCnt = new LongAdder8();
    private LongAdder8 syncClearSuccess = new LongAdder8();
    private LongAdder8 syncClearBuckets = new LongAdder8();
    private LongAdder8 syncClearLists = new LongAdder8();
    private LongAdder8 syncClearPages = new LongAdder8();

    /**
     * @param cacheId Cache ID.
     * @param name Name (for debug purpose).
     * @param pageMem Page memory.
     * @param reuseList Reuse list or {@code null} if this free list will be a reuse list for itself.
     * @param wal Write ahead log manager.
     * @param metaPageId Metadata page ID.
     * @param initNew {@code True} if new metadata should be initialized.
     * @throws IgniteCheckedException If failed.
     */
    public FreeListImpl2(
        final IgniteLogger log,
        int cacheId,
        String name,
        PageMemory pageMem,
        ReuseList reuseList,
        IgniteWriteAheadLogManager wal,
        long metaPageId,
        boolean initNew) throws IgniteCheckedException {
        super(cacheId, pageMem, wal);
        this.log = log;
        this.reuseList = new ReuseListImpl(cacheId, name, pageMem, wal, 0, true);

        int pageSize = pageMem.pageSize();

        assert U.isPow2(pageSize) : "Page size must be a power of 2: " + pageSize;
        assert U.isPow2(BUCKETS);
        assert BUCKETS <= pageSize : pageSize;

        // TODO this constant is used because currently we cannot reuse data pages as index pages
        // TODO and vice-versa. It should be removed when data storage format is finalized.
        MIN_SIZE_FOR_DATA_PAGE = pageSize - DataPageIO.MIN_DATA_PAGE_OVERHEAD;

        int shift = 0;

        while (pageSize > BUCKETS) {
            shift++;
            pageSize >>>= 1;
        }

        this.shift = shift;

        for (int i = 0; i < BUCKETS; i++) {
            AtomicReferenceArray<DataPageList> stacks = new AtomicReferenceArray<>(STACKS_PER_BUCKET);

            for (int j = 0; j < STACKS_PER_BUCKET; j++)
                stacks.set(j, new DataPageList(pageMem));

            buckets[i] = stacks;
        }

        compacter = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        compact();

                        Thread.sleep(100);
                    }
                }
                catch (Exception e) {
                    if (!(e instanceof InterruptedException))
                        e.printStackTrace();
                }
            }
        });

        compacter.setName("compacter");
        compacter.setDaemon(true);

        compacter.start();

        Thread dump = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        {
                            long scanCnt0 = scanCnt.sumThenReset();

                            if (scanCnt0 > 0) {
                                long scanCntSuccess0 = scanCntSuccess.sumThenReset();
                                long scanBuckets0 = scanBuckets.sumThenReset();

                                log.info("Bucket scans [total=" + scanCnt0 +
                                    ", success=" + scanCntSuccess0 +
                                    ", success %=" + scanCntSuccess0 / (double)scanCnt0 +
                                    ", avgBuckets=" + scanBuckets0 / (double)scanCnt0 + ']');
                            }
                        }

                        long syncClearCnt0 = syncClearCnt.sumThenReset();

                        if (syncClearCnt0 > 0) {
                            long syncClearSuccess0 = syncClearSuccess.sumThenReset();
                            long syncClearBuckets0 = syncClearBuckets.sumThenReset();
                            long syncClearLists0 = syncClearLists.sumThenReset();
                            long syncClearPages0 = syncClearPages.sumThenReset();

                            log.info("Sync clear [total=" + syncClearCnt0 +
                                ", success=" + syncClearSuccess0 +
                                ", success %=" + syncClearSuccess0 / (double)syncClearCnt0 +
                                ", avgBuckets=" + syncClearBuckets0 / (double)syncClearCnt0 +
                                ", avgLists=" + syncClearLists0 / (double)syncClearCnt0 +
                                ", avgPages=" + syncClearPages0 / (double)syncClearCnt0 +
                                ']');
                        }

                        Thread.sleep(5000);
                    }
                }
                catch (Exception e) {
                    if (!(e instanceof InterruptedException))
                        e.printStackTrace();
                }
            }
        });
        dump.setDaemon(true);
        dump.start();
    }

    private void putInBucket(int bucket, Page page) throws IgniteCheckedException {
        AtomicReferenceArray<DataPageList> b = buckets[bucket];

        while (true) {
            int idx = randomInt(STACKS_PER_BUCKET);

            DataPageList list = b.get(idx);

            if (list != null) {
                //System.out.println(Thread.currentThread().getName() + " put in bucket [b=" + bucket + ", stripe=" + idx + ']');

                list.put(page, bucket, idx);

                return;
            }
        }
    }

    private Page takeFromBucket(int bucket) throws IgniteCheckedException {
        AtomicReferenceArray<DataPageList> b = buckets[bucket];

//        for (int i = 0; i < STACKS_PER_BUCKET; i++) {
//            DataPageList list = b.get(i);
//
//            if (list != null) {
//                Page page = list.take(cacheId);
//
//                if (page != null)
//                    return page;
//            }
//        }
//
//        return null;
        while (true) {
            int idx = randomInt(STACKS_PER_BUCKET);

            DataPageList list = b.get(idx);

            if (list != null)
                return list.take(cacheId);
        }
    }

    public void close() {
        compacter.interrupt();

        U.interrupt(compacter);

        try {
            U.join(compacter);
        }
        catch (IgniteInterruptedCheckedException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param freeSpace Page free space.
     * @return Bucket.
     */
    private int bucket(int freeSpace) {
        assert freeSpace > 0 : freeSpace;

        int bucket = freeSpace >>> shift;

        assert bucket >= 0 && bucket < BUCKETS : bucket;

        return bucket;
    }

    /**
     * @param part Partition.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    private Page allocateDataPage(int part) throws IgniteCheckedException {
        assert part <= PageIdAllocator.MAX_PARTITION_ID;
        assert part != PageIdAllocator.INDEX_PARTITION;

        long pageId = pageMem.allocatePage(cacheId, part, PageIdAllocator.FLAG_DATA);

        return pageMem.page(cacheId, pageId);
    }

    public void dumpState(IgniteLogger log) throws IgniteCheckedException {
        int s = (int)Math.pow(2, shift);

        for (int b = 0; b < BUCKETS; b++) {
            log.info("Bucket [idx=" + b + ", spaceFrom=" + s * b + ", spaceTo=" + s * (b + 1) + "]");

            AtomicReferenceArray<DataPageList> stacks = buckets[b];

            for (int i = 0; i < stacks.length(); i++) {
                log.info("    Stripe: " + i);

                DataPageList pageList = stacks.get(i);

                assert pageList != null;

                pageList.dumpState(cacheId, log);
            }
        }
    }

    public void compact() throws IgniteCheckedException {
        for (int b = 0; b < BUCKETS; b++)
            compactBucket(b);
    }

    private void compactBucket(int b) throws IgniteCheckedException {
        AtomicReferenceArray<DataPageList> stacks = buckets[b];

        for (int i = 0; i < stacks.length(); i++) {
            DataPageList pageList = stacks.get(i);

            if (pageList != null && pageList.needCompact) {
                boolean take = stacks.compareAndSet(i, pageList, null);

                if (take) {
                    compactStack(pageList);

                    pageList.needCompact = false;

                    stacks.set(i, pageList);
                }
            }
        }
    }

    public IgniteLogger log;

    private void compactStack(DataPageList pageList) throws IgniteCheckedException {
        Page page;

        while ((page = pageList.take(cacheId)) != null) {
            //System.out.println("Start compact [b=" + b + ", stripe=" + i + ']');

            Boolean ok = writePage(pageMem, page, this, compact, null, wal, null, 0, Boolean.FALSE);

            assert ok;
        }
    }

    private final AtomicBoolean cg = new AtomicBoolean();

    public boolean locCompact = true;

    /** {@inheritDoc} */
    @Override public void insertDataRow(CacheDataRow row) throws IgniteCheckedException {
        int rowSize = getRowSize(row);

        int written = 0;

        do {
            final int freeSpace = Math.min(MIN_SIZE_FOR_DATA_PAGE, rowSize - written);

            final int bucket = bucket(freeSpace);

            Page foundPage = null;

            int cntr = 0;

            // TODO: properly handle reuse bucket.
            for (int b = bucket; b < BUCKETS; b++) {
                cntr++;

                foundPage = takeFromBucket(b);

                if (foundPage != null)
                    break;
            }

            scanCnt.increment();
            scanBuckets.add(cntr);

            if (foundPage != null)
                scanCntSuccess.increment();

            if (foundPage == null && bucket > 0)
                foundPage = syncClear(freeSpace, bucket);

            try (Page page = foundPage == null ? allocateDataPage(row.partition()) : foundPage) {
                // If it is an existing page, we do not need to initialize it.
                DataPageIO init = foundPage == null ? DataPageIO.VERSIONS.latest() : null;

                written = writePage(pageMem, page, this, writeRow, init, wal, row, written, FAIL_I);

                assert written != FAIL_I; // We can't fail here.
            }
        }
        while (written != COMPLETE);
    }

    enum Compact2Res {
        FOUND,
        MOVED,
        BUCKET_NOT_CHANGED
    }

    private Page syncClear(final int freeSpace, final int bucket) throws IgniteCheckedException {
        if (locCompact && cg.compareAndSet(false, true)) {
            int buckets = 0;
            int lists = 0;
            int pages = 0;

            boolean res = false;

            try {
                for (int b = 0; b < bucket; b++) {
                    buckets++;

                    AtomicReferenceArray<DataPageList> stacks = this.buckets[b];

                    final Integer B = b;

                    for (int i = 0; i < STACKS_PER_BUCKET; i++) {
                        DataPageList pageList = stacks.get(i);

                        if (pageList != null && pageList.needCompact) {
                            lists++;

                            boolean take = stacks.compareAndSet(i, pageList, null);

                            if (take) {
                                Page page;

                                List<Page> mvPages = null;

                                try {
                                    while ((page = pageList.take(cacheId)) != null) {
                                        pages++;

                                        Compact2Res found = writePage(pageMem,
                                            page,
                                            this,
                                            compact2,
                                            null,
                                            wal,
                                            B,
                                            freeSpace,
                                            null);

                                        assert found != null;

                                        if (found == Compact2Res.FOUND) {
                                            res = true;

                                            return page;
                                        }
                                        else if (found == Compact2Res.BUCKET_NOT_CHANGED) {
                                            if (mvPages == null)
                                                mvPages = new ArrayList<>();

                                            mvPages.add(page);
                                        }
                                    }
                                }
                                finally {
                                    if (mvPages != null) {
                                        for (int p = 0; p < mvPages.size(); p++)
                                            pageList.put(mvPages.get(p), b, i);
                                    }

                                    if (!res)
                                        pageList.needCompact = false;

                                    stacks.set(i, pageList);
                                }
                            }
                        }
                    }
                }
            }
            finally {
                if (res)
                    syncClearSuccess.increment();

                syncClearCnt.increment();
                syncClearBuckets.add(buckets);
                syncClearLists.add(lists);
                syncClearPages.add(pages);

                cg.set(false);
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean updateDataRow(long link, CacheDataRow row) throws IgniteCheckedException {
        assert link != 0;

        long pageId = PageIdUtils.pageId(link);
        int itemId = PageIdUtils.itemId(link);

        try (Page page = pageMem.page(cacheId, pageId)) {
            Boolean updated = writePage(pageMem, page, this, updateRow, row, itemId, null);

            assert updated != null; // Can't fail here.

            return updated != null ? updated : false;
        }
    }

    /** {@inheritDoc} */
    @Override public void removeDataRowByLink(long link) throws IgniteCheckedException {
        assert link != 0;

        long pageId = PageIdUtils.pageId(link);
        int itemId = PageIdUtils.itemId(link);

        long nextLink;

        try (Page page = pageMem.page(cacheId, pageId)) {
            nextLink = writePage(pageMem, page, this, rmvRow, null, itemId, FAIL_L);

            assert nextLink != FAIL_L; // Can't fail here.
        }

        while (nextLink != 0L) {
            itemId = PageIdUtils.itemId(nextLink);
            pageId = PageIdUtils.pageId(nextLink);

            try (Page page = pageMem.page(cacheId, pageId)) {
                nextLink = writePage(pageMem, page, this, rmvRow, null, itemId, FAIL_L);

                assert nextLink != FAIL_L; // Can't fail here.
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void addForRecycle(ReuseBag bag) throws IgniteCheckedException {
        reuseList.addForRecycle(bag);
//        assert reuseList == this: "not allowed to be a reuse list";
//
//        //put(bag, null, 0L, REUSE_BUCKET);
//
//        long pageId;
//
//        while ((pageId = bag.pollFreePage()) != 0) {
//            try (Page page = pageMem.page(cacheId, pageId)) {
//                //putInBucket(REUSE_BUCKET, page);
//                buckets[REUSE_BUCKET].get(0).put(page);
//            }
//        }
    }

    /** {@inheritDoc} */
    @Override public long takeRecycledPage() throws IgniteCheckedException {
        return reuseList.takeRecycledPage();
//        assert reuseList == this: "not allowed to be a reuse list";
//
//        Page page = buckets[REUSE_BUCKET].get(0).take(cacheId);
//
//        return page != null ? page.id() : 0;
    }

    /** {@inheritDoc} */
    @Override public long recycledPagesCount() throws IgniteCheckedException {
        //assert reuseList == this: "not allowed to be a reuse list";

        return reuseList.recycledPagesCount();
    }

    /**
     * @param row Row.
     * @return Entry size on page.
     * @throws IgniteCheckedException If failed.
     */
    private static int getRowSize(CacheDataRow row) throws IgniteCheckedException {
        int keyLen = row.key().valueBytesLength(null);
        int valLen = row.value().valueBytesLength(null);

        return keyLen + valLen + CacheVersionIO.size(row.version(), false) + 8;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "FreeList [name=" + "new" + ']';
    }
}
