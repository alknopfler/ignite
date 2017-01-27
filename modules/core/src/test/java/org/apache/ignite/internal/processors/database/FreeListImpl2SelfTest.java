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

package org.apache.ignite.internal.processors.database;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.freelist.DataPageList;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeListImpl;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeListImpl2;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class FreeListImpl2SelfTest extends GridCommonAbstractTest {
    /** */
    private static final int CPUS = Runtime.getRuntime().availableProcessors();

    /** */
    private static final long MB = 1024L * 1024L;

    /** */
    private PageMemoryNoStoreImpl pageMem;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        if (pageMem != null)
            pageMem.stop();

        pageMem = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCompact() throws Exception {
        pageMem = createPageMemory(1024);

        FreeListImpl2 fl = new FreeListImpl2(1, "freelist", pageMem, null, null, 0, true);

        for (int iter = 0; iter < 100_000; iter++) {
            System.out.println("Iter: " + iter + ", allocated=" + pageMem.loadedPages());

            List<Long> links = new ArrayList<>();

            for (int i = 0; i < 1000; i++) {
                TestDataRow row = new TestDataRow(64, 64);

                fl.insertDataRow(row);

                links.add(row.link());
            }

            for (Long link : links)
                fl.removeDataRowByLink(link);
        }

        fl.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCompact1() throws Exception {
        pageMem = createPageMemory(1024);

        FreeListImpl2 fl = new FreeListImpl2(1, "freelist", pageMem, null, null, 0, true);

        for (int iter = 0; iter < 1; iter++) {
            System.out.println("Iter: " + iter + ", allocated=" + pageMem.loadedPages());

            List<Long> links = new ArrayList<>();

            for (int i = 0; i < 100_000; i++) {
                TestDataRow row = new TestDataRow(64, 64);

                fl.insertDataRow(row);

                links.add(row.link());
            }

            for (Long link : links)
                fl.removeDataRowByLink(link);

            fl.locCompact = true;

            TestDataRow row = new TestDataRow(64, 64);

            fl.insertDataRow(row);
        }

        fl.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStack() throws Exception {
        pageMem = createPageMemory(1024);

        DataPageList pl = new DataPageList(pageMem);

        Deque<Long> ids = new LinkedList<>();

        for (int i = 0; i < 20_000; i++) {
            long id = pageMem.allocatePage(1, 1, PageIdAllocator.FLAG_DATA);

            assert id != 0;

            pl.put(pageMem.page(1, id));

            ids.push(id);
        }

        for (int i = 0; i < 20_000; i++) {
            Page page = pl.take(1);
            Long id = ids.pop();

            assertEquals((Long)page.id(), id);
        }

        assertNull(pl.take(1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testStackConcurrent1() throws Exception {
        pageMem = createPageMemory(1024);

        final DataPageList pl = new DataPageList(pageMem);

        Set<Long> ids = new HashSet<>();

        for (int i = 0; i < 100_000; i++) {
            long id = pageMem.allocatePage(1, 1, PageIdAllocator.FLAG_DATA);

            ids.add(id);
        }

        for (int i = 0; i < 100; i++) {
            System.out.println("Iter: " + i);

            for (Long id : ids)
                pl.put(pageMem.page(1, id));

            final int THREADS = 16;

            final CyclicBarrier b = new CyclicBarrier(THREADS);

            final ConcurrentHashSet<Long> taken = new ConcurrentHashSet<>();

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    b.await();

                    for (int i = 0; i < 20_000; i++) {
                        Page page = pl.take(1);

                        if (page != null)
                            taken.add(page.id());
                    }

                    Page page;

                    while ((page = pl.take(1)) != null)
                        taken.add(page.id());

                    return null;
                }
            }, THREADS, "take");

            assertEquals(ids.size(), taken.size());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStackConcurrent2() throws Exception {
        pageMem = createPageMemory(1024);

        final DataPageList pl = new DataPageList(pageMem);

        Set<Long> ids = new HashSet<>();

        for (int i = 0; i < 100_000; i++) {
            long id = pageMem.allocatePage(1, 1, PageIdAllocator.FLAG_DATA);

            ids.add(id);
        }

        for (int i = 0; i < 100; i++) {
            System.out.println("Iter: " + i);

            for (Long id : ids)
                pl.put(pageMem.page(1, id));

            final int THREADS = 16;

            final CyclicBarrier b = new CyclicBarrier(THREADS);

            final AtomicLong takeCnt = new AtomicLong();

            final AtomicLong putCnt = new AtomicLong(ids.size());

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    b.await();

                    for (int i = 0; i < 200_000; i++) {
                        Page page = pl.take(1);

                        if (page != null) {
                            takeCnt.incrementAndGet();

                            if (ThreadLocalRandom.current().nextBoolean()) {
                                pl.put(page);

                                putCnt.incrementAndGet();
                            }
                        }
                    }

                    Page page;

                    while ((page = pl.take(1)) != null)
                        takeCnt.incrementAndGet();

                    return null;
                }
            }, THREADS, "take");

            assertEquals(putCnt.get(), takeCnt.get());
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testInsertDeleteSingleThreaded_1024() throws Exception {
        checkInsertDeleteSingleThreaded(1024);
    }

    /**
     * @throws Exception if failed.
     */
    public void testInsertDeleteSingleThreaded_2048() throws Exception {
        checkInsertDeleteSingleThreaded(2048);
    }

    /**
     * @throws Exception if failed.
     */
    public void testInsertDeleteSingleThreaded_4096() throws Exception {
        checkInsertDeleteSingleThreaded(4096);
    }

    /**
     * @throws Exception if failed.
     */
    public void testInsertDeleteSingleThreaded_8192() throws Exception {
        checkInsertDeleteSingleThreaded(8192);
    }

    /**
     * @throws Exception if failed.
     */
    public void testInsertDeleteSingleThreaded_16384() throws Exception {
        checkInsertDeleteSingleThreaded(16384);
    }

    /**
     * @throws Exception if failed.
     */
    public void testInsertDeleteMultiThreaded_1024() throws Exception {
        checkInsertDeleteMultiThreaded(1024);
    }

    /**
     * @throws Exception if failed.
     */
    public void testInsertDeleteMultiThreaded_2048() throws Exception {
        checkInsertDeleteMultiThreaded(2048);
    }

    /**
     * @throws Exception if failed.
     */
    public void testInsertDeleteMultiThreaded_4096() throws Exception {
        checkInsertDeleteMultiThreaded(4096);
    }

    /**
     * @throws Exception if failed.
     */
    public void testInsertDeleteMultiThreaded_8192() throws Exception {
        checkInsertDeleteMultiThreaded(8192);
    }

    /**
     * @throws Exception if failed.
     */
    public void testInsertDeleteMultiThreaded_16384() throws Exception {
        checkInsertDeleteMultiThreaded(16384);
    }

    /**
     * @param pageSize Page size.
     * @throws Exception
     */
    protected void checkInsertDeleteMultiThreaded(final int pageSize) throws Exception {
        final FreeList list = createFreeList(pageSize);

        Random rnd = new Random();

        final ConcurrentMap<Long, TestDataRow> stored = new ConcurrentHashMap<>();

        for (int i = 0; i < 100; i++) {
            int keySize = rnd.nextInt(pageSize * 3 / 2) + 10;
            int valSize = rnd.nextInt(pageSize * 5 / 2) + 10;

            TestDataRow row = new TestDataRow(keySize, valSize);

            list.insertDataRow(row);

            assertTrue(row.link() != 0L);

            TestDataRow old = stored.put(row.link(), row);

            assertNull(old);
        }

        final AtomicBoolean grow = new AtomicBoolean(true);

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Random rnd = ThreadLocalRandom.current();

                for (int i = 0; i < 1_000_000; i++) {
                    boolean grow0 = grow.get();

                    if (grow0) {
                        if (stored.size() > 20_000) {
                            if (grow.compareAndSet(true, false))
                                info("Shrink... [" + stored.size() + ']');

                            grow0 = false;
                        }
                    }
                    else {
                        if (stored.size() < 1_000) {
                            if (grow.compareAndSet(false, true))
                                info("Grow... [" + stored.size() + ']');

                            grow0 = true;
                        }
                    }

                    boolean insert = rnd.nextInt(100) < 70 == grow0;

                    if (insert) {
                        int keySize = rnd.nextInt(pageSize * 3 / 2) + 10;
                        int valSize = rnd.nextInt(pageSize * 3 / 2) + 10;

                        TestDataRow row = new TestDataRow(keySize, valSize);

                        list.insertDataRow(row);

                        assertTrue(row.link() != 0L);

                        TestDataRow old = stored.put(row.link(), row);

                        assertNull(old);
                    }
                    else {
                        while (true) {
                            Iterator<TestDataRow> it = stored.values().iterator();

                            if (it.hasNext()) {
                                TestDataRow row = it.next();

                                TestDataRow rmvd = stored.remove(row.link);

                                if (rmvd != null) {
                                    list.removeDataRowByLink(row.link);

                                    break;
                                }
                            }
                        }
                    }
                }

                return null;
            }
        }, 8, "runner");
    }

    /**
     * @throws Exception if failed.
     */
    protected void checkInsertDeleteSingleThreaded(int pageSize) throws Exception {
        FreeList list = createFreeList(pageSize);

        Random rnd = new Random();

        Map<Long, TestDataRow> stored = new HashMap<>();

        for (int i = 0; i < 100; i++) {
            int keySize = rnd.nextInt(pageSize * 3 / 2) + 10;
            int valSize = rnd.nextInt(pageSize * 5 / 2) + 10;

            TestDataRow row = new TestDataRow(keySize, valSize);

            list.insertDataRow(row);

            assertTrue(row.link() != 0L);

            TestDataRow old = stored.put(row.link(), row);

            assertNull(old);
        }

        boolean grow = true;

        for (int i = 0; i < 1_000_000; i++) {
            if (grow) {
                if (stored.size() > 20_000) {
                    grow = false;

                    info("Shrink... [" + stored.size() + ']');
                }
            }
            else {
                if (stored.size() < 1_000) {
                    grow = true;

                    info("Grow... [" + stored.size() + ']');
                }
            }

            boolean insert = rnd.nextInt(100) < 70 == grow;

            if (insert) {
                int keySize = rnd.nextInt(pageSize * 3 / 2) + 10;
                int valSize = rnd.nextInt(pageSize * 3 / 2) + 10;

                TestDataRow row = new TestDataRow(keySize, valSize);

                list.insertDataRow(row);

                assertTrue(row.link() != 0L);

                TestDataRow old = stored.put(row.link(), row);

                assertNull(old);
            }
            else {
                Iterator<TestDataRow> it = stored.values().iterator();

                if (it.hasNext()) {
                    TestDataRow row = it.next();

                    TestDataRow rmvd = stored.remove(row.link);

                    assertTrue(rmvd == row);

                    list.removeDataRowByLink(row.link);
                }
            }
        }
    }

    /**
     * @return Page memory.
     */
    protected PageMemoryNoStoreImpl createPageMemory(int pageSize) throws Exception {
        long[] sizes = new long[CPUS];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = 1024 * MB / CPUS;

        PageMemoryNoStoreImpl pageMem = new PageMemoryNoStoreImpl(log, new UnsafeMemoryProvider(sizes), null, pageSize, true);

        pageMem.start();

        return pageMem;
    }

    /**
     * @param pageSize Page size.
     * @return Free list.
     * @throws Exception If failed.
     */
    protected FreeList createFreeList(int pageSize) throws Exception {
        pageMem = createPageMemory(pageSize);

        long metaPageId = pageMem.allocatePage(1, 1, PageIdAllocator.FLAG_DATA);

        return new FreeListImpl(1, "freelist", pageMem, null, null, metaPageId, true);
    }

    /**
     *
     */
    private static class TestDataRow implements CacheDataRow {
        /** */
        private long link;

        /** */
        private TestCacheObject key;

        /** */
        private TestCacheObject val;

        /** */
        private GridCacheVersion ver;

        /**
         * @param keySize Key size.
         * @param valSize Value size.
         */
        private TestDataRow(int keySize, int valSize) {
            key = new TestCacheObject(keySize);
            val = new TestCacheObject(valSize);
            ver = new GridCacheVersion(keySize, valSize, 0L, 1);
        }

        /** {@inheritDoc} */
        @Override public KeyCacheObject key() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public CacheObject value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public GridCacheVersion version() {
            return ver;
        }

        /** {@inheritDoc} */
        @Override public long expireTime() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int partition() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long link() {
            return link;
        }

        /** {@inheritDoc} */
        @Override public void link(long link) {
            this.link = link;
        }

        /** {@inheritDoc} */
        @Override public int hash() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     *
     */
    private static class TestCacheObject implements KeyCacheObject {
        /** */
        private byte[] data;

        /**
         * @param size Object size.
         */
        private TestCacheObject(int size) {
            data = new byte[size];

            Arrays.fill(data, (byte)size);
        }

        /** {@inheritDoc} */
        @Override public boolean internal() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public int partition() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void partition(int part) {
            assert false;
        }

        /** {@inheritDoc} */
        @Override public KeyCacheObject copy(int part) {
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> T value(CacheObjectContext ctx, boolean cpy) {
            return (T)data;
        }

        /** {@inheritDoc} */
        @Override public byte[] valueBytes(CacheObjectContext ctx) throws IgniteCheckedException {
            return data;
        }

        /** {@inheritDoc} */
        @Override public int valueBytesLength(CacheObjectContext ctx) throws IgniteCheckedException {
            return data.length;
        }

        /** {@inheritDoc} */
        @Override public boolean putValue(ByteBuffer buf) throws IgniteCheckedException {
            buf.put(data);

            return true;
        }

        /** {@inheritDoc} */
        @Override public int putValue(long addr) throws IgniteCheckedException {
            PageUtils.putBytes(addr, 0, data);

            return data.length;
        }

        /** {@inheritDoc} */
        @Override public boolean putValue(ByteBuffer buf, int off, int len) throws IgniteCheckedException {
            buf.put(data, off, len);

            return true;
        }

        /** {@inheritDoc} */
        @Override public byte cacheObjectType() {
            return 42;
        }

        /** {@inheritDoc} */
        @Override public boolean isPlatformType() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
            assert false;

            return this;
        }

        /** {@inheritDoc} */
        @Override public void finishUnmarshal(CacheObjectContext ctx, ClassLoader ldr) throws IgniteCheckedException {
            assert false;
        }

        /** {@inheritDoc} */
        @Override public void prepareMarshal(CacheObjectContext ctx) throws IgniteCheckedException {
            assert false;
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            assert false;

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            assert false;

            return false;
        }

        /** {@inheritDoc} */
        @Override public byte directType() {
            assert false;

            return 0;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            assert false;

            return 0;
        }

        /** {@inheritDoc} */
        @Override public void onAckReceived() {
            assert false;
        }
    }
}
