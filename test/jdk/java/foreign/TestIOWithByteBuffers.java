/*
 * Copyright (c) 2021, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

/*
 * @test
 * @modules java.base/sun.nio.ch
 *          jdk.incubator.foreign/jdk.internal.foreign
 * @run testng/othervm -Dforeign.restricted=permit TestIOWithByteBuffers
 */

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

import java.io.File;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.Channel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritePendingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.testng.Assert;
import org.testng.annotations.*;

import static java.lang.Thread.sleep;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.nio.file.StandardOpenOption.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.*;

public class TestIOWithByteBuffers {
    static final Class<IOException> IOE = IOException.class;
    static final Class<ExecutionException> EE = ExecutionException.class;
    static final Class<IllegalStateException> ISE = IllegalStateException.class;

    @DataProvider
    public Object[][] resourceScopeFactories() {
        return new Object[][] {
            { ResourceScope.newSharedScope()   },
            { ResourceScope.newConfinedScope() },
        };
    }

    // FileChannel: read/write after close
    @Test(dataProvider = "resourceScopeFactories")
    public void testFileChannelReadWriteAfterClose(ResourceScope scope) throws Exception {
        File tmp = File.createTempFile("tmp", "txt");
        tmp.deleteOnExit();
        try (var channel = FileChannel.open(tmp.toPath(), WRITE, READ)) {

            ByteBuffer bb = getSegmentBuffer(scope);
            ByteBuffer[] bbs = getSegmentBuffers(2, scope);
            scope.close();

            assertThrows(ISE, () -> channel.write(bb));
            assertThrows(ISE, () -> channel.read(bb));
            assertThrows(ISE, () -> channel.write(bbs, 0, 1));
            assertThrows(ISE, () -> channel.read(bbs, 0, 1));
        }
    }

    // SocketChannel: read/write after close
    @Test(dataProvider = "resourceScopeFactories")
    public void testSocketChannelReadWriteAfterClose(ResourceScope scope) throws Exception {
        try (var channel = SocketChannel.open();
             var server = ServerSocketChannel.open();
             var connectedChannel = connectChannels(server, channel)) {

            ByteBuffer bb = getSegmentBuffer(scope);
            ByteBuffer[] bbs = getSegmentBuffers(2, scope);
            scope.close();

            assertThrows(ISE, () -> connectedChannel.write(bb));
            assertThrows(ISE, () -> connectedChannel.read(bb));

            assertThrows(ISE, () -> connectedChannel.write(bbs, 0, 1));
            assertThrows(ISE, () -> connectedChannel.read(bbs, 0, 1));
        }
    }

    // AsyncSocketChannel: Write after close
    @Test(dataProvider = "resourceScopeFactories")
    public void testAysncSocketChannelWriteAfterClose(ResourceScope scope) throws Exception {
        try (var channel = AsynchronousSocketChannel.open();
             var server = AsynchronousServerSocketChannel.open();
             var connectedChannel = connectChannels(server, channel)) {

            ByteBuffer bb = getSegmentBuffer(scope);
            ByteBuffer[] bbs = getSegmentBuffers(2, scope);
            scope.close();
            {
                // write(ByteBuffer)
                assertWrappedException(List.of(EE, IOE, ISE), () -> connectedChannel.write(bb).get());
            }
            {
                // write(ByteBuffer, Attachment, CompletionHandler)
                var handler = new TestHandler<Integer>(null);
                connectedChannel.write(bb, (Void) null, handler);
                assertHandlerError(handler, "Already closed");
            }
            {
                // write(ByteBuffer, timeout, TimeUnit, Attachment, CompletionHandler)
                var handler = new TestHandler<Integer>(null);
                connectedChannel.write(bb, 0L, SECONDS, (Void) null, handler);
                assertHandlerError(handler, "Already closed");
            }
            {
                // write(ByteBuffer[], length, timeout, TimeUnit, Attachment, CompletionHandler)
                var handler = new TestHandler<Long>(null);
                connectedChannel.write(bbs, 0, bbs.length, 0L, SECONDS, (Void)null, handler);
                assertHandlerError(handler, "Already closed");
            }
        }
    }

    // AsyncSocketChannel: Read after close
    @Test(dataProvider = "resourceScopeFactories")
    public void testAysncSocketChannelReadAfterClose(ResourceScope scope) throws Exception {
        try (var channel = AsynchronousSocketChannel.open();
             var server = AsynchronousServerSocketChannel.open();
             var connectedChannel = connectChannels(server, channel)) {

            ByteBuffer bb = getSegmentBuffer(scope);
            ByteBuffer[] bbs = getSegmentBuffers(2, scope);
            scope.close();
            {
                // read(ByteBuffer)
                assertWrappedException(List.of(EE, IOE, ISE), () -> connectedChannel.read(bb).get());
            }
            {
                // read(ByteBuffer, Attachment, CompletionHandler)
                var handler = new TestHandler(null);
                connectedChannel.read(bb, (Void) null, handler);
                assertHandlerError(handler, "Already closed");
            }
            {
                // read(ByteBuffer, timeout, TimeUnit, Attachment, CompletionHandler)
                var handler = new TestHandler(null);
                connectedChannel.read(bb, 0L, SECONDS, (Void) null, handler);
                assertHandlerError(handler, "Already closed");
            }
            {
                // read(ByteBuffer[], length, timeout, TimeUnit, Attachment, CompletionHandler)
                var handler = new TestHandler(null);
                connectedChannel.read(bbs, 0, bbs.length, 0L, SECONDS, (Void) null, handler);
                assertHandlerError(handler, "Already closed");
            }
        }
    }

    // TODO: Future test case failing
    // TODO: Do we need test for write with Future?
    @Test(dataProvider = "resourceScopeFactories")
    public void testAsyncChannelLockAndRead(ResourceScope scope) throws Exception {
        try (var channel = AsynchronousSocketChannel.open();
            var server = AsynchronousServerSocketChannel.open();
            var connectedChannel = connectChannels(server, channel)) {

            /*
               Completion handler w/ or w/o Future returning read variant initiates
               a read that will not complete until write is made. This allows
               the 'blocking' (not correct term) read complete, which will in turn unlock the scope
               and allow it to be closed.
             */
//            {
//                // read(ByteBuffer)
//                var readBuffer = getSegmentBuffer(scope);
//                var future = connectedChannel.read(readBuffer);
//                var handler = new TestHandler<Future<Integer>>(future);
//                assertActiveScope(handler, scope);
//
//                var writeBuffer = getSegmentBuffer(scope);
//                channel.write(writeBuffer).get();
//                assertCompletion(handler, scope);
//            }
            {
                // read(ByteBuffer, timeout, TimeUnit, Attachment, CompletionHandler)
                var readBuffer = getSegmentBuffer(scope);
                var handler = new TestHandler<Integer>(null);
                connectedChannel.read(readBuffer, 30, SECONDS, null, handler);
                assertActiveScope(handler, scope);

                var writeBuffer = getSegmentBuffer(scope);
                channel.write(writeBuffer).get();
                assertCompletion(handler, scope);
            }
            {
                // read(ByteBuffer[], length, timeout, TimeUnit, Attachment, CompletionHandler)
                var readBuffers = getSegmentBuffers(1, scope);
                var handler = new TestHandler<Long>(null);
                connectedChannel.read(readBuffers, 0, readBuffers.length, 0L,
                        TimeUnit.SECONDS, (Void)null, handler);
                assertActiveScope(handler, scope);

                var writeBuffer = getSegmentBuffer(scope);
                channel.write(writeBuffer).get();
                assertCompletion(handler, scope);
            }
        }
    }

    @Test
    public void testAsyncChannelLockAndWriteConfined() throws Exception {
        try (var server = AsynchronousServerSocketChannel.open();
             var channel = AsynchronousSocketChannel.open();
             var connectedChannel = connectChannels(server, channel)) {

            var scope = ResourceScope.newConfinedScope();
            var handler = new TestHandler(null);
            ByteBuffer[] bbs = getSegmentBuffers(1, scope);
            new Thread(() ->
                        connectedChannel.write(bbs, 0, bbs.length, 0L,
                                TimeUnit.SECONDS, (Void) null, handler)).start();
                assertHandlerError(handler, "Attempted access outside owning thread");
        }
    }

    @Test
    public void testAsyncChannelLockAndWriteSharedMultipleScopes() throws Exception {
        try (var server = AsynchronousServerSocketChannel.open();
             var channel = AsynchronousSocketChannel.open();
             var connectedChannel = connectChannels(server, channel)) {

            // number of bytes written
            final AtomicLong bytesWritten = new AtomicLong(0);
            // set to true to signal that no more buffers should be written
            final AtomicBoolean continueWriting = new AtomicBoolean(true);

            // write until socket buffer is full so as to create the conditions
            // for when a write does not complete immediately
            var scope = ResourceScope.newSharedScope();
            ByteBuffer[] bbs = getSegmentBuffers(1, scope);
            channel.write(bbs, 0, bbs.length, 0L, TimeUnit.SECONDS, (Void) null,
                    new CompletionHandler<Long, Void>() {
                        public void completed(Long result, Void att) {
                            long n = result;
                            if (n <= 0)
                                throw new RuntimeException("No bytes written");
                            bytesWritten.addAndGet(n);
                            if (continueWriting.get()) {
                                ByteBuffer[] bbs = getSegmentBuffers(8, scope);
                                channel.write(bbs, 0, bbs.length, 0L, TimeUnit.SECONDS,
                                        (Void) null, this);
                            }
                        }
                        public void failed(Throwable t, Void att) {
                            fail(t.getMessage());
                        }
                    });
            // give time for socket buffer to fill up.
            Thread.sleep(5*1000);

            var exception = expectThrows(ISE, () -> scope.close());
            assertTrue(exception.getMessage().contains("Scope is acquired by"));
            assertTrue(scope.isAlive());

            // signal handler to stop further writing
            continueWriting.set(false);

            // read until done
            drainTheSwamp(connectedChannel, bytesWritten);
        }
    }

    @Test
    public void testAsyncChannelLockAndWriteSharedSingleScope() throws Exception {
        try (var server = AsynchronousServerSocketChannel.open();
             var channel = AsynchronousSocketChannel.open();
             var connectedChannel = connectChannels(server, channel)) {

            // number of bytes written
            final AtomicLong bytesWritten = new AtomicLong(0);
            // set to true to signal that no more buffers should be written
            final AtomicBoolean continueWriting = new AtomicBoolean(true);

            // write until socket buffer is full so as to create the conditions
            // for when a write does not complete immediately
            var scope = ResourceScope.newSharedScope();
            ByteBuffer[] bbs = getSegmentBuffers(1, scope);
            channel.write(bbs, 0, bbs.length, 0L, TimeUnit.SECONDS, (Void) null,
                    new CompletionHandler<Long, Void>() {
                        public void completed(Long result, Void att) {
                            long n = result;
                            if (n <= 0)
                                throw new RuntimeException("No bytes written");
                            bytesWritten.addAndGet(n);
                            if (continueWriting.get()) {
                                ByteBuffer[] bbs = getSegmentBuffers(8, scope, 7);
                                channel.write(bbs, 0, bbs.length, 0L, TimeUnit.SECONDS,
                                        (Void) null, this);
                            }
                        }
                        public void failed(Throwable t, Void att) {
                            fail(t.getMessage());
                        }
                    });
            // give time for socket buffer to fill up.
            Thread.sleep(5*1000);

            var exception = expectThrows(ISE, () -> scope.close());
            assertTrue(exception.getMessage().contains("Scope is acquired by"));
            assertTrue(scope.isAlive());

            // signal handler to stop further writing
            continueWriting.set(false);

            // read until done
            drainTheSwamp(connectedChannel, bytesWritten);
        }
    }

    // Test Helper methods

    static class TestHandler<V> implements CompletionHandler<V, Void> {
        volatile V result;
        volatile Future<Integer> future;
        volatile Throwable throwable;
        final CountDownLatch latch = new CountDownLatch(1);

        TestHandler(Future<Integer> future) {
            this.future = future;
        }
        @Override
        public void completed (V result, Void att) {
            System.out.println("Success! ");
            if (future != null) {
                try {
                     future.get();
                } catch (Exception ex) {
                    fail("Future did not return result");
                }
            } else {
                if (result instanceof Long || result instanceof Integer) {
                    long l = (long) result;
                if (l <= 0)
                    throw new RuntimeException("No bytes read");
                }
                latch.countDown();
            }
        }
        @Override
        public void failed (Throwable exc, Void att){
            System.out.println("Failed!");
            this.throwable = exc;
            System.out.println(exc.getMessage());
            latch.countDown();
            //TODO: should fail() be called here?
        }
        private V getResult() {
            return result;
        }
        private Throwable getThrowable() {
            return throwable;
        }
        private String getThrowableMessage() {
            return throwable.getMessage();
        }
        private boolean isDone() {
            if (future != null)
                return future.isDone();
        return latch.getCount() == 0;
        }
        private int checkResult() throws Exception {
            if (future != null) {
                return future.get();
            }
            latch.await();
            return -1;
        }
    }

    static SocketChannel connectChannels(ServerSocketChannel ssc, SocketChannel sc) throws Exception {
        ssc.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(),0));
        sc.connect(ssc.getLocalAddress());
        return ssc.accept();
    }

    static AsynchronousSocketChannel connectChannels(AsynchronousServerSocketChannel assc,
                                                          AsynchronousSocketChannel asc) throws Exception {
        assc.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        asc.connect(assc.getLocalAddress()).get();
        return assc.accept().get();
    }

    private ByteBuffer getSegmentBuffer(ResourceScope scope) {
        int size = 2048;
        var smeg = MemorySegment.allocateNative(size, 1, scope);
        var r = new Random();
        for (int i = 0; i < size; i++) {
            MemoryAccess.setByteAtOffset(smeg, i, ((byte) r.nextInt(255)));
        }
        return smeg.asByteBuffer();
    }

    private ByteBuffer[] getSegmentBuffers(int len, ResourceScope scope) {
        ByteBuffer[] bufs = new ByteBuffer[len];
        for (int i=0; i < len; i++)
            bufs[i] = getSegmentBuffer(scope);
        return bufs;
    }

    private ByteBuffer[] getSegmentBuffers(int len, ResourceScope scope, int index) {
        int size = 2048;
        var r = new Random();
        byte[] b = new byte[size];
        ByteBuffer[] bufs = new ByteBuffer[len];
        for (int i = 0; i < len; i++)
            if (i != index) {
                r.nextBytes(b);
                bufs[i] = ByteBuffer.wrap(b);
            } else {
                bufs[i] = getSegmentBuffer(scope);
            }
        return bufs;
    }

    private void drainTheSwamp(AsynchronousSocketChannel connectedChannel,
                               AtomicLong bytesWritten) throws Exception {
        ByteBuffer buf = ByteBuffer.allocateDirect(4096);
        long total = 0L;
        do {
            int n = connectedChannel.read(buf).get();
            if (n <= 0)
                throw new RuntimeException("No bytes read while draining buffer");
            buf.rewind();
            total += n;
        } while (total < bytesWritten.get());
    }

    // Test Assertions

    private void assertWrappedException(List<Class<? extends Exception>> exceptionList, Assert.ThrowingRunnable method) {
        var exception = expectThrows(exceptionList.get(0), method);
        // Check wrapped exceptions
        var t = exception.getCause();
        assertTrue(exceptionList.get(1).isInstance(t), "got: " + t);
        var t1 = t.getCause();
        assertTrue(exceptionList.get(2).isInstance(t1), "got: " + t);
    }

    private void assertHandlerError(TestHandler<?> handler, String failMsg) throws InterruptedException {
        handler.latch.await();
        assertTrue(ISE.isInstance(handler.getThrowable()));
        assertTrue(handler.getThrowableMessage().contains(failMsg));
        assertEquals(handler.getResult(), null);
    }

    private void assertActiveScope(TestHandler<?> handler, ResourceScope scope) {
        assertFalse(handler.isDone());
        assertTrue(scope.isAlive());

        var exception = expectThrows(ISE, () -> scope.close());
        assertTrue(exception.getMessage().contains("Scope is acquired by"));
        assertTrue(scope.isAlive());
    }

    private void assertCompletion(TestHandler<?> handler, ResourceScope scope) throws Exception {
        handler.checkResult();
        assertTrue(handler.isDone());
        assertTrue(scope.isAlive());
    }
}
