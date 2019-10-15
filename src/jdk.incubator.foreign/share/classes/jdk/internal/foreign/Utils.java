/*
 *  Copyright (c) 2019, Oracle and/or its affiliates. All rights reserved.
 *  DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 *  This code is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License version 2 only, as
 *  published by the Free Software Foundation.  Oracle designates this
 *  particular file as subject to the "Classpath" exception as provided
 *  by Oracle in the LICENSE file that accompanied this code.
 *
 *  This code is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  version 2 for more details (a copy is included in the LICENSE file that
 *  accompanied this code).
 *
 *  You should have received a copy of the GNU General Public License version
 *  2 along with this work; if not, write to the Free Software Foundation,
 *  Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 *   Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 *  or visit www.oracle.com if you need additional information or have any
 *  questions.
 *
 */

package jdk.internal.foreign;

import jdk.incubator.foreign.MemorySegment;
import jdk.internal.access.JavaNioAccess;
import jdk.internal.access.SharedSecrets;
import jdk.internal.access.foreign.UnmapperProxy;
import jdk.internal.foreign.MemoryScope.ConfinedScope;
import jdk.internal.misc.Unsafe;
import sun.nio.ch.FileChannelImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Supplier;

public final class Utils {

    private static Unsafe unsafe = Unsafe.getUnsafe();

    // The maximum alignment supported by malloc - typically 16 on 64-bit platforms.
    private final static long MAX_ALIGN = 16;

    private static final JavaNioAccess javaNioAccess = SharedSecrets.getJavaNioAccess();

    public static long alignUp(long n, long alignment) {
        return (n + alignment - 1) & -alignment;
    }

    public static long bitsToBytesOrThrow(long bits, Supplier<RuntimeException> exFactory) {
        if (bits % 8 == 0) {
            return bits / 8;
        } else {
            throw exFactory.get();
        }
    }

    // segment factories

    public static MemorySegment makeNativeSegment(long bytesSize, long alignmentBytes) {
        long alignedSize = bytesSize;

        if (alignmentBytes > MAX_ALIGN) {
            alignedSize = bytesSize + (alignmentBytes - 1);
        }

        long buf = unsafe.allocateMemory(alignedSize);
        long alignedBuf = Utils.alignUp(buf, alignmentBytes);
        MemoryScope scope = new ConfinedScope(null, Thread.currentThread(), () -> unsafe.freeMemory(buf));
        MemorySegment segment = new MemorySegmentImpl(buf, null, alignedSize, 0, scope);
        if (alignedBuf != buf) {
            long delta = alignedBuf - buf;
            segment = segment.slice(delta, bytesSize);
        }
        return segment;
    }

    public static MemorySegment makeArraySegment(byte[] arr) {
        return makeArraySegment(arr, arr.length, Unsafe.ARRAY_BYTE_BASE_OFFSET, Unsafe.ARRAY_BYTE_INDEX_SCALE);
    }

    public static MemorySegment makeArraySegment(char[] arr) {
        return makeArraySegment(arr, arr.length, Unsafe.ARRAY_CHAR_BASE_OFFSET, Unsafe.ARRAY_CHAR_INDEX_SCALE);
    }

    public static MemorySegment makeArraySegment(short[] arr) {
        return makeArraySegment(arr, arr.length, Unsafe.ARRAY_SHORT_BASE_OFFSET, Unsafe.ARRAY_SHORT_INDEX_SCALE);
    }

    public static MemorySegment makeArraySegment(int[] arr) {
        return makeArraySegment(arr, arr.length, Unsafe.ARRAY_INT_BASE_OFFSET, Unsafe.ARRAY_INT_INDEX_SCALE);
    }

    public static MemorySegment makeArraySegment(float[] arr) {
        return makeArraySegment(arr, arr.length, Unsafe.ARRAY_FLOAT_BASE_OFFSET, Unsafe.ARRAY_FLOAT_INDEX_SCALE);
    }

    public static MemorySegment makeArraySegment(long[] arr) {
        return makeArraySegment(arr, arr.length, Unsafe.ARRAY_LONG_BASE_OFFSET, Unsafe.ARRAY_LONG_INDEX_SCALE);
    }

    public static MemorySegment makeArraySegment(double[] arr) {
        return makeArraySegment(arr, arr.length, Unsafe.ARRAY_DOUBLE_BASE_OFFSET, Unsafe.ARRAY_DOUBLE_INDEX_SCALE);
    }

    private static MemorySegment makeArraySegment(Object arr, int size, int base, int scale) {
        MemoryScope scope = new ConfinedScope(null, Thread.currentThread(), null);
        return new MemorySegmentImpl(base, arr, size * scale, 0, scope);
    }

    public static MemorySegment makeBufferSegment(ByteBuffer bb) {
        long bbAddress = javaNioAccess.getBufferAddress(bb);
        Object base = javaNioAccess.getBufferBase(bb);

        int pos = bb.position();
        int limit = bb.limit();

        MemoryScope bufferScope = new ConfinedScope(bb, Thread.currentThread(), null);
        return new MemorySegmentImpl(bbAddress + pos, base, limit - pos, 0, bufferScope);
    }

    // create and map a file into a fresh segment
    public static MemorySegmentImpl makeMappedSegment(Path path, long bytesSize, FileChannel.MapMode mapMode) throws IOException {
        if (bytesSize <= 0) throw new IllegalArgumentException("Requested bytes size must be > 0.");
        try (FileChannelImpl channelImpl = (FileChannelImpl)FileChannel.open(path, openOptions(mapMode))) {
            UnmapperProxy unmapperProxy = channelImpl.mapInternal(mapMode, 0L, bytesSize);
            MemoryScope scope = new ConfinedScope(null, Thread.currentThread(), () -> unmapperProxy.unmap());
            return new MemorySegmentImpl(unmapperProxy.address(), null, bytesSize, 0, scope);
        }
    }

    private static OpenOption[] openOptions(FileChannel.MapMode mapMode) {
        if (mapMode == FileChannel.MapMode.READ_ONLY) {
            return new OpenOption[] { StandardOpenOption.READ };
        } else if (mapMode == FileChannel.MapMode.READ_WRITE || mapMode == FileChannel.MapMode.PRIVATE) {
            return new OpenOption[] { StandardOpenOption.READ, StandardOpenOption.WRITE };
        } else {
            throw new UnsupportedOperationException("Unsupported map mode: " + mapMode);
        }
    }
}
