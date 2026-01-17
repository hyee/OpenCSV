package com.opencsv;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class FileBuffer implements Closeable {
    public static int BUFFER_RESERVED_SIZE = 1 << 20;
    public String fileName;
    public String extName;
    public File file;
    public int bufferSize;
    public ByteBuffer buffer;
    public long position = 0;
    protected RandomAccessFile out;
    protected DeflaterOutputStream zipStream;
    protected String zipType = null;
    protected FileChannel channel;
    protected int currentBytes = 0;
    protected Charset charset;
    protected byte[] bList = new byte[BUFFER_RESERVED_SIZE];
    protected int bLen = 0;

    public FileBuffer(int bufferSize, String path, String default_ext, String charset) throws IOException {
        try {
            file = new File(path);
            fileName = file.getName();
            int index = fileName.lastIndexOf(".");
            Boolean isZip = false;
            if (index > -1) {
                extName = fileName.substring(index + 1);
                fileName = fileName.substring(0, index);
                if (extName.equalsIgnoreCase("zip") || extName.equalsIgnoreCase("gz")) {
                    zipType = extName.toLowerCase();
                    isZip = true;
                    index = fileName.lastIndexOf(".");
                    if (default_ext != null && index > -1 && fileName.substring(index + 1).equalsIgnoreCase(default_ext)) {
                        extName = default_ext;
                        fileName = fileName.substring(0, index);
                    }
                }
            } else if (default_ext != null) {
                extName = default_ext;
                file = new File(path + "." + extName);
            } else extName = "";

            out = new RandomAccessFile(file, "rw");
            channel = out.getChannel();
            this.bufferSize = bufferSize;

            if (isZip) {
                BufferedOutputStream buff = new BufferedOutputStream(Channels.newOutputStream(channel));
                if (zipType.equals("zip")) {
                    ZipOutputStream zip = new ZipOutputStream(buff);
                    zip.putNextEntry(new ZipEntry(fileName + (extName.equals(null) ? "" : "." + default_ext)));
                    zipStream = zip;
                } else zipStream = new GZIPOutputStream(buff, true);
            } else {
                zipType = null;
                buffer = ByteBuffer.allocateDirect(bufferSize + BUFFER_RESERVED_SIZE);
                buffer.order(ByteOrder.nativeOrder());
            }
            if (charset != null) this.charset = Charset.forName(charset);
        } catch (IOException e) {
            close();
            throw e;
        }
    }

    public FileBuffer(int bufferSize, String path, String default_ext) throws IOException {
        this(bufferSize, path, default_ext, null);
    }

    public FileBuffer(int bufferSize, String path) throws IOException {
        this(bufferSize, path, null);
    }

    protected void fill(boolean force) throws IOException {
        if ((!force && bLen < BUFFER_RESERVED_SIZE) || bLen == 0 || channel == null || !channel.isOpen()) return;
        try {
            if (zipType != null) zipStream.write(bList, 0, bLen);
            else buffer.put(bList, 0, bLen);
            currentBytes += bLen;
            bLen = 0;
        } catch (IOException e) {
            bLen = 0;
            currentBytes = 0;
            close();
            throw e;
        }
    }

    public FileBuffer write(byte[] bytes, int startPos) throws IOException {
        for (byte b : bytes) {
            if (bList == null) return this;
            bList[bLen++] = b;
            fill(false);
        }
        return this;
    }

    public FileBuffer write(byte[] bytes) throws IOException {
        return write(bytes, 0);
    }

    public FileBuffer write(char c) throws IOException {
        if (bList == null) return this;
        bList[bLen++] = (byte) c;
        fill(false);
        return this;
    }

    public FileBuffer write(String str) throws IOException {
        return write(str.getBytes());
    }

    public boolean flush(boolean force) throws IOException {
        try {
            fill(force);
            if (currentBytes > 0 && (force || currentBytes >= bufferSize - 1024)) {
                position += currentBytes;
                if (zipType == null) {
                    buffer.flip();
                    channel.write(buffer);
                    buffer.clear();
                }
                currentBytes = 0;
                return true;
            }
            return false;
        } catch (IOException e) {
            currentBytes = 0;
            close();
            throw e;
        }
    }

    public boolean flush() throws IOException {
        return flush(false);
    }

    private static boolean PRE_JAVA_9 = System.getProperty("java.specification.version", "9").startsWith("1.");
    private static Method cleanMethod;
    private static Method attachmentMethod;
    private static Object theUnsafe;

    //ref: https://stackoverflow.com/questions/2972986/how-to-unmap-a-file-from-memory-mapped-using-filechannel-in-java/54046774#54046774
    static void getCleanMethodPrivileged() {
        if (PRE_JAVA_9) {
            try {
                cleanMethod = Class.forName("sun.misc.Cleaner").getMethod("clean");
                cleanMethod.setAccessible(true);
                final Class<?> directByteBufferClass =
                        Class.forName("sun.nio.ch.DirectBuffer");
                attachmentMethod = directByteBufferClass.getMethod("attachment");
                attachmentMethod.setAccessible(true);
            } catch (final Exception ex) {
            }
        } else {
            try {
                Class<?> unsafeClass;
                try {
                    unsafeClass = Class.forName("sun.misc.Unsafe");
                } catch (Exception e) {
                    // jdk.internal.misc.Unsafe doesn't yet have invokeCleaner(),
                    // but that method should be added if sun.misc.Unsafe is removed.
                    unsafeClass = Class.forName("jdk.internal.misc.Unsafe");
                }
                cleanMethod = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
                cleanMethod.setAccessible(true);
                final Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
                theUnsafeField.setAccessible(true);
                theUnsafe = theUnsafeField.get(null);
            } catch (final Exception ex) {
            }
        }
    }

    static {
        AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                getCleanMethodPrivileged();
                return null;
            }
        });
    }

    private static boolean closeDirectByteBufferPrivileged(
            final ByteBuffer byteBuffer) {
        try {
            if (cleanMethod == null) {
                return false;
            }
            if (PRE_JAVA_9) {
                if (attachmentMethod == null) {
                    return false;
                }
                // Make sure duplicates and slices are not cleaned, since this can result in
                // duplicate attempts to clean the same buffer, which trigger a crash with:
                // "A fatal error has been detected by the Java Runtime Environment:
                // EXCEPTION_ACCESS_VIOLATION"
                // See: https://stackoverflow.com/a/31592947/3950982
                if (attachmentMethod.invoke(byteBuffer) != null) {
                    // Buffer is a duplicate or slice
                    return false;
                }
                // Invoke ((DirectBuffer) byteBuffer).cleaner().clean()
                final Method cleaner = byteBuffer.getClass().getMethod("cleaner");
                cleaner.setAccessible(true);
                cleanMethod.invoke(cleaner.invoke(byteBuffer));
                return true;
            } else {
                if (theUnsafe == null) {
                    return false;
                }
                // In JDK9+, calling the above code gives a reflection warning on stderr,
                // need to call Unsafe.theUnsafe.invokeCleaner(byteBuffer) , which makes
                // the same call, but does not print the reflection warning.
                try {
                    cleanMethod.invoke(theUnsafe, byteBuffer);
                    return true;
                } catch (final IllegalArgumentException e) {
                    // Buffer is a duplicate or slice
                    return false;
                }
            }
        } catch (final Exception e) {
            return false;
        }
    }

    public void close() {
        try {
            flush(true);
            if (channel != null && channel.isOpen()) {
                if (zipStream != null) {
                    zipStream.finish();
                    zipStream.close();
                }
                channel.close();
            }
            if (buffer != null && buffer.isDirect()) {
                AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> closeDirectByteBufferPrivileged(buffer));
            }
            if (out != null) out.close();
            channel = null;
            out = null;
            buffer = null;
            zipStream = null;
            bList = null;
        } catch (IOException e) {
            //e.printStackTrace();
        }
    }
}

