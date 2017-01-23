package com.opencsv;

import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
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
    public int position = 0;
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
            bList[bLen++] = b;
            fill(false);
        }
        return this;
    }

    public FileBuffer write(byte[] bytes) throws IOException {
        return write(bytes, 0);
    }

    public FileBuffer write(char c) throws IOException {
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
            if (buffer != null && buffer.isDirect()) ((DirectBuffer) buffer).cleaner().clean();
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

