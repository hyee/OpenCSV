package com.opencsv;

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

/**
 * Created by 1506428 on 7/18/2015.
 */
public class FileBuffer {
    public static int BUFFER_RESERVED_SIZE = 2097152;
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


    public FileBuffer(int bufferSize, String path, String default_ext,String charset) throws IOException {
        try {
            file = new File(path);
            fileName = file.getName();
            int index = fileName.lastIndexOf(".");
            Boolean isZip=false;
            if (index > -1) {
                extName = fileName.substring(index + 1);
                fileName = fileName.substring(0, index);
                if (extName.equalsIgnoreCase("zip") || extName.equalsIgnoreCase("gz")) {
                    zipType = extName.toLowerCase();
                    isZip=true;
                    index = fileName.lastIndexOf(".");
                    if(default_ext!=null&&index>-1&&fileName.substring(index + 1).equalsIgnoreCase(default_ext)) {
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
            buffer = ByteBuffer.allocateDirect(bufferSize + BUFFER_RESERVED_SIZE);
            buffer.order(ByteOrder.nativeOrder());
            if (isZip) {
                BufferedOutputStream buff = new BufferedOutputStream(Channels.newOutputStream(channel));
                if (zipType.equals("zip")) {
                    ZipOutputStream zip = new ZipOutputStream(buff);
                    zip.putNextEntry(new ZipEntry(fileName + (extName.equals(null)  ? "" : "." + default_ext)));
                    zipStream = zip;
                } else zipStream = new GZIPOutputStream(buff, true);
            }
            if(charset!=null) this.charset=Charset.forName(charset);

        } catch (IOException e) {
            close();
            throw e;
        }
    }

    public FileBuffer(int bufferSize, String path, String default_ext) throws IOException {
        this(bufferSize,path,default_ext,null);
    }

    public FileBuffer(int bufferSize, String path) throws IOException {
        this(bufferSize,path,null);
    }

    public FileBuffer write(byte[] bytes) {
        currentBytes += bytes.length;
        buffer.put(bytes);
        return this;
    }

    public FileBuffer write(char c) {
        ++currentBytes;
        buffer.put((byte)c);
        return this;

    }

    public FileBuffer write(String str) {
        return write(str.getBytes());
    }

    public boolean flush(boolean force) throws IOException {
        try {
            if (currentBytes > 0 && (force || currentBytes >= bufferSize - 1024)) {
                currentBytes = buffer.position();
                position += currentBytes;
                buffer.flip();
                if (zipStream == null) channel.write(buffer);
                else {
                    byte[] bytes = new byte[currentBytes];
                    buffer.get(bytes, 0, bytes.length);
                    zipStream.write(bytes);
                }
                currentBytes = 0;
                buffer.clear();
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
            if (zipStream != null) {
                zipStream.finish();
                zipStream.close();
            }
            if (channel != null && channel.isOpen()) channel.close();
            if (out != null) out.close();
            buffer = null;
            zipStream = null;
            channel = null;
            out = null;
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    class ByteBufferStream extends OutputStream {
        @Override
        public synchronized void write(int b) throws IOException {//outputStream的写方法
            // TODO Auto-generated method stub
            buffer.put((byte) b);
        }

        @Override
        public synchronized void write(byte[] bytes, int off, int len) throws IOException {
            buffer.put(bytes, off, len);
        }

    }
}


