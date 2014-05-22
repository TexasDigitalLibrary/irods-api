/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umiacs.irods.operation;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umiacs.irods.api.BitstreamCallback;
import edu.umiacs.irods.api.IRodsConnection;
import edu.umiacs.irods.api.IRodsRequestException;
import edu.umiacs.irods.api.RodsUtil;
import edu.umiacs.irods.api.pi.ApiNumberEnum;
import edu.umiacs.irods.api.pi.DataObjInp_PI;
import edu.umiacs.irods.api.pi.ErrorEnum;
import edu.umiacs.irods.api.pi.IntPi;
import edu.umiacs.irods.api.pi.KeyValPair_PI;
import edu.umiacs.irods.api.pi.OprTypeEnum;
import edu.umiacs.irods.api.pi.PortList_PI;
import edu.umiacs.irods.api.pi.PortalOprOut_PI;
import edu.umiacs.irods.api.pi.RodsObjStat_PI;

/**
 * 
 * @author toaster
 */
public class IrodsProxyInputStream extends InputStream {

    private static final Logger LOG = LoggerFactory.getLogger(IrodsProxyInputStream.class);
    private IRodsConnection connection;
    private int fd;
    private File tempFile = null;
    private static FileOutputStream out;
    private static FileChannel ch;
    private InputStream fileStream = null;
    private Socket proxyStream = null;

    public IrodsProxyInputStream(String fileName, IRodsConnection connection) throws IOException {
        this.connection = connection;
        open(fileName);
    }

    private void open(String fileName) throws IOException {
        IrodsApiRequest apiReq;
        DataObjInp_PI body;
        int status;
        RodsObjStat_PI stat;

        if (connection != null) {
            stat = stat(fileName);
            if (stat.getObjSize() <= 0)
                return;

            // the body of api request
            int createMode = 0;
            int openFlags = 0;
            int offset = 0;
            long dataSize = stat.getObjSize();
            // this can be -1(no threads), 0(auto threads) or #(n number of threads) [NOTE: -1 and 1 don't seem to work with TACC iRODS 3.2]
            int numThreads = 0;
            OprTypeEnum oprType = OprTypeEnum.GET_OPR;
            KeyValPair_PI keyValPair_PI = new KeyValPair_PI((Map<String, String>) null);
            body = new DataObjInp_PI(fileName, createMode, openFlags, offset, dataSize, numThreads, oprType, keyValPair_PI);

            // the api request
            ApiNumberEnum apiNumber = ApiNumberEnum.DATA_OBJ_GET_AN;
            BitstreamCallback bs = null;
            apiReq = new IrodsApiRequest(apiNumber, body, bs);

            status = apiReq.sendRequest(connection);
            InputStream dataStream = apiReq.getResultInputStream();

            // LOG.info("DATA_OBJ_GET_AN status: " + status + " for stat(" + stat + ") dataStream[" + dataStream + "]");
            // error Opening
            if (status < 0) {
                connection = null;
                throw new IRodsRequestException("Error opening file, error: " + ErrorEnum.valueOf(status));

            } else if (dataStream == null) {
                // external data stream
                PortalOprOut_PI outPI = apiReq.getResultPI(PortalOprOut_PI.class);
                fd = outPI.getL1descInx();

                // private DataInputStream is;
                PortList_PI portlist = outPI.getPortListPI();
                int cookie = portlist.getCookie();
                int port = portlist.getPortNum();
                String host = portlist.getHostAddr();
                int threads = outPI.getNumThreads();

                // LOG.warn("Received Proxy port for data: " + outPI);
                if (port == 0 || host.length() == 0) {
                    throw new IOException("Received invalid Proxy port data!");
                }

                // calculate real chunk sizes
                long realChunkSize = (dataSize / threads);
                long remainderChunkSize = (dataSize % threads);
                ArrayList<socketRunner> sockets = new ArrayList<socketRunner>();
                // create all the socketRunners
                for (int i = 0; i < threads; i++) {
                    sockets.add(new socketRunner(host, port, cookie, dataSize, realChunkSize, remainderChunkSize, this));
                }
                try {
                    // put all the chunks together
                    Path path = Paths.get(fileName);
                    tempFile = File.createTempFile("iRods", path.getFileName().toString());
                    LOG.info("iRods saving temp file to: " + tempFile.getPath());
                    // File outFile = new File("out.flv");
                    out = new FileOutputStream(tempFile);
                    ch = out.getChannel();
                    // run all the socketRunners
                    for (socketRunner socket : sockets) {
                        Thread temp = new Thread(socket);
                        temp.start();
                    }
                    // wait for all of them to be done
                    for (socketRunner socket : sockets) {
                        synchronized (socket) {
                            while (!socket.isDone())
                                ;
                        }
                    }
                    LOG.warn("all Proxy chunks are done!");
                } finally {
                    out.flush();
                    ch.close();
                    out.close();
                }
                // LOG.warn("wrote the file?");
                fileStream = new FileInputStream(tempFile);
            } else {
                LOG.info("File embedded in control channel");
                fileStream = dataStream;
            }
        } // end if(connection != null)
    }// end open()

    protected synchronized void writeToFile(long offset, ByteBuffer src) throws IOException {
        // LOG.info("Writing to file @ offset: " + offset + " dataSize: " + src.capacity());
        if (ch != null && out != null) {
            ch.position(offset);
            ch.write(src);
            out.flush();
        }
    }

    private static class socketRunner implements Runnable {
        private static final Logger LOG = LoggerFactory.getLogger(socketRunner.class);

        // input variables
        private Socket proxyStream;
        private DataInputStream is;
        private OutputStream os;
        private int cookie;
        private int port;
        private String host;
        private long dataSize;
        private long realChunkSize;
        private long remainderChunkSize;
        private IrodsProxyInputStream irodsProxyInputStream;

        // response header
        private int oprType;
        private int flags;
        private long offset;
        private long totalBytes;

        private long written = 0;
        private int bufferIdealSize = 1024;

        // flag to know this thread is done
        private boolean done = false;

        public socketRunner(String host, int port, int cookie, long dataSize, long realChunkSize, long remainderChunkSize, IrodsProxyInputStream irodsProxyInputStream) {
            this.host = host;
            this.port = port;
            this.cookie = cookie;
            this.realChunkSize = realChunkSize;
            this.remainderChunkSize = remainderChunkSize;
            this.dataSize = dataSize;
            this.irodsProxyInputStream = irodsProxyInputStream;
        }

        public void run() {
            try {
                proxyStream = new Socket(host, port);
                os = proxyStream.getOutputStream();
                is = new DataInputStream(proxyStream.getInputStream());
                os.write(RodsUtil.renderInt(cookie));
                oprType = RodsUtil.parseInt(RodsUtil.readBytes(4, is));
                flags = RodsUtil.parseInt(RodsUtil.readBytes(4, is));
                offset = RodsUtil.parseLong(RodsUtil.readBytes(8, is));
                totalBytes = RodsUtil.parseLong(RodsUtil.readBytes(8, is));
                // if we have sub-chunks
                if (totalBytes != realChunkSize) {
                    // if we're the last chunk, add the remainder to our realChunkSize
                    if (((realChunkSize + remainderChunkSize) + offset) == dataSize) {
                        realChunkSize += remainderChunkSize;
                    }
                }
                LOG.info("Opened Proxy port for host: " + host + ":" + port + " totalBytes: " + totalBytes + " offset: " + offset + " oprType: " + OprTypeEnum.values()[oprType] + " flags: " + flags);
                while (written < realChunkSize) {
                    readBytes((int) totalBytes);
                }
                LOG.info("Done loading Proxy chunk!");
            } catch (UnknownHostException e) {
                LOG.error(e.getLocalizedMessage());
            } catch (IOException e) {
                LOG.error(e.getLocalizedMessage());
            } finally {
                try {
                    proxyStream.close();
                } catch (IOException e) {
                    LOG.error(e.getLocalizedMessage());
                }
            }
            done = true;
        } // end of run()

        public boolean readBytes(int numToRead) throws IOException {
            long read = 0;
            // download in buffers of bufferSize
            while (read < numToRead && written < realChunkSize) {
                // calculate buffer size so as to not read too many bytes
                // buffer size is bufferSize unless bufferSize makes us go over on numToRead... then make buffersize = numToRead - read
                int bufferSize = (int) ((read + bufferIdealSize) <= numToRead ? bufferIdealSize : (numToRead - read));
                // if we're going to go over on writing... make sure we don't read too many bytes
                if (bufferSize + written > realChunkSize) {
                    bufferSize = (int) (realChunkSize - written);
                }
                byte[] bytes = new byte[bufferSize];
                int readNow = is.read(bytes);
                if (readNow > 0) {
                    // move the byte array to a real-sized one so that padded 0's don't get written to file if bytes[] was not filled with read()
                    byte[] actualBytes = new byte[readNow];
                    for (int i = 0; i < actualBytes.length; i++) {
                        actualBytes[i] = bytes[i];
                    }
                    irodsProxyInputStream.writeToFile((offset + written), ByteBuffer.wrap(actualBytes));
                    read += readNow;
                    written += readNow;
                }
            }
            // throw away 24 bytes between sub chunks (undocumented sub-chunk header?)
            byte[] headerBetweenSubChunks = new byte[24];
            is.read(headerBetweenSubChunks);
            return true;
        }

        public synchronized boolean isDone() {
            return done;
        }
    } // end of static class

    /*
     * private void openOld(String fileName) throws IOException { IrodsApiRequest apiReq; DataObjInp_PI body; int status; RodsObjStat_PI stat;
     * 
     * if (connection != null) { stat = stat(fileName); if (stat.getObjSize() <= 0) return;
     * 
     * // -1 is no threading, 0 is let server decide, 1+ is manual setting (0 is the only one that works?) body = new DataObjInp_PI(fileName, 0, 0, 0, stat.getObjSize(), 0, OprTypeEnum.GET_OPR, new KeyValPair_PI((Map) null));
     * 
     * apiReq = new IrodsApiRequest(ApiNumberEnum.DATA_OBJ_GET_AN, body, null);
     * 
     * status = apiReq.sendRequest(connection); dataStream = apiReq.getResultInputStream(); LOG.warn("DATA_OBJ_GET_AN status: " + status + " for stat(" + stat + ") dataStream[" + dataStream + "]"); // error Opening if (status < 0) { connection = null; throw new IRodsRequestException("Error opening file, error: " + ErrorEnum.valueOf(status));
     * 
     * } else if (dataStream == null) { // external data stream PortalOprOut_PI outPI = apiReq.getResultPI(PortalOprOut_PI.class); fd = outPI.getL1descInx();
     * 
     * // private DataInputStream is; PortList_PI portlist = outPI.getPortListPI(); int cookie = portlist.getCookie(); int port = portlist.getPortNum(); String host = portlist.getHostAddr();
     * 
     * LOG.warn("Received Proxy port for data: " + outPI); if (port == 0 || host.length() == 0) { throw new IOException("Received invalid Proxy port data!"); } proxyStream = new Socket(host, port); OutputStream os = proxyStream.getOutputStream(); DataInputStream is = new DataInputStream(proxyStream.getInputStream()); this.dataStream = is;
     * 
     * os.write(RodsUtil.renderInt(cookie)); RodsUtil.readBytes(8, is); long offset = RodsUtil.parseLong(RodsUtil.readBytes(8, is)); if (offset != 0) { throw new IOException("Stream offset is not 0, offset: " + offset); }
     * 
     * long totalBytes = RodsUtil.parseLong(RodsUtil.readBytes(8, is)); LOG.warn("Opened Proxy port for data " + host + ":" + port); } else { LOG.warn("File embedded in control channel"); } } }
     */

    private RodsObjStat_PI stat(String path) throws IRodsRequestException {
        try {

            DataObjInp_PI inPi = new DataObjInp_PI(path, 0, 0, 0, 0, 0, OprTypeEnum.NO_OPR_TYPE, new KeyValPair_PI((Map<String, String>) null));

            IrodsApiRequest apiReq = new IrodsApiRequest(ApiNumberEnum.OBJ_STAT_AN, inPi, null);

            int status = apiReq.sendRequest(connection);

            if (status == ErrorEnum.USER_FILE_DOES_NOT_EXIST.getInt()) {
                return null;
            }
            if (status < 0) {
                throw new IRodsRequestException(ErrorEnum.valueOf(status));
            }

            return apiReq.getResultPI(RodsObjStat_PI.class);
        } catch (IOException ex) {
            if (ex instanceof IRodsRequestException) {
                throw (IRodsRequestException) ex;
            }
            throw new IRodsRequestException("Communication error sending request", ex);
        }

    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (fileStream == null)
            return -1;
        return fileStream.read(b, off, len);
    }

    @Override
    public int read() throws IOException {
        throw new UnsupportedOperationException("Inputstream does not allow single char reads");
    }

    @Override
    public void close() throws IOException {

        if (fd > 0) {
            IntPi inPi = new IntPi(fd);
            IrodsApiRequest apiReq = new IrodsApiRequest(ApiNumberEnum.OPR_COMPLETE_AN, inPi, null);

            int status = apiReq.sendRequest(connection);

            if (status < 0) {
                throw new IRodsRequestException(ErrorEnum.valueOf(status));
            }
            if (proxyStream != null) {
                proxyStream.close();
            }
        }
        if (fileStream != null) {
            fileStream.close();
        }
        if (tempFile != null) {
            tempFile.delete();
        }
    }
}
