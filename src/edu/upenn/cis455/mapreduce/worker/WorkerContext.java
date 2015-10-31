package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis455.mapreduce.enumeration.WorkerStatus;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;

/**
 * @author brishi
 */
public class WorkerContext {

    String ipAddress;
    int port;
    WorkerStatus status;
    String job;
    int keysRead;
    int keysWritten;

    Date lastUpdated;

    public WorkerContext() {
        port = 8080;
        status = WorkerStatus.IDLE;
        job = "NO NAME YET.";
        keysRead = 0;
        keysWritten = 0;
        lastUpdated = new Date();
    }

    public WorkerContext(HttpServletRequest request) {
        ipAddress = request.getRemoteAddr();
        port = Integer.parseInt(request.getParameter("port"));
        job = request.getParameter("job");
        status = WorkerStatus.valueOf(request.getParameter("status"));
        keysRead = Integer.parseInt(request.getParameter("keysRead"));
        keysWritten = Integer.parseInt(request.getParameter("keysWritten"));
        lastUpdated = new Date();

        // TODO ? set last updated status time / last received UPDATE: done, right? ^
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public WorkerStatus getWorkerStatus() {
        return status;
    }

    public void setWorkerStatus(WorkerStatus workerStatus) {
        this.status = workerStatus;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public int getKeysRead() {
        return keysRead;
    }

    public void setKeysRead(int keysRead) {
        this.keysRead = keysRead;
    }

    public int getKeysWritten() {
        return keysWritten;
    }

    public void setKeysWritten(int keysWritten) {
        this.keysWritten = keysWritten;
    }

    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public WorkerStatus getStatus() {
        return status;
    }

    public void setStatus(WorkerStatus status) {
        this.status = status;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }
}
