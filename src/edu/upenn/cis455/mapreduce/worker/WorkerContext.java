package edu.upenn.cis455.mapreduce.worker;

import com.sun.xml.internal.ws.api.pipe.FiberContextSwitchInterceptor;
import edu.upenn.cis455.mapreduce.enumeration.WorkerStatus;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

/**
 * @author brishi
 */
public class WorkerContext {
    int port;
    WorkerStatus status;
    String job;
    int keysRead;
    int keysWritten;

    public WorkerContext() {
        port = 8080;
        status = WorkerStatus.IDLE;
        job = "NO NAME YET.";
        keysRead = 0;
        keysWritten = 0;
    }

    public WorkerContext(HttpServletRequest request) {
        port = Integer.parseInt(request.getParameter("port"));
        job = request.getParameter("job");
        status = WorkerStatus.valueOf(request.getParameter("status"));
        keysRead = Integer.parseInt(request.getParameter("keysRead"));
        keysWritten = Integer.parseInt(request.getParameter("keysWritten"));

        // TODO ? set last updated status time / last received
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
}
