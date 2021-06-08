package org.iplantc.service.io;

import org.apache.log4j.Logger;
import org.iplantc.service.io.queue.UploadRunnable;
import org.iplantc.service.io.queue.listeners.FilesTransferListener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@WebListener
public class FileUploadServletContextListener implements ServletContextListener {
    private static final Logger log = Logger.getLogger(FileUploadServletContextListener.class);
    private static ExecutorService uploadPool;

    /**
     * Schedules a new {@link UploadRunnable} in the static {@link #uploadPool} bound to this class.
     * @return a Future returning a null value when the {@code uploadRunnable} completes.
     */
    public static Future<?> scheduleUploadTask(UploadRunnable uploadRunnable) {
        return uploadPool.submit(uploadRunnable);
    }

    /**
     * Instantiates an {@link ExecutorService} to run {@link Settings#MAX_STAGING_TASKS} instances of
     * {@link FilesTransferListener} as background threads.
     *
     * @param event the initialization event
     */
    @Override
    public void contextInitialized(ServletContextEvent event) {
        // create the executor service. This is just a reserve of Settings.MAX_STAGING_TASKS threads that can
        // handle the last mile file upload tasks initiated by the FileManagementResource.
        uploadPool = Executors.newFixedThreadPool(5);
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
        // stop the pool, sending interrupt exceptions to each runnable.
        log.error("Starting shut down of file upload pool");
        uploadPool.shutdown();
        try {
            // Wait a while for existing tasks to terminate
            if (!uploadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                uploadPool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!uploadPool.awaitTermination(30, TimeUnit.SECONDS))
                    log.error("File upload pool did not terminate");
            }
            log.error("Completed shut down of file upload pool");
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            uploadPool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}
