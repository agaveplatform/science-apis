package org.iplantc.service.io;

import org.apache.log4j.Logger;
import org.iplantc.service.io.queue.listeners.FilesTransferListener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@WebListener
public class FileEventServletContextListener implements ServletContextListener {
    private static final Logger log = Logger.getLogger(FileEventServletContextListener.class);
    private ExecutorService pool;

    /**
     * Instantiates an {@link ExecutorService} to run {@link Settings#MAX_STAGING_TASKS} instances of
     * {@link FilesTransferListener} as background threads.
     *
     * @param event the initialization event
     */
    @Override
    public void contextInitialized(ServletContextEvent event) {
        // create the executor service
        pool = Executors.newFixedThreadPool(Settings.MAX_STAGING_TASKS);
        // submit a FilesTransferListener per thread in the pool.
        for (int i = 0; i < Settings.MAX_STAGING_TASKS; i++) {
            pool.submit(new FilesTransferListener());
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
        // stop the pool, sending interrupt exceptions to each runnable.
        log.error("Starting shut down of file event listener pool");
        pool.shutdown();
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(30, TimeUnit.SECONDS))
                    log.error("File event listener pool did not terminate");
            }
            log.error("Completed shutdown of file event listener pool");
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}
