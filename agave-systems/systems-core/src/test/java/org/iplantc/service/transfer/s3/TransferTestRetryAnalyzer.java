package org.iplantc.service.transfer.s3;

import org.apache.log4j.Logger;
import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;
import org.testng.util.RetryAnalyzerCount;

import java.util.concurrent.atomic.AtomicInteger;


public class TransferTestRetryAnalyzer extends RetryAnalyzerCount {
    Logger log = Logger.getLogger(TransferTestRetryAnalyzer.class);

    /**
     * Default constructor
     */
    public TransferTestRetryAnalyzer() {
        setCount(4);
    }

    /**
     * Returns true all the time. This force the default behavior of retrying
     * until the counter hits zero.
     *
     * @param result the result of the test. Doesn't matter why here. If it fails, so be it.
     * @return true all the time
     */
    @Override
    public boolean retryMethod(ITestResult result) {
        String message = String.format("Retrying %s#%s after failure %d\n",
                result.getTestClass().getName(),
                result.getMethod().getMethodName(),
                getCount());
        log.debug(message);
        System.out.println(message);
        return true;
    }

}
