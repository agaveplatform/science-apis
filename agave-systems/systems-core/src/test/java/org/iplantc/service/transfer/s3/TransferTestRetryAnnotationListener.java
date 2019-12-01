package org.iplantc.service.transfer.s3;

import org.testng.IAnnotationTransformer;
import org.testng.annotations.ITestAnnotation;
import org.testng.util.RetryAnalyzerCount;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * Adds failed test retry semantics to all tests deriving from RemoteDataClientTestUtils
 */
public class TransferTestRetryAnnotationListener implements IAnnotationTransformer {

    @Override
    public void transform(ITestAnnotation annotation, Class testClass, Constructor testConstructor, Method testMethod) {
        annotation.setRetryAnalyzer(TransferTestRetryAnalyzer.class);
    }
}
