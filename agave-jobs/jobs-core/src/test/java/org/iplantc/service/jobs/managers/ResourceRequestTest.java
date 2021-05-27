package org.iplantc.service.jobs.managers;

import org.iplantc.service.apps.model.Software;
import org.iplantc.service.jobs.exceptions.JobProcessingException;
import org.iplantc.service.systems.model.BatchQueue;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

@Test(groups={"unit"})
public class ResourceRequestTest {

    //############################################################################################################
    //        Max nodes Tests
    //############################################################################################################

    @Test
    public void testSetMaxNodes() {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = mock(ResourceRequest.class);
        when(software.getDefaultNodes()).thenReturn(null);
        when(resourceRequest.getSoftware()).thenReturn(software);
        when(resourceRequest.getMaxNodes()).thenCallRealMethod();
        try {
            doCallRealMethod().when(resourceRequest).setMaxNodes(anyString());
            resourceRequest.setMaxNodes("2");

            assertEquals(resourceRequest.getMaxNodes().longValue(), 2L,
                    "Setting max nodes to 1 should succeed and result in a value of 1");
        } catch(JobProcessingException e) {
            fail("Positive integer value should not throw job processing exception", e);
        }
    }

    @Test
    public void testSetMaxNodesSelectsSoftwareDefaultNodes() {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = mock(ResourceRequest.class);
        when(software.getDefaultNodes()).thenReturn(5L);
        when(resourceRequest.getSoftware()).thenReturn(software);
        when(resourceRequest.getMaxNodes()).thenCallRealMethod();
        try {
            doCallRealMethod().when(resourceRequest).setMaxNodes(anyString());
            resourceRequest.setMaxNodes((String)null);

            assertEquals(resourceRequest.getMaxNodes().longValue(), 5L,
                    "Setting max nodes to null should use value from software default nodes");
        } catch(JobProcessingException e) {
            fail("Positive integer value should not throw job processing exception", e);
        }
    }

    @DataProvider
    public Object[][] testSetEmptyValueProvider() {
        return new Object[][] {
                {""},
                {"  "},
                {null}
        };
    }

    @Test(dataProvider = "testSetEmptyValueProvider")
    public void testSetMaxNodesDefaultsToOne(String testValue) {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = mock(ResourceRequest.class);
        when(software.getDefaultNodes()).thenReturn(null);
        when(resourceRequest.getSoftware()).thenReturn(software);
        when(resourceRequest.getMaxNodes()).thenCallRealMethod();
        try {
            doCallRealMethod().when(resourceRequest).setMaxNodes(anyString());
            resourceRequest.setMaxNodes(testValue);

            assertEquals(resourceRequest.getMaxNodes().longValue(), 1L,
                    "Setting max nodes to blank value should default to 1 when software default nodes is null");
        } catch(JobProcessingException e) {
            fail("Positive integer value should not throw job processing exception", e);
        }
    }

    @DataProvider
    public Object[][] testSetInvalidNumericValueThrowsExceptionProvider() {
        return new Object[][] {
                {"-1", "Setting to negative value should throw exception"},
                {"four", "Setting to non numeric value should throw exception"},
                {"3.14159", "Setting to non integer value should throw exception"}
        };
    }

    @Test(dataProvider = "testSetInvalidNumericValueThrowsExceptionProvider", expectedExceptions = JobProcessingException.class)
    public void testSetMaxNodesThrowsExceptionForInvalidValue(String tesValue, String message) throws JobProcessingException {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = mock(ResourceRequest.class);
        when(software.getDefaultNodes()).thenReturn(null);
        when(resourceRequest.getSoftware()).thenReturn(software);
        when(resourceRequest.getMaxNodes()).thenCallRealMethod();
        try {
            doCallRealMethod().when(resourceRequest).setMaxNodes(anyString());
        } catch(JobProcessingException ignored) {}

        resourceRequest.setMaxNodes(tesValue);
        fail(message);

    }


    //############################################################################################################
    //        Processor per node Tests
    //############################################################################################################

    @Test
    public void testSetMaxProcessorsPerNode() {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = mock(ResourceRequest.class);
        when(software.getDefaultProcessorsPerNode()).thenReturn(null);
        when(resourceRequest.getSoftware()).thenReturn(software);
        when(resourceRequest.getMaxProcessorsPerNode()).thenCallRealMethod();
        try {
            doCallRealMethod().when(resourceRequest).setMaxProcessorsPerNode(anyString());
            resourceRequest.setMaxProcessorsPerNode("2");

            assertEquals(resourceRequest.getMaxProcessorsPerNode().longValue(), 2L,
                    "Setting max processor per node to 1 should succeed and result in a value of 1");
        } catch(JobProcessingException e) {
            fail("Positive integer value should not throw job processing exception", e);
        }
    }

    @Test
    public void testSetMaxProcessorsPerNodeSelectsSoftwareDefaultNodes() {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = mock(ResourceRequest.class);
        when(software.getDefaultProcessorsPerNode()).thenReturn(5L);
        when(resourceRequest.getSoftware()).thenReturn(software);
        when(resourceRequest.getMaxProcessorsPerNode()).thenCallRealMethod();
        try {
            doCallRealMethod().when(resourceRequest).setMaxProcessorsPerNode(anyString());
            resourceRequest.setMaxProcessorsPerNode((String)null);

            assertEquals(resourceRequest.getMaxProcessorsPerNode().longValue(), 5L,
                    "Setting max processor per node to null should use value from software default nodes");
        } catch(JobProcessingException e) {
            fail("Positive integer value should not throw job processing exception", e);
        }
    }

    @Test(dataProvider = "testSetEmptyValueProvider")
    public void testSetMaxProcessorsPerNodeDefaultsToOne(String testValue) {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = mock(ResourceRequest.class);
        when(software.getDefaultProcessorsPerNode()).thenReturn(null);
        when(resourceRequest.getSoftware()).thenReturn(software);
        when(resourceRequest.getMaxProcessorsPerNode()).thenCallRealMethod();
        try {
            doCallRealMethod().when(resourceRequest).setMaxProcessorsPerNode(anyString());
            resourceRequest.setMaxProcessorsPerNode(testValue);

            assertEquals(resourceRequest.getMaxProcessorsPerNode().longValue(), 1L,
                    "Setting max processor per node to blank should default to 1 when software default nodes is null");
        } catch(JobProcessingException e) {
            fail("Positive integer value should not throw job processing exception", e);
        }
    }

    @Test(dataProvider = "testSetInvalidNumericValueThrowsExceptionProvider", expectedExceptions = JobProcessingException.class)
    public void testSetMaxProcessorsPerNodeThrowsExceptionForInvalidValue(String tesValue, String message) throws JobProcessingException {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = mock(ResourceRequest.class);
        when(software.getDefaultProcessorsPerNode()).thenReturn(null);
        when(resourceRequest.getSoftware()).thenReturn(software);
        when(resourceRequest.getMaxProcessorsPerNode()).thenCallRealMethod();
        try {
            doCallRealMethod().when(resourceRequest).setMaxProcessorsPerNode(anyString());
        } catch(JobProcessingException ignored) {}

        resourceRequest.setMaxProcessorsPerNode(tesValue);
        fail(message);
    }

    //############################################################################################################
    //        Max Memory Tests
    //############################################################################################################

    @Test
    public void testSetMaxMemoryPerNodeString() {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = mock(ResourceRequest.class);
        when(software.getDefaultMemoryPerNode()).thenReturn(null);
        when(resourceRequest.getSoftware()).thenReturn(software);
        when(resourceRequest.getMaxMemoryPerNode()).thenCallRealMethod();
        try {
            doCallRealMethod().when(resourceRequest).setMaxMemoryPerNode(anyString());
            resourceRequest.setMaxMemoryPerNode("2GB");

            assertEquals(resourceRequest.getMaxMemoryPerNode(), 2D,
                    "Setting max memory per node to 2 should succeed and result in a value of 2");
        } catch(JobProcessingException e) {
            fail("Valid max memory per node value should not throw job processing exception", e);
        }
    }

    @Test
    public void testSetMaxMemoryPerNodeSelectsSoftwareDefaultMemory() {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = mock(ResourceRequest.class);
        when(software.getDefaultMemoryPerNode()).thenReturn(5D);
        when(resourceRequest.getSoftware()).thenReturn(software);
        when(resourceRequest.getMaxMemoryPerNode()).thenCallRealMethod();
        try {
            doCallRealMethod().when(resourceRequest).setMaxMemoryPerNode(anyString());
            resourceRequest.setMaxMemoryPerNode((String)null);

            assertEquals(resourceRequest.getMaxMemoryPerNode(), 5D,
                    "Setting max memory per node to null should default to software default memory value");
        } catch(JobProcessingException e) {
            fail("Null max memory per node value should not throw job processing exception", e);
        }
    }

    @Test(dataProvider = "testSetEmptyValueProvider")
    public void testSetMaxMemoryPerNodeAllowsBlankValue(String testValue) {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = mock(ResourceRequest.class);
        when(software.getDefaultMemoryPerNode()).thenReturn(null);
        when(resourceRequest.getSoftware()).thenReturn(software);
        when(resourceRequest.getMaxMemoryPerNode()).thenCallRealMethod();
        try {
            doCallRealMethod().when(resourceRequest).setMaxMemoryPerNode(anyString());
            resourceRequest.setMaxMemoryPerNode(testValue);
            assertNull(resourceRequest.getMaxMemoryPerNode(), "Blank memory per node value should result in a null default memory value");
        } catch(JobProcessingException e) {
            fail("Blank max memory per node value should not throw an exception.", e);
        }
    }

    @DataProvider
    public Object[][] testSetInvalidTMemoryValueThrowsExceptionProvider() {
        return new Object[][] {
                {"0", "Integer zero memory value should throw exception"},
                {"0.0", "Decimal zero memory value should throw exception"},
                {"225X", "Invalid magnitude designation should throw exception"},
                {"0.01L", "Invalid magnitude designation should throw exception"},
        };
    }

    @Test(dataProvider = "testSetInvalidTMemoryValueThrowsExceptionProvider", expectedExceptions = JobProcessingException.class)
    public void testSetMaxMemoryPerNodeString(String testValue, String message) throws JobProcessingException {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = mock(ResourceRequest.class);
        when(software.getDefaultMemoryPerNode()).thenReturn(null);
        when(resourceRequest.getSoftware()).thenReturn(software);
        when(resourceRequest.getMaxMemoryPerNode()).thenCallRealMethod();
        try {
            doCallRealMethod().when(resourceRequest).setMaxMemoryPerNode(anyString());
        } catch(JobProcessingException ignored) {}

        resourceRequest.setMaxMemoryPerNode(testValue);
        fail(message);
    }



    //############################################################################################################
    //        Max runtime Tests
    //############################################################################################################

    @Test
    public void testSetMaxRequestedTime() {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = mock(ResourceRequest.class);
        when(software.getDefaultMaxRunTime()).thenReturn("02:22:22");
        when(resourceRequest.getSoftware()).thenReturn(software);
        when(resourceRequest.getMaxRequestedTime()).thenCallRealMethod();
        try {
            doCallRealMethod().when(resourceRequest).setMaxRequestedTime(anyString());
            resourceRequest.setMaxRequestedTime("01:11:11");

            assertEquals(resourceRequest.getMaxRequestedTime(), "01:11:11",
                    "Setting max memory per node to 01:11:11 should succeed and result in a value of 01:11:11");
        } catch(JobProcessingException e) {
            fail("Valid max memory per node value should not throw job processing exception", e);
        }
    }

    @Test
    public void testSetMaxRequestedTimeSelectsSoftwareDefaultMemory() {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = mock(ResourceRequest.class);
        when(software.getDefaultMaxRunTime()).thenReturn("02:22:22");
        when(resourceRequest.getSoftware()).thenReturn(software);
        when(resourceRequest.getMaxRequestedTime()).thenCallRealMethod();
        try {
            doCallRealMethod().when(resourceRequest).setMaxRequestedTime(anyString());
            resourceRequest.setMaxRequestedTime(null);

            assertEquals(resourceRequest.getMaxRequestedTime(), "02:22:22",
                    "Setting max memory per node to null should default to system default memory value");
        } catch(JobProcessingException e) {
            fail("Null max memory per node value should not throw job processing exception", e);
        }
    }

    @Test(dataProvider = "testSetEmptyValueProvider")
    public void testSetMaxRequestedTimeDefaultsToNull(String testValue) {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = mock(ResourceRequest.class);
        when(software.getDefaultMaxRunTime()).thenReturn(null);
        when(resourceRequest.getSoftware()).thenReturn(software);
        when(resourceRequest.getMaxRequestedTime()).thenCallRealMethod();
        try {
            doCallRealMethod().when(resourceRequest).setMaxRequestedTime(anyString());
            resourceRequest.setMaxRequestedTime(testValue);

            assertNull(resourceRequest.getMaxRequestedTime(),
                    "Setting max requested time to blank value should default to null when system default memory is null");
        } catch(JobProcessingException e) {
            fail("Null max requeted time value should not throw job processing exception", e);
        }
    }

    @DataProvider
    public Object[][] testSetInvalidTemporalValueThrowsExceptionProvider() {
        return new Object[][] {
                {"01", "Invalid max time request format should throw exception"},
                {"01:00", "Short max time request format should throw exception"},
                {"1:0:0", "Non zero-padded max time request format should throw exception"},
                {"11111:00:00", "Max time request larger than max allowable queue time format should throw exception"},
                {"2 hours", "Textual time format should throw exception"},
                {"3600", "Numeric time format should throw exception"},
        };
    }

    @Test(dataProvider = "testSetInvalidTemporalValueThrowsExceptionProvider", expectedExceptions = JobProcessingException.class)
    public void testSetMaxRequestedTimeThrowsExceptionOnInvalidValue(String testValue, String message) throws JobProcessingException {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = mock(ResourceRequest.class);
        when(software.getDefaultMaxRunTime()).thenReturn(null);
        when(resourceRequest.getSoftware()).thenReturn(software);
        when(resourceRequest.getMaxRequestedTime()).thenCallRealMethod();
        try {
            doCallRealMethod().when(resourceRequest).setMaxRequestedTime(anyString());
        } catch(JobProcessingException ignored) {}

        resourceRequest.setMaxRequestedTime(testValue);
        fail(message);
    }

    //############################################################################################################
    //        compareTo tests
    //############################################################################################################

    @DataProvider
    public Object[][] testCompareToProvider() {
        return new Object[][] {
                {1L,    2.0D,   1L,     "00:01:00"},
        };
    }

    @Test(dataProvider = "testCompareToProvider")
    public void testCompareEqualsBatchQueue(Long maxNodes, Double maxMemoryPerNode, Long maxProcessorsPerNode, String maxRequestedTime) throws JobProcessingException {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = new ResourceRequest(software);
        resourceRequest.setMaxNodes(maxNodes);
        resourceRequest.setMaxMemoryPerNode(maxMemoryPerNode);
        resourceRequest.setMaxProcessorsPerNode(maxProcessorsPerNode);
        resourceRequest.setMaxRequestedTime(maxRequestedTime);

        BatchQueue batchQueue = new BatchQueue("testQueue");
        batchQueue.setMaxNodes(maxNodes);
        batchQueue.setMaxMemoryPerNode(maxMemoryPerNode);
        batchQueue.setMaxProcessorsPerNode(maxProcessorsPerNode);
        batchQueue.setMaxRequestedTime(maxRequestedTime);

        int result = resourceRequest.compareTo(batchQueue);
        Assert.assertEquals(result, 0, "Resource request with same value as batch queue should return 0");
    }

    @DataProvider
    public Object[][] testCompareLessThanBatchQueueProvider() {
        return new Object[][] {
//                {8L,    8.0D,   8L,     "01:00:00"},
                {1L,    8.0D,   8L,     "01:00:00"},
                {8L,    1.0D,   8L,     "01:00:00"},
                {8L,    8.0D,   1L,     "01:00:00"},
                {8L,    8.0D,   8L,     "00:30:00"},
        };
    }

    @Test(dataProvider = "testCompareLessThanBatchQueueProvider")
    public void testCompareLessThanBatchQueue(Long maxNodes, Double maxMemoryPerNode, Long maxProcessorsPerNode, String maxRequestedTime) throws JobProcessingException {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = new ResourceRequest(software);
        resourceRequest.setMaxNodes(maxNodes);
        resourceRequest.setMaxMemoryPerNode(maxMemoryPerNode);
        resourceRequest.setMaxProcessorsPerNode(maxProcessorsPerNode);
        resourceRequest.setMaxRequestedTime(maxRequestedTime);

        BatchQueue batchQueue = new BatchQueue("testQueue");
        batchQueue.setMaxNodes(8L);
        batchQueue.setMaxMemoryPerNode(8.0D);
        batchQueue.setMaxProcessorsPerNode(8L);
        batchQueue.setMaxRequestedTime("01:00:00");


        int result = resourceRequest.compareTo(batchQueue);
        Assert.assertEquals(result, -1, "Resource request with same value as batch queue should return 0");
    }

    @DataProvider
    public Object[][] testCompareGreaterThanBatchQueueProvider() {
        return new Object[][] {
                {9L,    8.0D,   8L,     "01:00:00"},
                {8L,    9.0D,   8L,     "01:00:00"},
                {8L,    8.0D,   9L,     "01:00:00"},
                {8L,    8.0D,   8L,     "02:00:00"},

                {7L,    9.0D,   8L,     "01:00:00"},
                {7L,    9.0D,   9L,     "01:00:00"},
                {7L,    8.0D,   8L,     "02:00:00"},

                {7L,    9.0D,   8L,     "01:00:00"},
                {7L,    9.0D,   8L,     "01:00:00"},
                {7L,    9.0D,   8L,     "01:00:00"},

        };
    }

    @Test(dataProvider = "testCompareGreaterThanBatchQueueProvider")
    public void testCompareGreaterThanBatchQueue(Long maxNodes, Double maxMemoryPerNode, Long maxProcessorsPerNode, String maxRequestedTime) throws JobProcessingException {
        Software software = mock(Software.class);
        ResourceRequest resourceRequest = new ResourceRequest(software);
        resourceRequest.setMaxNodes(maxNodes);
        resourceRequest.setMaxMemoryPerNode(maxMemoryPerNode);
        resourceRequest.setMaxProcessorsPerNode(maxProcessorsPerNode);
        resourceRequest.setMaxRequestedTime(maxRequestedTime);

        BatchQueue batchQueue = new BatchQueue("testQueue");
        batchQueue.setMaxNodes(8L);
        batchQueue.setMaxMemoryPerNode(8.0D);
        batchQueue.setMaxProcessorsPerNode(8L);
        batchQueue.setMaxRequestedTime("01:00:00");


        int result = resourceRequest.compareTo(batchQueue);
        Assert.assertEquals(result, 1, "Resource request with same value as batch queue should return 0");
    }

}
