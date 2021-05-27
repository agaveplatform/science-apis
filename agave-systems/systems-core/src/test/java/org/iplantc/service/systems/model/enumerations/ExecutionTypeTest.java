package org.iplantc.service.systems.model.enumerations;

import org.apache.commons.collections.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.iplantc.service.systems.model.enumerations.SchedulerType.*;

/**
 * Tests the compatibility methods of {@link ExecutionType}. These are used to discover which {@link ExecutionType}
 * and {@link SchedulerType} are compatibile with a given {@link ExecutionType}.
 */
@Test(groups={"unit"})
public class ExecutionTypeTest {

    @DataProvider
    public static Object[][] testGetCompatibleExecutionTypesProvider() {
        return new Object[][]{
                {ExecutionType.CLI, List.of(ExecutionType.CLI)},
                {ExecutionType.ATMOSPHERE, List.of(ExecutionType.CLI)},
                {ExecutionType.CONDOR, List.of(ExecutionType.CONDOR)},
                {ExecutionType.HPC, List.of(ExecutionType.HPC, ExecutionType.CLI)},
        };
    }

    @Test(dataProvider = "testGetCompatibleExecutionTypesProvider")
    public void testGetCompatibleExecutionTypes(ExecutionType testExecutionType, List<ExecutionType> expectedExecutionTypes) {
        Assert.assertEquals(testExecutionType.getCompatibleExecutionTypes().size(), expectedExecutionTypes.size(),
                "Scheduler types returned should have the same number of entries, but do not.");

        Collection diff = CollectionUtils.disjunction(expectedExecutionTypes, testExecutionType.getCompatibleExecutionTypes());
        Assert.assertTrue(diff.isEmpty(),
                "Return values differ from those expected: " + diff);
    }

    @DataProvider
    public static Object[][] testGetCompatibleSchedulerTypesProvider() {
        return new Object[][]{
                {ExecutionType.CLI, List.of(FORK)},
                {ExecutionType.ATMOSPHERE, List.of()},
                {ExecutionType.CONDOR, List.of(CONDOR, CUSTOM_CONDOR)},
                {ExecutionType.HPC, Arrays.stream(SchedulerType.values()).filter(t -> ! List.of(FORK, CONDOR, CUSTOM_CONDOR, UNKNOWN).contains(t)).collect(Collectors.toList())},
        };
    }

    @Test(dataProvider = "testGetCompatibleSchedulerTypesProvider")
    public void testGetCompatibleSchedulerTypes(ExecutionType testExecutionType, List<SchedulerType> expectedSchedulerTypes) {
        Assert.assertEquals(testExecutionType.getCompatibleSchedulerTypes().size(), expectedSchedulerTypes.size(),
                "Scheduler types returned should have the same number of entries, but do not.");

        Collection diff = CollectionUtils.disjunction(expectedSchedulerTypes, testExecutionType.getCompatibleSchedulerTypes());
        Assert.assertTrue(diff.isEmpty(),
                "Return values differ from those expected: " + diff);
    }
}