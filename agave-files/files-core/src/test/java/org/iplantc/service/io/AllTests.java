package org.iplantc.service.io;

import org.testng.TestListenerAdapter;
import org.testng.TestNG;
/**
 * Run all unit tests for TGFM middlewae service.
 *
 * @author Rion Dooley <dooley [at] cct [dot] lsu [dot] edu>
 */
public class AllTests {
	

	public static void main(String args[]) {
		TestListenerAdapter tla = new TestListenerAdapter();
		TestNG testng = new TestNG();
		testng.setTestClasses(new Class[] { AllTests.class });
		testng.addListener(tla);
		testng.run();
	}
}
