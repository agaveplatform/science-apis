package org.iplantc.service.io.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

/*
 * This test verifies that the hashes for the path generated using sql in databse match with the one generated from java
 * Also the test compares the paths which are hashed to same value are identical. Ideally different path values can have the same hash value.
 * But since we have 8 bytes worth of longs which hash around 2 million paths, the likelihood of collision is one in a billion.
 *
 * Skipping for general runs as this is a one-time validation prior to migration and does not need to be included
 * by default on unit tests.
 */

@Test(groups = { "unit", "notReady" })
public class PathHashTest {

	@Test
	public void verifyHash() throws NoSuchAlgorithmException {

		ClassLoader classLoader = getClass().getClassLoader();

		String csvFile = "logicalfiles/pathhash.txt";
		String line = "";
		String cvsSplitBy = "\\t";

		HashMap<Long, String> collisions = new HashMap<>(2000000);

		try (BufferedReader br = new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream(csvFile)))) {

			while ((line = br.readLine()) != null) {

				// use tab as separator
				String[] logicalfile = line.split(cvsSplitBy);

				long hashFromJava = ServiceUtils.getMD5LongHash(logicalfile[0]);
				long hashFromsql = Long.valueOf(logicalfile[1].trim());

				Assert.assertEquals(hashFromJava, hashFromsql, "Hash values do not match for path:" + logicalfile[0]
						+ " hashFromJava:" + hashFromJava + " hashFromsql: " + hashFromsql);
				
				if(collisions.containsKey(hashFromsql)) {
					String path = collisions.get(hashFromsql);
					Assert.assertEquals(collisions.get(hashFromsql), logicalfile[0], "There is a collision for key: " + hashFromsql + " Path1:" + logicalfile[0] + " Path2: " + path);
				}
				
				collisions.put(hashFromsql, logicalfile[0]);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
