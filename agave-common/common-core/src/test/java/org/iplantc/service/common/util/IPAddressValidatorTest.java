package org.iplantc.service.common.util;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * IPAddress validator Testing
 * 
 * @author mkyong
 *
 */
@Test(groups={"unit"})
public class IPAddressValidatorTest {

	@DataProvider
	public Object[][] validIPAddressProvider() {
		return new Object[][] { new Object[] { "1.1.1.1" },
				new Object[] { "255.255.255.255" },
				new Object[] { "192.168.1.1" }, new Object[] { "10.10.1.1" },
				new Object[] { "132.254.111.10" },
				new Object[] { "26.10.2.10" }, new Object[] { "127.0.0.1" } };
	}

	@DataProvider
	public Object[][] invalidIPAddressProvider() {
		return new Object[][] { new Object[] { "10.10.10" },
				new Object[] { "10.10" }, new Object[] { "10" },
				new Object[] { "a.a.a.a" }, new Object[] { "10.0.0.a" },
				new Object[] { "10.10.10.256" },
				new Object[] { "222.222.2.999" },
				new Object[] { "999.10.10.20" },
				new Object[] { "2222.22.22.22" },
				new Object[] { "22.2222.22.2" }, new Object[] { "10.10.10" },
				new Object[] { "10.10.10" }, };
	}

	@Test(dataProvider = "validIPAddressProvider")
	public void validate(String ip) {
		boolean valid = new IPAddressValidator().validate(ip);
		Assert.assertTrue(valid);
	}

	@Test(dataProvider = "invalidIPAddressProvider")
	public void validateReturnsFalseOnInvalidIPAddress(String ip) {
		boolean valid = new IPAddressValidator().validate(ip);
		Assert.assertFalse(valid);
	}
}