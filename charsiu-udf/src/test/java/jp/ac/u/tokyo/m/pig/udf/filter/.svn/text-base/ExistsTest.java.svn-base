package jp.ac.u.tokyo.m.pig.udf.filter;

import jp.ac.u.tokyo.m.pig.udf.filter.Exists;
import jp.ac.u.tokyo.m.test.TestUtil;
import junit.framework.Assert;

import org.junit.Test;

public class ExistsTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static final String GROUP_FILTER_FORMAT = "4legs(dog, cat, caw), etc(fish, bug)";

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testSimpleExists() throws Throwable {
		Exists tExists = new Exists(GROUP_FILTER_FORMAT);

		Assert.assertEquals(new Boolean(true), tExists.exec(TestUtil.createTuple("dog")));
		Assert.assertEquals(new Boolean(true), tExists.exec(TestUtil.createTuple("cat")));
		Assert.assertEquals(new Boolean(true), tExists.exec(TestUtil.createTuple("caw")));
		Assert.assertEquals(new Boolean(true), tExists.exec(TestUtil.createTuple("fish")));
		Assert.assertEquals(new Boolean(true), tExists.exec(TestUtil.createTuple("bug")));

		Assert.assertEquals(new Boolean(true), tExists.exec(TestUtil.createTuple("catcher")));
		Assert.assertEquals(new Boolean(true), tExists.exec(TestUtil.createTuple("fisher")));

		Assert.assertEquals(new Boolean(false), tExists.exec(TestUtil.createTuple("horse")));
	}

	@Test
	public void testExistsWithaExclusionWords() throws Throwable {
		Exists tExists = new Exists(GROUP_FILTER_FORMAT, "catcher, fisher");

		Assert.assertEquals(new Boolean(true), tExists.exec(TestUtil.createTuple("dog")));
		Assert.assertEquals(new Boolean(true), tExists.exec(TestUtil.createTuple("cat")));
		Assert.assertEquals(new Boolean(true), tExists.exec(TestUtil.createTuple("caw")));
		Assert.assertEquals(new Boolean(true), tExists.exec(TestUtil.createTuple("fish")));
		Assert.assertEquals(new Boolean(true), tExists.exec(TestUtil.createTuple("bug")));

		Assert.assertEquals(new Boolean(false), tExists.exec(TestUtil.createTuple("catcher")));
		Assert.assertEquals(new Boolean(false), tExists.exec(TestUtil.createTuple("fisher")));

		Assert.assertEquals(new Boolean(false), tExists.exec(TestUtil.createTuple("horse")));
	}

	// -----------------------------------------------------------------------------------------------------------------

}
