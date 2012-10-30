/*
 * Copyright 2012 Hiromasa Horiguchi ( The University of Tokyo )
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
