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

package jp.ac.u.tokyo.m.pig.udf.eval.span;

import jp.ac.u.tokyo.m.pig.udf.AliasConstants;
import jp.ac.u.tokyo.m.pig.udf.eval.span.GetDaySpan;
import jp.ac.u.tokyo.m.test.TestUtil;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.BeforeClass;
import org.junit.Test;

public class GetDaySpanTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static final String BASE_DATE_WESTERN_CALENDAR = "20120326";
	private static final String BASE_DATE_JAPANESE_CALENDAR = "4240326";

	private static GetDaySpan mGetDaySpan;

	// -----------------------------------------------------------------------------------------------------------------

	@BeforeClass
	public static void init() {
		mGetDaySpan = new GetDaySpan();
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testExecOutput() throws Throwable {
		TestUtil.assertEqualsPigObjects(0, mGetDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, BASE_DATE_WESTERN_CALENDAR)));

		TestUtil.assertEqualsPigObjects(1, mGetDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, "20120327")));
		TestUtil.assertEqualsPigObjects(7, mGetDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, "20120402")));
		TestUtil.assertEqualsPigObjects(365, mGetDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, "20130326")));

		TestUtil.assertEqualsPigObjects(-1, mGetDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, "20120325")));
		TestUtil.assertEqualsPigObjects(-7, mGetDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, "20120319")));
		TestUtil.assertEqualsPigObjects(-366, mGetDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, "20110326")));
	}

	@Test
	public void testExecOutputJapaneseCalendar() throws Throwable {
		TestUtil.assertEqualsPigObjects(0, mGetDaySpan.exec(TestUtil.createTuple(BASE_DATE_JAPANESE_CALENDAR, BASE_DATE_WESTERN_CALENDAR)));
		TestUtil.assertEqualsPigObjects(0, mGetDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, BASE_DATE_JAPANESE_CALENDAR)));
		TestUtil.assertEqualsPigObjects(0, mGetDaySpan.exec(TestUtil.createTuple(BASE_DATE_JAPANESE_CALENDAR, BASE_DATE_JAPANESE_CALENDAR)));

		TestUtil.assertEqualsPigObjects(7, mGetDaySpan.exec(TestUtil.createTuple(BASE_DATE_JAPANESE_CALENDAR, "4240402")));
		TestUtil.assertEqualsPigObjects(-7, mGetDaySpan.exec(TestUtil.createTuple(BASE_DATE_JAPANESE_CALENDAR, "4240319")));
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test(expected = RuntimeException.class)
	public void testExecExceptionDigit1() throws Throwable {
		mGetDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, "x"));
	}

	@Test(expected = RuntimeException.class)
	public void testExecExceptionDigit8() throws Throwable {
		mGetDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, "yyyyMMdd"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testExecExceptionDigit7() throws Throwable {
		mGetDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, "GyyMMdd"));
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testSchema() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				TestUtil.createSchema(new FieldSchema(AliasConstants.GET_DAY_SPAN_OUT_ALIAS, DataType.INTEGER)),
				mGetDaySpan.outputSchema(null/* unused input schema */));
	}

	// -----------------------------------------------------------------------------------------------------------------

}
