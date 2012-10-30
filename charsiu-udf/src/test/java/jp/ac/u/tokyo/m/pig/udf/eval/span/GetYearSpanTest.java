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
import jp.ac.u.tokyo.m.pig.udf.eval.span.GetYearSpan;
import jp.ac.u.tokyo.m.test.TestUtil;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.BeforeClass;
import org.junit.Test;

public class GetYearSpanTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static final String BASE_DATE_WESTERN_CALENDAR = "20120326";
	private static final String BASE_DATE_JAPANESE_CALENDAR = "4240326";

	private static GetYearSpan mGetYearSpan;

	// -----------------------------------------------------------------------------------------------------------------

	@BeforeClass
	public static void init() {
		mGetYearSpan = new GetYearSpan();
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testExecOutput() throws Throwable {
		TestUtil.assertEqualsPigObjects(0, mGetYearSpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, BASE_DATE_WESTERN_CALENDAR)));

		TestUtil.assertEqualsPigObjects(1, mGetYearSpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, "20130326")));
		TestUtil.assertEqualsPigObjects(-1, mGetYearSpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, "20110326")));

		TestUtil.assertEqualsPigObjects(1, mGetYearSpan.exec(TestUtil.createTuple("20120102", "20140101")));
		TestUtil.assertEqualsPigObjects(-1, mGetYearSpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, "20100327")));
	}

	@Test
	public void testExecOutputJapaneseCalendar() throws Throwable {
		TestUtil.assertEqualsPigObjects(0, mGetYearSpan.exec(TestUtil.createTuple(BASE_DATE_JAPANESE_CALENDAR, BASE_DATE_WESTERN_CALENDAR)));
		TestUtil.assertEqualsPigObjects(0, mGetYearSpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, BASE_DATE_JAPANESE_CALENDAR)));
		TestUtil.assertEqualsPigObjects(0, mGetYearSpan.exec(TestUtil.createTuple(BASE_DATE_JAPANESE_CALENDAR, BASE_DATE_JAPANESE_CALENDAR)));

		TestUtil.assertEqualsPigObjects(1, mGetYearSpan.exec(TestUtil.createTuple(BASE_DATE_JAPANESE_CALENDAR, "4250326")));
		TestUtil.assertEqualsPigObjects(-1, mGetYearSpan.exec(TestUtil.createTuple(BASE_DATE_JAPANESE_CALENDAR, "4230326")));
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test(expected = RuntimeException.class)
	public void testExecExceptionDigit1() throws Throwable {
		mGetYearSpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, "x"));
	}

	@Test(expected = RuntimeException.class)
	public void testExecExceptionDigit8() throws Throwable {
		mGetYearSpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, "yyyyMMdd"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testExecExceptionDigit7() throws Throwable {
		mGetYearSpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, "GyyMMdd"));
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testSchema() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				TestUtil.createSchema(new FieldSchema(AliasConstants.GET_YEAR_SPAN_OUT_ALIAS, DataType.INTEGER)),
				mGetYearSpan.outputSchema(null/* unused input schema */));
	}

	// -----------------------------------------------------------------------------------------------------------------

}
