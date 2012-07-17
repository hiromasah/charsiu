package jp.ac.u.tokyo.m.pig.udf.eval.span;

import jp.ac.u.tokyo.m.pig.udf.AliasConstants;
import jp.ac.u.tokyo.m.pig.udf.eval.span.AddDaySpan;
import jp.ac.u.tokyo.m.test.TestUtil;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.BeforeClass;
import org.junit.Test;

public class AddDaySpanTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static final String BASE_DATE_WESTERN_CALENDAR = "20120326";
	private static final String BASE_DATE_JAPANESE_CALENDAR = "4240326";

	private static AddDaySpan mAddDaySpan;

	// -----------------------------------------------------------------------------------------------------------------

	@BeforeClass
	public static void init() {
		mAddDaySpan = new AddDaySpan();
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testExecOutput() throws Throwable {
		TestUtil.assertEqualsPigObjects(BASE_DATE_WESTERN_CALENDAR, mAddDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, 0)));

		TestUtil.assertEqualsPigObjects("20120327", mAddDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, 1)));
		TestUtil.assertEqualsPigObjects("20120402", mAddDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, 7)));
		TestUtil.assertEqualsPigObjects("20130326", mAddDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, 365)));

		TestUtil.assertEqualsPigObjects("20120325", mAddDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, -1)));
		TestUtil.assertEqualsPigObjects("20120319", mAddDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, -7)));
		TestUtil.assertEqualsPigObjects("20110326", mAddDaySpan.exec(TestUtil.createTuple(BASE_DATE_WESTERN_CALENDAR, -366)));
	}

	@Test
	public void testExecOutputJapaneseCalendar() throws Throwable {
		TestUtil.assertEqualsPigObjects(BASE_DATE_WESTERN_CALENDAR, mAddDaySpan.exec(TestUtil.createTuple(BASE_DATE_JAPANESE_CALENDAR, 0)));

		TestUtil.assertEqualsPigObjects("20120402", mAddDaySpan.exec(TestUtil.createTuple(BASE_DATE_JAPANESE_CALENDAR, 7)));
		TestUtil.assertEqualsPigObjects("20120319", mAddDaySpan.exec(TestUtil.createTuple(BASE_DATE_JAPANESE_CALENDAR, -7)));
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test(expected = RuntimeException.class)
	public void testExecExceptionDigit1() throws Throwable {
		mAddDaySpan.exec(TestUtil.createTuple("x", 0));
	}

	@Test(expected = RuntimeException.class)
	public void testExecExceptionDigit8() throws Throwable {
		mAddDaySpan.exec(TestUtil.createTuple("yyyyMMdd", 0));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testExecExceptionDigit7() throws Throwable {
		mAddDaySpan.exec(TestUtil.createTuple("GyyMMdd", 0));
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testSchema() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				TestUtil.createSchema(new FieldSchema(AliasConstants.ADD_DAY_SPAN_OUT_ALIAS, DataType.CHARARRAY)),
				mAddDaySpan.outputSchema(null/* unused input schema */));
	}

	// -----------------------------------------------------------------------------------------------------------------

}
