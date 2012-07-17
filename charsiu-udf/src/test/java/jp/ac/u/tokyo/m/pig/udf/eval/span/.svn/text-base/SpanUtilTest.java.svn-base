package jp.ac.u.tokyo.m.pig.udf.eval.span;

import java.text.ParseException;
import java.util.Calendar;

import jp.ac.u.tokyo.m.pig.udf.eval.span.SpanUtil;

import org.junit.Assert;
import org.junit.Test;

public class SpanUtilTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static final Calendar BASE_DATE = createDate(2000, 0, 2);

	// -----------------------------------------------------------------------------------------------------------------

	private static Calendar createDate(int aYear, int aMonth, int aDate) {
		Calendar tBaseDate = Calendar.getInstance();
		tBaseDate.set(aYear, aMonth, aDate, 0, 0, 0);
		tBaseDate.set(Calendar.MILLISECOND, 0);
		return tBaseDate;
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testCalcDifferenceAsDays() {
		Assert.assertEquals(new Integer(0), SpanUtil.calcDifferenceAsDays(BASE_DATE, BASE_DATE));
		Assert.assertEquals(new Integer(1), SpanUtil.calcDifferenceAsDays(BASE_DATE, createDate(2000, 0, 3)));
		Assert.assertEquals(new Integer(366), SpanUtil.calcDifferenceAsDays(BASE_DATE, createDate(2001, 0, 2)));

		Assert.assertEquals(new Integer(-1), SpanUtil.calcDifferenceAsDays(createDate(2000, 0, 3), BASE_DATE));
		Assert.assertEquals(new Integer(-366), SpanUtil.calcDifferenceAsDays(createDate(2001, 0, 2), BASE_DATE));
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testAddDays() {
		Calendar tBaseDate = (Calendar) BASE_DATE.clone();
		SpanUtil.addDays(tBaseDate, 0);
		Assert.assertEquals(createDate(2000, 0, 2), tBaseDate);
		SpanUtil.addDays(tBaseDate, 1);
		Assert.assertEquals(createDate(2000, 0, 3), tBaseDate);
		SpanUtil.addDays(tBaseDate, 365);
		Assert.assertEquals(createDate(2001, 0, 2), tBaseDate);
		SpanUtil.addDays(tBaseDate, -365);
		Assert.assertEquals(createDate(2000, 0, 3), tBaseDate);
		SpanUtil.addDays(tBaseDate, -1);
		Assert.assertEquals(createDate(2000, 0, 2), tBaseDate);
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testCalcDifferenceAsYear() throws ParseException {
		Assert.assertEquals(new Integer(0), SpanUtil.calcDifferenceAsYears(BASE_DATE, BASE_DATE));
		Assert.assertEquals(new Integer(0), SpanUtil.calcDifferenceAsYears(BASE_DATE, createDate(2001, 0, 1)));
		Assert.assertEquals(new Integer(1), SpanUtil.calcDifferenceAsYears(BASE_DATE, createDate(2001, 0, 2)));

		Assert.assertEquals(new Integer(0), SpanUtil.calcDifferenceAsYears(createDate(2001, 0, 1), BASE_DATE));
		Assert.assertEquals(new Integer(-1), SpanUtil.calcDifferenceAsYears(createDate(2001, 0, 2), BASE_DATE));
	}

	// -----------------------------------------------------------------------------------------------------------------

}
