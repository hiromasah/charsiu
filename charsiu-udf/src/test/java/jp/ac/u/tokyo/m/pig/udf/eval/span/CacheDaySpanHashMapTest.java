package jp.ac.u.tokyo.m.pig.udf.eval.span;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import jp.ac.u.tokyo.m.pig.udf.eval.span.CacheDaySpanHashMap;
import jp.ac.u.tokyo.m.string.StringFormatConstants;
import junit.framework.Assert;

import org.junit.Test;

public class CacheDaySpanHashMapTest {

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testGetDaySpan() throws ParseException {
		CacheDaySpanHashMap tDaySpanMap = CacheDaySpanHashMap.INSTANCE;
		String tBaseDateString = "20000101";

		Assert.assertEquals(new Integer(0), tDaySpanMap.getDaySpan(tBaseDateString, "20000101"));
		Assert.assertEquals(new Integer(1), tDaySpanMap.getDaySpan(tBaseDateString, "20000102"));
		Assert.assertEquals(new Integer(366), tDaySpanMap.getDaySpan(tBaseDateString, "20010101"));

		Assert.assertEquals(new Integer(-365), tDaySpanMap.getDaySpan(tBaseDateString, "19990101"));
	}

	@Test
	public void testAddDaySpan() throws ParseException {
		CacheDaySpanHashMap tDaySpanMap = CacheDaySpanHashMap.INSTANCE;
		String tBaseDateString = "20000101";

		Assert.assertEquals("20000101", tDaySpanMap.addDaySpan(tBaseDateString, 0));
		Assert.assertEquals("20000102", tDaySpanMap.addDaySpan(tBaseDateString, 1));
		Assert.assertEquals("20010101", tDaySpanMap.addDaySpan(tBaseDateString, 366));

		Assert.assertEquals("19990101", tDaySpanMap.addDaySpan(tBaseDateString, -365));
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testCacheDaySpanHashMap() throws ParseException {
		CacheDaySpanHashMap tDaySpanMap = CacheDaySpanHashMap.INSTANCE;
		String tBaseDateString = "20110401";

		// キャッシュするので２回目以降は高速
		checkGetDaySpan(tDaySpanMap, tBaseDateString);
		checkAddDaySpan(tDaySpanMap, tBaseDateString);

		checkGetDaySpan(tDaySpanMap, tBaseDateString);
		checkAddDaySpan(tDaySpanMap, tBaseDateString);
	}

	private void checkGetDaySpan(CacheDaySpanHashMap aDaySpanMap, String aBaseDateString) throws ParseException {
		SimpleDateFormat tDateFormat = StringFormatConstants.FORMAT_DATE;
		Calendar tBaseDate = Calendar.getInstance();
		tBaseDate.setTime(tDateFormat.parse(aBaseDateString));
		long tStartTime = System.currentTimeMillis();
		for (Integer tCount = 0; tCount < 10000; tCount++) {
			Assert.assertEquals(tCount
					, aDaySpanMap.getDaySpan(aBaseDateString, tDateFormat.format(tBaseDate.getTime())));
			tBaseDate.add(Calendar.DATE, 1);
		}
		System.out.println((System.currentTimeMillis() - tStartTime) + " ms");
	}

	private void checkAddDaySpan(CacheDaySpanHashMap aDaySpanMap, String aBaseDateString) throws ParseException {
		SimpleDateFormat tDateFormat = StringFormatConstants.FORMAT_DATE;
		Calendar tBaseDate = Calendar.getInstance();
		tBaseDate.setTime(tDateFormat.parse(aBaseDateString));
		long tStartTime = System.currentTimeMillis();
		for (int tCount = 0; tCount < 10000; tCount++) {
			Assert.assertEquals(tDateFormat.format(tBaseDate.getTime())
					, aDaySpanMap.addDaySpan(aBaseDateString, tCount));
			tBaseDate.add(Calendar.DATE, 1);
		}
		System.out.println((System.currentTimeMillis() - tStartTime) + " ms");
	}

	// -----------------------------------------------------------------------------------------------------------------

}
