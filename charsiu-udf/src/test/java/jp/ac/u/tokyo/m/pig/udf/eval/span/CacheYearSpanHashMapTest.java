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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import jp.ac.u.tokyo.m.pig.udf.eval.span.CacheYearSpanHashMap;
import jp.ac.u.tokyo.m.string.StringFormatConstants;
import junit.framework.Assert;

import org.junit.Test;

public class CacheYearSpanHashMapTest {

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testCacheYearSpanHashMap() throws ParseException {
		CacheYearSpanHashMap tYearSpanMap = CacheYearSpanHashMap.INSTANCE;
		String tBaseDateString = "30010715";

		Integer tExpected = 0;
		Assert.assertEquals(tExpected, tYearSpanMap.getYearSpan(tBaseDateString, "30010715"));
		Assert.assertEquals(tExpected, tYearSpanMap.getYearSpan(tBaseDateString, "30020714"));

		tExpected = 1;
		Assert.assertEquals(tExpected, tYearSpanMap.getYearSpan(tBaseDateString, "30020715"));
		Assert.assertEquals(tExpected, tYearSpanMap.getYearSpan(tBaseDateString, "30030714"));

		tExpected = 2;
		Assert.assertEquals(tExpected, tYearSpanMap.getYearSpan(tBaseDateString, "30030715"));
		Assert.assertEquals(tExpected, tYearSpanMap.getYearSpan(tBaseDateString, "30040714"));
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testTimeSingleYearMapFactory() throws ParseException {
		CacheYearSpanHashMap tYearSpanMap = CacheYearSpanHashMap.INSTANCE;
		String tBaseDateString = "20110401";

		// キャッシュするので２回目以降は高速
		checkGetYearSpan(tYearSpanMap, tBaseDateString);
		checkGetYearSpan(tYearSpanMap, tBaseDateString);
	}

	private void checkGetYearSpan(CacheYearSpanHashMap aYearSpanMap, String aBaseDateString) throws ParseException {
		SimpleDateFormat tDateFormat = StringFormatConstants.FORMAT_DATE.get();
		Calendar tBaseDate = Calendar.getInstance();
		tBaseDate.setTime(tDateFormat.parse(aBaseDateString));
		long tStartTime = System.currentTimeMillis();
		for (Long tCount = 0L; tCount < 10000; tCount++) {
			aYearSpanMap.getYearSpan(aBaseDateString, tDateFormat.format(tBaseDate.getTime()));
			tBaseDate.add(Calendar.DATE, 1);
		}
		System.out.println((System.currentTimeMillis() - tStartTime) + " ms");
	}

	// -----------------------------------------------------------------------------------------------------------------

}
