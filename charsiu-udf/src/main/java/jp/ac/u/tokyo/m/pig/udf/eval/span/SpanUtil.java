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
import java.util.Calendar;

public class SpanUtil {

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * This method calculate difference of date from a couple of Calendar Object. <br>
	 * <br>
	 * ２値間のミリ秒の差から日付の差を求めます。 <br>
	 * 現状、利用される日付が HH:mm:ss SSS = 00:00:00 000 なので、単純に計算しています。 <br>
	 */
	public static Integer calcDifferenceAsDays(Calendar aBaseDate, Calendar aTargetDate) {
		long tDifference = aTargetDate.getTimeInMillis() - aBaseDate.getTimeInMillis();
		// 86400000 = 1000[msec] * 60[sec] * 60[min] * 24[hour], Long.MAX_VALUE / 86400000 < Integer.MAX_VALUE
		return (int) (tDifference / 86400000);
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * This method adds a value of aAddDays to aTarget. <br>
	 * <br>
	 * aTarget に aAddDays の値の日数を加算します。 <br>
	 */
	public static void addDays(Calendar aTarget, int aAddDays) {
		aTarget.add(Calendar.DATE, aAddDays);
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * This method calculate difference of years from a couple of Calendar Object. <br>
	 * <br>
	 * 2つの日付から年数の差を求めます。 <br>
	 * 年齢の算出方法（翌年同日であれば1年経過と判定）と同じです。 <br>
	 */
	public static Integer calcDifferenceAsYears(Calendar aBaseDate, Calendar aTargetDate) throws ParseException {
		if (aBaseDate.getTimeInMillis() > aTargetDate.getTimeInMillis()) {
			return -1 * calcDifferenceAsYearsPrivate(aTargetDate, aBaseDate);
		} else {
			return calcDifferenceAsYearsPrivate(aBaseDate, aTargetDate);
		}
	}

	private static int calcDifferenceAsYearsPrivate(Calendar aBaseDate, Calendar aTargetDate) {
		int tBaseDateMonth = aBaseDate.get(Calendar.MONTH);
		int tTargetDateMonth = aTargetDate.get(Calendar.MONTH);
		return aTargetDate.get(Calendar.YEAR) - aBaseDate.get(Calendar.YEAR) -
				(tBaseDateMonth > tTargetDateMonth ||
						(tBaseDateMonth == tTargetDateMonth && aBaseDate.get(Calendar.DATE) > aTargetDate.get(Calendar.DATE))
						? 1 : 0);
	}

	// -----------------------------------------------------------------------------------------------------------------

}
