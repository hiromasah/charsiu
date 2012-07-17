package jp.ac.u.tokyo.m.pig.udf.eval.span;

import java.text.ParseException;
import java.util.Calendar;

public class SpanUtil {

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * ２値間のミリ秒の差から日付の差を求めます。
	 * 現状、利用される日付が HH:mm:ss SSS = 00:00:00 000 なので、単純に計算しています。
	 */
	public static Integer calcDifferenceAsDays(Calendar aBaseDate, Calendar aTargetDate) {
		long tDifference = aTargetDate.getTimeInMillis() - aBaseDate.getTimeInMillis();
		// 86400000 = 1000[msec] * 60[sec] * 60[min] * 24[hour], Long.MAX_VALUE / 86400000 < Integer.MAX_VALUE
		return (int) (tDifference / 86400000);
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * aTarget に aAddDays の値の日数を加算します。
	 */
	public static void addDays(Calendar aTarget, int aAddDays) {
		aTarget.add(Calendar.DATE, aAddDays);
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * 2つの日付から年数の差を求めます。
	 * 年齢の算出方法（翌年同日であれば1年経過と判定）と同じです。
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
