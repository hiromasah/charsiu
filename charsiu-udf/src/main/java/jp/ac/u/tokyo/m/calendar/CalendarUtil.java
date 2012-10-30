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

package jp.ac.u.tokyo.m.calendar;

public class CalendarUtil {

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * A variety of format year-month-day string convert to Christian era format(yyyyMMdd).<br>
	 * フォーマットのばらついた年月日文字列を、西暦年月日文字列に整形します。 <br>
	 * 
	 * @param aDate
	 *            A variety of format year-month-day string <br>
	 *            任意のフォーマットの年月日文字列 <br>
	 * @return
	 *         Christian era format(yyyyMMdd) <br>
	 *         西暦年月日文字列 <br>
	 */
	public static String getWesternCalendarFormatDate(String aDate) {
		String tDate = aDate.trim();
		int tDateLength = tDate.length();
		// digit 8 : Christian era | 西暦
		if (tDateLength == 8) {
			return aDate;
		}
		// digit 7 : Japanese Calendar = ( [Era Code(１-character)] + [YYMMdd] ) | 和暦 = ( [元号コード(１文字)] + [YYMMdd] )
		else if (tDateLength == 7) {
			return CalendarConstants.JAPANESE_CALENDAR_CASTER.castToWesternCalendarFormat(tDate);
		}
		return aDate;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
