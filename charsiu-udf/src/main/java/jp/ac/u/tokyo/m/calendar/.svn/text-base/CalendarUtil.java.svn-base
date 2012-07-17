package jp.ac.u.tokyo.m.calendar;

public class CalendarUtil {

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * フォーマットのばらついた年月日文字列を、西暦年月日文字列に整形します。
	 * 
	 * @param aDate
	 *            任意のフォーマットの年月日文字列
	 * @return 西暦年月日文字列
	 */
	public static String getWesternCalendarFormatDate(String aDate) {
		String tDate = aDate.trim();
		int tDateLength = tDate.length();
		// 8桁 : 西暦
		if (tDateLength == 8) {
			return aDate;
		}
		// 7桁 : 和暦 = ( [元号コード(１文字)] + [YYMMdd] )
		else if (tDateLength == 7) {
			return CalendarConstants.JAPANESE_CALENDAR_CASTER.castToWesternCalendarFormat(tDate);
		}
		return aDate;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
