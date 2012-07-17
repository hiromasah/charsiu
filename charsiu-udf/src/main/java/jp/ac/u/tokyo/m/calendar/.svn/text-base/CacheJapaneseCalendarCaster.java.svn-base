package jp.ac.u.tokyo.m.calendar;

import java.util.HashMap;

public class CacheJapaneseCalendarCaster implements JapaneseCalendarCaster {

	// -----------------------------------------------------------------------------------------------------------------

	HashMap<String, String> mJapaneseCalendarDictionaryMap;

	HashMap<String, Integer> mJapaneseCalendarYearOffsetMap;

	HashMap<String, Integer> mJapaneseCalendarYearParsedMap;

	// -----------------------------------------------------------------------------------------------------------------

	public CacheJapaneseCalendarCaster() {
		mJapaneseCalendarDictionaryMap = new HashMap<String, String>();
		mJapaneseCalendarYearOffsetMap = new HashMap<String, Integer>();
		mJapaneseCalendarYearParsedMap = new HashMap<String, Integer>();
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public String castToWesternCalendarFormat(String aDateOfJapaneseCalendarFormat) {
		String tDateOfWesternCalendarFormat = mJapaneseCalendarDictionaryMap.get(aDateOfJapaneseCalendarFormat);
		if (tDateOfWesternCalendarFormat == null) {
			String tJapaneseCalendarCode = aDateOfJapaneseCalendarFormat.substring(0, 1);
			String tJapaneseCalendarYear = aDateOfJapaneseCalendarFormat.substring(1, 3);
			Integer tJapaneseCalendarYearOffset = mJapaneseCalendarYearOffsetMap.get(tJapaneseCalendarCode);
			if (tJapaneseCalendarYearOffset == null) {
				try {
					tJapaneseCalendarYearOffset = Integer.parseInt(CalendarConstants.JAPANESE_CALENDAR_OFFSET.getProperty(tJapaneseCalendarCode));
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException(CalendarConstants.JAPANESE_CALENDAR_OFFSET_INI + " に未定義の和暦コードが検出されました : " + tJapaneseCalendarCode, e);
				}
				mJapaneseCalendarYearOffsetMap.put(tJapaneseCalendarCode, tJapaneseCalendarYearOffset);
			}
			Integer tJapaneseCalendarYearParsed = mJapaneseCalendarYearParsedMap.get(tJapaneseCalendarYear);
			if (tJapaneseCalendarYearParsed == null) {
				tJapaneseCalendarYearParsed = Integer.parseInt(tJapaneseCalendarYear);
				mJapaneseCalendarYearParsedMap.put(tJapaneseCalendarYear, tJapaneseCalendarYearParsed);
			}
			String tWesternCalendarYear = Integer.toString(tJapaneseCalendarYearOffset + tJapaneseCalendarYearParsed);
			tDateOfWesternCalendarFormat = tWesternCalendarYear + aDateOfJapaneseCalendarFormat.substring(3);
			mJapaneseCalendarDictionaryMap.put(aDateOfJapaneseCalendarFormat, tDateOfWesternCalendarFormat);
		}
		return tDateOfWesternCalendarFormat;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
