package jp.ac.u.tokyo.m.calendar;

import java.util.Properties;

import jp.ac.u.tokyo.m.resource.ResourceLoadUtil;

public class CalendarConstants {

	static final String JAPANESE_CALENDAR_OFFSET_INI = "japanese-calendar-offset.ini";

	static final Properties JAPANESE_CALENDAR_OFFSET = ResourceLoadUtil.loadNecessaryPackagePrivateProperties(
			new CalendarConstants().getClass(),
			JAPANESE_CALENDAR_OFFSET_INI);

	public static final JapaneseCalendarCaster JAPANESE_CALENDAR_CASTER = new CacheJapaneseCalendarCaster();

}
