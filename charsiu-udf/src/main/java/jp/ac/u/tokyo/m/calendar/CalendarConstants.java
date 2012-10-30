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

import java.util.Properties;

import jp.ac.u.tokyo.m.resource.ResourceLoadUtil;

public class CalendarConstants {

	static final String JAPANESE_CALENDAR_OFFSET_INI = "japanese-calendar-offset.ini";

	static final Properties JAPANESE_CALENDAR_OFFSET = ResourceLoadUtil.loadNecessaryPackagePrivateProperties(
			new CalendarConstants().getClass(),
			JAPANESE_CALENDAR_OFFSET_INI);

	public static final JapaneseCalendarCaster JAPANESE_CALENDAR_CASTER = new CacheJapaneseCalendarCaster();

}
