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

import junit.framework.Assert;

import org.junit.Test;

public class CalendarTest {

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testJapaneseCalendarCaster() throws Throwable {
		JapaneseCalendarCaster tCaster = CalendarConstants.JAPANESE_CALENDAR_CASTER;

		Assert.assertEquals("20110715", tCaster.castToWesternCalendarFormat("4230715"));
		Assert.assertEquals("19890108", tCaster.castToWesternCalendarFormat("4010108"));

		Assert.assertEquals("19890107", tCaster.castToWesternCalendarFormat("3640107"));
		Assert.assertEquals("19261225", tCaster.castToWesternCalendarFormat("3011225"));

		Assert.assertEquals("19261225", tCaster.castToWesternCalendarFormat("2151225"));
		Assert.assertEquals("19120730", tCaster.castToWesternCalendarFormat("2010730"));

		Assert.assertEquals("19120730", tCaster.castToWesternCalendarFormat("1450730"));
		Assert.assertEquals("18680908", tCaster.castToWesternCalendarFormat("1010908"));

		Assert.assertEquals("1989", tCaster.castToWesternCalendarFormat("401"));
		Assert.assertEquals("1989", tCaster.castToWesternCalendarFormat("H01"));
	}

	@Test
	public void testCalendarUtil() throws Throwable {
		Assert.assertEquals("20110715", CalendarUtil.getWesternCalendarFormatDate("4230715"));
		Assert.assertEquals("20110715", CalendarUtil.getWesternCalendarFormatDate("20110715"));
		Assert.assertEquals("19890108", CalendarUtil.getWesternCalendarFormatDate("4010108"));
		Assert.assertEquals("19890108", CalendarUtil.getWesternCalendarFormatDate("19890108"));

		Assert.assertEquals("19890107", CalendarUtil.getWesternCalendarFormatDate("3640107"));
		Assert.assertEquals("19261225", CalendarUtil.getWesternCalendarFormatDate("3011225"));

		Assert.assertEquals("19261225", CalendarUtil.getWesternCalendarFormatDate("2151225"));
		Assert.assertEquals("19120730", CalendarUtil.getWesternCalendarFormatDate("2010730"));

		Assert.assertEquals("19120730", CalendarUtil.getWesternCalendarFormatDate("1450730"));
		Assert.assertEquals("18680908", CalendarUtil.getWesternCalendarFormatDate("1010908"));

		Assert.assertEquals("18680101", CalendarUtil.getWesternCalendarFormatDate("1010101"));
		Assert.assertEquals("19120101", CalendarUtil.getWesternCalendarFormatDate("2010101"));
		Assert.assertEquals("19260101", CalendarUtil.getWesternCalendarFormatDate("3010101"));
		Assert.assertEquals("19890101", CalendarUtil.getWesternCalendarFormatDate("4010101"));
	}

	// -----------------------------------------------------------------------------------------------------------------

}
