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
