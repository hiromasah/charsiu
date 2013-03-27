/*
 * Copyright 2012-2013 Hiromasa Horiguchi ( The University of Tokyo )
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

package jp.ac.u.tokyo.m.pig.udf.eval.util;

import java.util.ArrayList;
import java.util.List;

import jp.ac.u.tokyo.m.test.TestUtil;

import org.junit.BeforeClass;
import org.junit.Test;

public class StringableUtilTest {

	// TODO String => List<ColumnEvaluator>
	// TODO ^ reverse
	// TODO String => List<ColumnEvaluationSettings>
	// TODO ^ reverse

	// -----------------------------------------------------------------------------------------------------------------

	private static String mColumnEvaluatorsString;
	private static List<ColumnEvaluator> mColumnEvaluators;

	private static String mColumnEvaluationSettingsString;
	private static List<ColumnEvaluationSetting> mColumnEvaluationSettings;

	@BeforeClass
	public static void init() throws InstantiationException, IllegalAccessException {
		String tColumnAccessorString = "*0,120,0,*0,110,1,*0,10,1,";
		String tUDFNameMax = "org.apache.pig.builtin.IntMax";
		String tUDFNameMin = "org.apache.pig.builtin.IntMin";
		String tColumnEvaluatorString1 = tColumnAccessorString + "@" + tUDFNameMax;
		String tColumnEvaluatorString2 = tColumnAccessorString + "@" + tUDFNameMin;
		mColumnEvaluatorsString = tColumnEvaluatorString1 + "&" + tColumnEvaluatorString2;

		mColumnEvaluators = new ArrayList<ColumnEvaluator>();
		mColumnEvaluators.add(StringableColumnEvaluator.parse(tColumnEvaluatorString1));
		mColumnEvaluators.add(StringableColumnEvaluator.parse(tColumnEvaluatorString2));

		mColumnEvaluationSettingsString = "%" + mColumnEvaluatorsString + "%" + mColumnEvaluatorsString;

		mColumnEvaluationSettings = new ArrayList<ColumnEvaluationSetting>();
		mColumnEvaluationSettings.add(new ColumnEvaluationSetting(StringableUtil.parseColumnEvaluatorsString("")));
		mColumnEvaluationSettings.add(new ColumnEvaluationSetting(StringableUtil.parseColumnEvaluatorsString(mColumnEvaluatorsString)));
		mColumnEvaluationSettings.add(new ColumnEvaluationSetting(StringableUtil.parseColumnEvaluatorsString(mColumnEvaluatorsString)));
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Test of String => List&lt;ColumnEvaluator&gt;
	 */
	@Test
	public void testStringToColumnEvaluators() throws InstantiationException, IllegalAccessException {
		TestUtil.assertEqualsPigObjects(
				mColumnEvaluators,
				StringableUtil.parseColumnEvaluatorsString(mColumnEvaluatorsString));
	}

	/**
	 * Test of List&lt;ColumnEvaluator&gt; => String
	 */
	@Test
	public void testColumnEvaluatorsToString() {
		TestUtil.assertEqualsPigObjects(
				mColumnEvaluatorsString,
				StringableUtil.toStringColumnEvaluators(mColumnEvaluators));
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Test of String => List&lt;ColumnEvaluationSetting&gt;
	 */
	@Test
	public void testStringToColumnEvaluationSettings() throws InstantiationException, IllegalAccessException {
		TestUtil.assertEqualsPigObjects(
				mColumnEvaluationSettings,
				StringableUtil.parseColumnEvaluationSettingsString(mColumnEvaluationSettingsString));
	}

	/**
	 * Test of List&lt;ColumnEvaluationSetting&gt; => String
	 */
	@Test
	public void testColumnEvaluationSettingsToString() {
		TestUtil.assertEqualsPigObjects(
				mColumnEvaluationSettingsString,
				StringableUtil.toStringColumnEvaluationSettings(mColumnEvaluationSettings));
	}

	// -----------------------------------------------------------------------------------------------------------------

}
