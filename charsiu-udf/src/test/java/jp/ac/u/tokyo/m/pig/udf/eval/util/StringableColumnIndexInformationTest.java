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

import jp.ac.u.tokyo.m.pig.udf.eval.util.MulticastEvaluationConstants.AccessType;
import jp.ac.u.tokyo.m.test.TestUtil;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.BeforeClass;
import org.junit.Test;

public class StringableColumnIndexInformationTest {

	private static StringableColumnIndexInformation mTop;
	private static StringableColumnIndexInformation mChild;
	private static StringableColumnIndexInformation mChildChild;

	private static String mTopString;
	private static String mChildString;
	private static String mChildChildString;

	private static DefaultColumnIndexInformation mTopDefaultColumnIndexInformation;
	private static DefaultColumnIndexInformation mChildDefaultColumnIndexInformation;
	private static DefaultColumnIndexInformation mChildChildDefaultColumnIndexInformation;

	@BeforeClass
	public static void init() throws FrontendException {
		mChildChild = new StringableColumnIndexInformation(0, DataType.INTEGER, AccessType.SUB_BAG, null);
		mChild = new StringableColumnIndexInformation(0, DataType.TUPLE, AccessType.SUB_BAG, mChildChild);
		mTop = new StringableColumnIndexInformation(0, DataType.BAG, AccessType.FLAT, mChild);

		mChildChildString = "*0,10,1,";
		mChildString = "*0,110,1,*0,10,1,";
		mTopString = "*0,120,0,*0,110,1,*0,10,1,";

		FieldSchema tScoreFieldSchemaSensitive = new FieldSchema("score", DataType.INTEGER);
		FieldSchema tRateFieldSchemaSensitive = new FieldSchema("rate", DataType.INTEGER);
		FieldSchema tScoreTupleFieldSchemaSensitive = new FieldSchema("score_tuple",
				TestUtil.createSchema(
						tScoreFieldSchemaSensitive,
						tRateFieldSchemaSensitive
						),
				DataType.TUPLE);
		FieldSchema tInputFieldSchemaSensitive = new FieldSchema("score_1st",
				TestUtil.createSchema(tScoreTupleFieldSchemaSensitive),
				DataType.BAG);
		mChildChildDefaultColumnIndexInformation = new DefaultColumnIndexInformation(0, tScoreFieldSchemaSensitive, AccessType.SUB_BAG);
		mChildDefaultColumnIndexInformation = new DefaultColumnIndexInformation(0, tScoreTupleFieldSchemaSensitive, AccessType.SUB_BAG);
		mChildDefaultColumnIndexInformation.setChild(mChildChildDefaultColumnIndexInformation);
		mTopDefaultColumnIndexInformation = new DefaultColumnIndexInformation(0, tInputFieldSchemaSensitive, AccessType.FLAT);
		mTopDefaultColumnIndexInformation.setChild(mChildDefaultColumnIndexInformation);
	}

	/**
	 * Test of String => StringableColumnIndexInformation
	 */
	@Test
	public void testStringToInstance() {
		TestUtil.assertEqualsPigObjects(
				mChildChild,
				StringableColumnIndexInformation.parse(mChildChildString));
		TestUtil.assertEqualsPigObjects(
				mChild,
				StringableColumnIndexInformation.parse(mChildString));
		TestUtil.assertEqualsPigObjects(
				mTop,
				StringableColumnIndexInformation.parse(mTopString));
	}

	/**
	 * Test of StringableColumnIndexInformation => String
	 */
	@Test
	public void testInstanceToString() {
		TestUtil.assertEqualsPigObjects(
				mChildChildString,
				mChildChild.toString());
		TestUtil.assertEqualsPigObjects(
				mChildString,
				mChild.toString());
		TestUtil.assertEqualsPigObjects(
				mTopString,
				mTop.toString());
	}

	/**
	 * Test of DefaultColumnIndexInformation => String
	 */
	@Test
	public void testDefaultColumnIndexInformationToString() {
		TestUtil.assertEqualsPigObjects(
				mChildChildString,
				StringableColumnIndexInformation.toString(mChildChildDefaultColumnIndexInformation));
		TestUtil.assertEqualsPigObjects(
				mChildString,
				StringableColumnIndexInformation.toString(mChildDefaultColumnIndexInformation));
		TestUtil.assertEqualsPigObjects(
				mTopString,
				StringableColumnIndexInformation.toString(mTopDefaultColumnIndexInformation));
	}

}
