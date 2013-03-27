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

import jp.ac.u.tokyo.m.pig.udf.eval.util.MulticastEvaluationConstants.AccessType;
import jp.ac.u.tokyo.m.test.TestUtil;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.BeforeClass;
import org.junit.Test;

public class StringableColumnAccessorTest {

	private static StringableColumnAccessor mInstance;
	private static String mInstanceString;

	private static DefaultColumnAccessor mInstanceDefaultColumnAccessor;

	private static DefaultColumnAccessor mInstanceDefaultColumnAccessorSimple;
	private static String mInstanceStringSimple;

	@BeforeClass
	public static void init() throws FrontendException {
		ArrayList<ColumnIndexInformation> tColumnIndexInformations = new ArrayList<ColumnIndexInformation>();
		tColumnIndexInformations.add(
				new StringableColumnIndexInformation(0, DataType.BAG, AccessType.FLAT,
						new StringableColumnIndexInformation(0, DataType.TUPLE, AccessType.SUB_BAG,
								new StringableColumnIndexInformation(0, DataType.INTEGER, AccessType.SUB_BAG, null))));
		tColumnIndexInformations.add(
				new StringableColumnIndexInformation(0, DataType.BAG, AccessType.FLAT,
						new StringableColumnIndexInformation(0, DataType.TUPLE, AccessType.SUB_BAG,
								new StringableColumnIndexInformation(3, DataType.INTEGER, AccessType.SUB_BAG, null))));
		tColumnIndexInformations.add(
				new StringableColumnIndexInformation(0, DataType.BAG, AccessType.FLAT,
						new StringableColumnIndexInformation(0, DataType.TUPLE, AccessType.SUB_BAG,
								new StringableColumnIndexInformation(4, DataType.INTEGER, AccessType.SUB_BAG, null))));
		mInstance = new StringableColumnAccessor(tColumnIndexInformations);

		mInstanceString = "*0,120,0,*0,110,1,*0,10,1," + "#" + "*0,120,0,*0,110,1,*3,10,1," + "#" + "*0,120,0,*0,110,1,*4,10,1,";

		FieldSchema tColumn0FieldSchemaSensitive = new FieldSchema("column_0", DataType.INTEGER);
		FieldSchema tColumn1FieldSchemaSensitive = new FieldSchema("column_1", DataType.INTEGER);
		FieldSchema tColumn2FieldSchemaSensitive = new FieldSchema("column_2", DataType.INTEGER);
		FieldSchema tColumn3FieldSchemaSensitive = new FieldSchema("column_3", DataType.INTEGER);
		FieldSchema tColumn4FieldSchemaSensitive = new FieldSchema("column_4", DataType.INTEGER);
		FieldSchema tScoreTupleFieldSchemaSensitive = new FieldSchema("bag_tuple",
				TestUtil.createSchema(
						tColumn0FieldSchemaSensitive,
						tColumn1FieldSchemaSensitive,
						tColumn2FieldSchemaSensitive,
						tColumn3FieldSchemaSensitive,
						tColumn4FieldSchemaSensitive
						),
				DataType.TUPLE);
		FieldSchema tInputFieldSchemaSensitive = new FieldSchema("top_bag",
				TestUtil.createSchema(tScoreTupleFieldSchemaSensitive),
				DataType.BAG);
		mInstanceDefaultColumnAccessor = new DefaultColumnAccessor("_.bag_tuple.column_0, _.bag_tuple.column_3, _.bag_tuple.column_4", tInputFieldSchemaSensitive);

		// 多くの場合は下記のように single access で delimiter "#" は出現しない
		mInstanceDefaultColumnAccessorSimple = new DefaultColumnAccessor("_.bag_tuple.column_0", tInputFieldSchemaSensitive);
		mInstanceStringSimple = "*0,120,0,*0,110,1,*0,10,1,";
	}

	/**
	 * Test of String => StringableColumnAccessor
	 */
	@Test
	public void testStringToInstance() {
		TestUtil.assertEqualsPigObjects(
				mInstance,
				StringableColumnAccessor.parse(mInstanceString));
	}

	/**
	 * Test of StringableColumnAccessor => String
	 */
	@Test
	public void testInstanceToString() {
		TestUtil.assertEqualsPigObjects(
				mInstanceString,
				mInstance.toString());
	}

	/**
	 * Test of DefaultColumnAccessor => String
	 */
	@Test
	public void testDefaultColumnAccessorToString() {
		TestUtil.assertEqualsPigObjects(
				mInstanceString,
				StringableColumnAccessor.toString(mInstanceDefaultColumnAccessor));
	}

	/**
	 * Test of DefaultColumnAccessor => String
	 * single column access
	 */
	@Test
	public void testDefaultColumnAccessorToString_Simple() {
		TestUtil.assertEqualsPigObjects(
				mInstanceStringSimple,
				StringableColumnAccessor.toString(mInstanceDefaultColumnAccessorSimple));
	}

}
