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

import jp.ac.u.tokyo.m.test.TestUtil;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.BeforeClass;
import org.junit.Test;

public class StringableColumnEvaluatorTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static StringableColumnEvaluator mInstanceThrough;
	private static String mInstanceThroughString;

	@BeforeClass
	public static void initThrough() {
		mInstanceThrough = new StringableColumnEvaluator(ThroughColumnEvaluator.INSTANCE);
		mInstanceThroughString = "";
	}

	/**
	 * Test of String => StringableColumnAccessor(ThroughColumnEvaluator)
	 */
	@Test
	public void testStringToInstance_Through() throws InstantiationException, IllegalAccessException {
		TestUtil.assertEqualsPigObjects(
				mInstanceThrough,
				StringableColumnEvaluator.parse(mInstanceThroughString));
	}

	/**
	 * Test of StringableColumnAccessor(ThroughColumnEvaluator) => String
	 */
	@Test
	public void testInstanceToString_Through() {
		TestUtil.assertEqualsPigObjects(
				mInstanceThroughString,
				StringableColumnEvaluator.toString(mInstanceThrough));
		TestUtil.assertEqualsPigObjects(
				mInstanceThroughString,
				StringableColumnEvaluator.toString(ThroughColumnEvaluator.INSTANCE));
	}

	// -----------------------------------------------------------------------------------------------------------------

	private static StringableColumnEvaluator mInstanceDefault;
	private static String mInstanceDefaultString;

	private static DefaultColumnEvaluator mDefaultColumnEvaluatorInstance;
	private static DefaultColumnAccessor mDefaultColumnAccessor;
	private static String mColumnAccessorString;
	private static String mUDFName;
	private static EvalFunc<?> mUDF;

	@BeforeClass
	public static void initDefault() throws FrontendException, InstantiationException, IllegalAccessException {
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

		mDefaultColumnAccessor = new DefaultColumnAccessor("_.bag_tuple.column_0", tInputFieldSchemaSensitive);
		mColumnAccessorString = "*0,120,0,*0,110,1,*0,10,1,";

		mUDFName = "org.apache.pig.builtin.IntMax";
		mUDF = (EvalFunc<?>) ReflectionUtil.getClassForName(mUDFName).newInstance();

		mInstanceDefaultString = mColumnAccessorString + "@" + mUDFName;
		mDefaultColumnEvaluatorInstance = new DefaultColumnEvaluator(mDefaultColumnAccessor, mUDF);
		mInstanceDefault = new StringableColumnEvaluator(mDefaultColumnEvaluatorInstance);
	}

	/**
	 * Test of String => StringableColumnAccessor(DefaultColumnEvaluator)
	 */
	@Test
	public void testStringToInstance_Default() throws InstantiationException, IllegalAccessException {
		TestUtil.assertEqualsPigObjects(
				mInstanceDefault,
				StringableColumnEvaluator.parse(mInstanceDefaultString));
	}

	/**
	 * Test of StringableColumnAccessor(DefaultColumnEvaluator) => String
	 */
	@Test
	public void testInstanceToString_Default() {
		TestUtil.assertEqualsPigObjects(
				mInstanceDefaultString,
				StringableColumnEvaluator.toString(mInstanceDefault));
		TestUtil.assertEqualsPigObjects(
				mInstanceDefaultString,
				StringableColumnEvaluator.toString(mDefaultColumnEvaluatorInstance));
	}

	// -----------------------------------------------------------------------------------------------------------------

}
