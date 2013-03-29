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

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.BeforeClass;
import org.junit.Test;

public class ReflectionUtilTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static Schema mIntBagSchema;
	private static Schema mLongBagSchema;
	private static Schema mFloatBagSchema;
	private static Schema mDoubleBagSchema;
	private static Schema mChararrayBagSchema;
	private static Schema mBytearrayBagSchema;

	@BeforeClass
	public static void initSensitive() throws FrontendException {
		mIntBagSchema = TestUtil.createSchema(
				new FieldSchema("score_bag",
						TestUtil.createSchema(
								new FieldSchema("score", DataType.INTEGER)),
						DataType.BAG));
		mLongBagSchema = TestUtil.createSchema(
				new FieldSchema("score_bag",
						TestUtil.createSchema(
								new FieldSchema("score", DataType.LONG)),
						DataType.BAG));
		mFloatBagSchema = TestUtil.createSchema(
				new FieldSchema("score_bag",
						TestUtil.createSchema(
								new FieldSchema("score", DataType.FLOAT)),
						DataType.BAG));
		mDoubleBagSchema = TestUtil.createSchema(
				new FieldSchema("score_bag",
						TestUtil.createSchema(
								new FieldSchema("score", DataType.DOUBLE)),
						DataType.BAG));
		mChararrayBagSchema = TestUtil.createSchema(
				new FieldSchema("score_bag",
						TestUtil.createSchema(
								new FieldSchema("score", DataType.CHARARRAY)),
						DataType.BAG));
		mBytearrayBagSchema = TestUtil.createSchema(
				new FieldSchema("score_bag",
						TestUtil.createSchema(
								new FieldSchema("score", DataType.BYTEARRAY)),
						DataType.BAG));
	}

	@Test
	public void testGetUDFInstance_MAX() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				org.apache.pig.builtin.IntMax.class.getName(),
				ReflectionUtil.getUDFInstance("org.apache.pig.builtin.MAX", mIntBagSchema).getClass().getName());
		TestUtil.assertEqualsPigObjects(
				org.apache.pig.builtin.LongMax.class.getName(),
				ReflectionUtil.getUDFInstance("org.apache.pig.builtin.MAX", mLongBagSchema).getClass().getName());
		TestUtil.assertEqualsPigObjects(
				org.apache.pig.builtin.FloatMax.class.getName(),
				ReflectionUtil.getUDFInstance("org.apache.pig.builtin.MAX", mFloatBagSchema).getClass().getName());
		TestUtil.assertEqualsPigObjects(
				org.apache.pig.builtin.DoubleMax.class.getName(),
				ReflectionUtil.getUDFInstance("org.apache.pig.builtin.MAX", mDoubleBagSchema).getClass().getName());
		TestUtil.assertEqualsPigObjects(
				org.apache.pig.builtin.StringMax.class.getName(),
				ReflectionUtil.getUDFInstance("org.apache.pig.builtin.MAX", mChararrayBagSchema).getClass().getName());
		TestUtil.assertEqualsPigObjects(
				org.apache.pig.builtin.MAX.class.getName(),
				ReflectionUtil.getUDFInstance("org.apache.pig.builtin.MAX", mBytearrayBagSchema).getClass().getName());
	}

	@Test
	public void testGetUDFInstance_COUNT() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				org.apache.pig.builtin.COUNT.class.getName(),
				ReflectionUtil.getUDFInstance("org.apache.pig.builtin.COUNT", mIntBagSchema).getClass().getName());
		TestUtil.assertEqualsPigObjects(
				org.apache.pig.builtin.COUNT.class.getName(),
				ReflectionUtil.getUDFInstance("org.apache.pig.builtin.COUNT", mLongBagSchema).getClass().getName());
		TestUtil.assertEqualsPigObjects(
				org.apache.pig.builtin.COUNT.class.getName(),
				ReflectionUtil.getUDFInstance("org.apache.pig.builtin.COUNT", mFloatBagSchema).getClass().getName());
		TestUtil.assertEqualsPigObjects(
				org.apache.pig.builtin.COUNT.class.getName(),
				ReflectionUtil.getUDFInstance("org.apache.pig.builtin.COUNT", mDoubleBagSchema).getClass().getName());
		TestUtil.assertEqualsPigObjects(
				org.apache.pig.builtin.COUNT.class.getName(),
				ReflectionUtil.getUDFInstance("org.apache.pig.builtin.COUNT", mChararrayBagSchema).getClass().getName());
		TestUtil.assertEqualsPigObjects(
				org.apache.pig.builtin.COUNT.class.getName(),
				ReflectionUtil.getUDFInstance("org.apache.pig.builtin.COUNT", mBytearrayBagSchema).getClass().getName());
	}

	// -----------------------------------------------------------------------------------------------------------------

}
