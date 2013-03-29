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

import jp.ac.u.tokyo.m.pig.udf.AliasConstants;
import jp.ac.u.tokyo.m.test.TestUtil;

import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.BeforeClass;
import org.junit.Test;

public class MulticastEvaluateTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static MulticastEvaluate mMulticastEvaluateSimple;
	private static Schema mInputSchemaSimple;
	private static Schema mOutputSchemaSimple;
	private static Tuple mInputDataSimple;
	private static Tuple mOutputDataSimple;

	// -----------------------------------------------------------------------------------------------------------------

	@BeforeClass
	public static void initSimple() throws FrontendException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		mMulticastEvaluateSimple = new MulticastEvaluate(
				"MAX", "score_.*", "_", "MAX_result",
				"MIN", "score_.*", "_", "MIN_result");
		// {name_1st: chararray, score_1st: {score_tuple: (score: int)},name_2nd: chararray,score_2nd: {score_tuple: (score: int)}}
		mInputSchemaSimple = TestUtil.createSchema(
				new FieldSchema("name_1st", DataType.CHARARRAY),
				new FieldSchema("score_1st",
						TestUtil.createSchema(
								new FieldSchema("score_tuple",
										TestUtil.createSchema(
												new FieldSchema("score", DataType.INTEGER)
												),
										DataType.TUPLE)),
						DataType.BAG),
				new FieldSchema("name_2nd", DataType.CHARARRAY),
				new FieldSchema("score_2nd",
						TestUtil.createSchema(
								new FieldSchema("score_tuple",
										TestUtil.createSchema(
												new FieldSchema("score", DataType.INTEGER)
												),
										DataType.TUPLE)),
						DataType.BAG)
				);
		mOutputSchemaSimple = TestUtil.createSchema(
				new FieldSchema(AliasConstants.MULTICAST_EVALUATE_ALIAS_TOP,
						TestUtil.createSchema(
								new FieldSchema("name_1st", DataType.CHARARRAY),
								new FieldSchema("score_1st_MAX_result", DataType.INTEGER),
								new FieldSchema("score_1st_MIN_result", DataType.INTEGER),
								new FieldSchema("name_2nd", DataType.CHARARRAY),
								new FieldSchema("score_2nd_MAX_result", DataType.INTEGER),
								new FieldSchema("score_2nd_MIN_result", DataType.INTEGER)
								),
						DataType.TUPLE));
		// dog {(4),(1),(7)} cat {(2),(5),(8)}
		mInputDataSimple = TestUtil.createTuple(
				"dog",
				TestUtil.createBag(1, 4, 1, 7),
				"cat",
				TestUtil.createBag(1, 2, 5, 8));
		// dog 7 1 cat 8 2
		mOutputDataSimple = TestUtil.createTuple(
				"dog",
				7, 1,
				"cat",
				8, 2);
	}

	@Test
	public void testSchemaSimple() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				mOutputSchemaSimple,
				mMulticastEvaluateSimple.outputSchema(mInputSchemaSimple));
	}

	@Test
	public void testExecSimple() throws Throwable {
		mMulticastEvaluateSimple.outputSchema(mInputSchemaSimple);
		mMulticastEvaluateSimple = new MulticastEvaluate(
				"MAX", "score_.*", "_", "MAX_result",
				"MIN", "score_.*", "_", "MIN_result");
		TestUtil.assertEqualsPigObjects(mOutputDataSimple, mMulticastEvaluateSimple.exec(mInputDataSimple));
	}

	// -----------------------------------------------------------------------------------------------------------------

	private static MulticastEvaluate mMulticastEvaluateAll;
	private static Schema mOutputSchemaAll;
	private static Tuple mOutputDataAll;

	@BeforeClass
	public static void initAll() throws FrontendException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		mMulticastEvaluateAll = new MulticastEvaluate(
				"MIN", "score_.*", "_", "MIN_result",
				"MAX", "score_.*", "_", "MAX_result",
				"SUM", "score_.*", "_", "SUM_result",
				"AVG", "score_.*", "_", "AVG_result",
				"SIZE", "score_.*", "_", "SIZE_result",
				"COUNT", "score_.*", "_", "COUNT_result");
		mOutputSchemaAll = TestUtil.createSchema(
				new FieldSchema(AliasConstants.MULTICAST_EVALUATE_ALIAS_TOP,
						TestUtil.createSchema(
								new FieldSchema("name_1st", DataType.CHARARRAY),
								new FieldSchema("score_1st_MIN_result", DataType.INTEGER),
								new FieldSchema("score_1st_MAX_result", DataType.INTEGER),
								new FieldSchema("score_1st_SUM_result", DataType.LONG),
								new FieldSchema("score_1st_AVG_result", DataType.DOUBLE),
								new FieldSchema("score_1st_SIZE_result", DataType.LONG),
								new FieldSchema("score_1st_COUNT_result", DataType.LONG),
								new FieldSchema("name_2nd", DataType.CHARARRAY),
								new FieldSchema("score_2nd_MIN_result", DataType.INTEGER),
								new FieldSchema("score_2nd_MAX_result", DataType.INTEGER),
								new FieldSchema("score_2nd_SUM_result", DataType.LONG),
								new FieldSchema("score_2nd_AVG_result", DataType.DOUBLE),
								new FieldSchema("score_2nd_SIZE_result", DataType.LONG),
								new FieldSchema("score_2nd_COUNT_result", DataType.LONG)
								),
						DataType.TUPLE));
		mOutputDataAll = TestUtil.createTuple(
				"dog",
				1, 7, 12, 4.0, 3, 3,
				"cat",
				2, 8, 15, 5.0, 3, 3);
	}

	@Test
	public void testSchemaAll() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				mOutputSchemaAll,
				mMulticastEvaluateAll.outputSchema(mInputSchemaSimple));
	}

	@Test
	public void testExecAll() throws Throwable {
		mMulticastEvaluateAll.outputSchema(mInputSchemaSimple);
		mMulticastEvaluateAll = new MulticastEvaluate(
				"MIN", "score_.*", "_", "MIN_result",
				"MAX", "score_.*", "_", "MAX_result",
				"SUM", "score_.*", "_", "SUM_result",
				"AVG", "score_.*", "_", "AVG_result",
				"SIZE", "score_.*", "_", "SIZE_result",
				"COUNT", "score_.*", "_", "COUNT_result");
		TestUtil.assertEqualsPigObjects(mOutputDataAll, mMulticastEvaluateAll.exec(mInputDataSimple));
	}

	// -----------------------------------------------------------------------------------------------------------------

	private static MulticastEvaluate mMulticastEvaluateSensitive;
	private static Schema mInputSchemaSensitive;
	private static Schema mOutputSchemaSensitive;
	private static Tuple mInputDataSensitive;
	private static Tuple mOutputDataSensitive;

	@BeforeClass
	public static void initSensitive() throws FrontendException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		mMulticastEvaluateSensitive = new MulticastEvaluate(
				"MAX", "score_.*", "_.score_tuple.score", "MAX_result",
				"MIN", "score_.*", "_.score_tuple.score", "MIN_result");

		// {name_1st: chararray, score_1st: {score_tuple: (score: int)},name_2nd: chararray,score_2nd: {score_tuple: (score: int)}}
		mInputSchemaSensitive = TestUtil.createSchema(
				new FieldSchema("name_1st", DataType.CHARARRAY),
				new FieldSchema("score_1st",
						TestUtil.createSchema(
								new FieldSchema("score_tuple",
										TestUtil.createSchema(
												new FieldSchema("score", DataType.INTEGER)
												),
										DataType.TUPLE)),
						DataType.BAG),
				new FieldSchema("name_2nd", DataType.CHARARRAY),
				new FieldSchema("score_2nd",
						TestUtil.createSchema(
								new FieldSchema("score_tuple",
										TestUtil.createSchema(
												new FieldSchema("score", DataType.INTEGER)
												),
										DataType.TUPLE)),
						DataType.BAG)
				);
		mOutputSchemaSensitive = TestUtil.createSchema(
				new FieldSchema(AliasConstants.MULTICAST_EVALUATE_ALIAS_TOP,
						TestUtil.createSchema(
								new FieldSchema("name_1st", DataType.CHARARRAY),
								new FieldSchema("score_1st_MAX_result", DataType.INTEGER),
								new FieldSchema("score_1st_MIN_result", DataType.INTEGER),
								new FieldSchema("name_2nd", DataType.CHARARRAY),
								new FieldSchema("score_2nd_MAX_result", DataType.INTEGER),
								new FieldSchema("score_2nd_MIN_result", DataType.INTEGER)
								),
						DataType.TUPLE));

		// dog {(3),(1),(7)} cat {(2),(1),(8)}
		mInputDataSensitive = TestUtil.createTuple(
				"dog",
				TestUtil.createBag(1, 3, 1, 7),
				"cat",
				TestUtil.createBag(1, 2, 1, 8));

		mOutputDataSensitive = TestUtil.createTuple(
				"dog",
				7, 1,
				"cat",
				8, 1);
	}

	@Test
	public void testSchemaSensitive() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				mOutputSchemaSensitive,
				mMulticastEvaluateSensitive.outputSchema(mInputSchemaSensitive));
	}

	@Test
	public void testExecSensitive() throws Throwable {
		mMulticastEvaluateSensitive.outputSchema(mInputSchemaSensitive);
		mMulticastEvaluateSensitive = new MulticastEvaluate(
				"MAX", "score_.*", "_.score_tuple.score", "MAX_result",
				"MIN", "score_.*", "_.score_tuple.score", "MIN_result");
		TestUtil.assertEqualsPigObjects(mOutputDataSensitive, mMulticastEvaluateSensitive.exec(mInputDataSensitive));

	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testFQCNUDFName() throws Throwable {
		new MulticastEvaluate(
				"org.apache.pig.builtin.DIFF", "score_.*", "_.score_tuple.score", "result");
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test(expected = IllegalArgumentException.class)
	public void testExceptionUnknownUDF() throws Throwable {
		new MulticastEvaluate(
				"UnknownUDF", "score_.*", "_.score_tuple.score", "result");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testExceptionNotUDF() throws Throwable {
		new MulticastEvaluate(
				"java.lang.String", "score_.*", "_.score_tuple.score", "result");
	}

	// -----------------------------------------------------------------------------------------------------------------

}
