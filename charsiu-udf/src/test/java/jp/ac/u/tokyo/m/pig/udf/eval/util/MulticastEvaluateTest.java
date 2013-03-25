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
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.BeforeClass;
import org.junit.Test;

public class MulticastEvaluateTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static MulticastEvaluate mMulticastEvaluate;

	private static Schema INPUT_SCHEMA;

	private static Tuple INPUT_DATA;

	private static Tuple OUTPUT_DATA;

	// -----------------------------------------------------------------------------------------------------------------

	// {name_1st: chararray, score_1st: {score_tuple: (score: int)},name_2nd: chararray,score_2nd: {score_tuple: (score: int)}}
	// {name_1st: chararray, score_1st: {score_tuple: (score: int)},name_2st: chararray,score_2st: {score_tuple: (score: int)}}
	// [name_1st: chararray, score_1st: bag({score_tuple: (score: int)}), name_2st: chararray, score_2st: bag({score_tuple: (score: int)})]
	@BeforeClass
	public static void init() throws FrontendException {
		mMulticastEvaluate = new MulticastEvaluate("MAX", "score_.*", "_", "MIN", "score_.*", "_");

		INPUT_SCHEMA = TestUtil.createSchema(
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

		// dog {(3),(1),(7)} cat {(2),(1),(8)}
		INPUT_DATA = TestUtil.createTuple(
				"dog",
				TestUtil.createBag(1, 3, 1, 7),
				"cat",
				TestUtil.createBag(1, 2, 1, 8));

		OUTPUT_DATA = TestUtil.createTuple(
				"dog",
				7, 1,
				"cat",
				8, 1);
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testExec() throws Throwable {
		mMulticastEvaluate.outputSchema(INPUT_SCHEMA);
		TestUtil.assertEqualsPigObjects(OUTPUT_DATA, mMulticastEvaluate.exec(INPUT_DATA));
	}

	@Test
	public void testParseReflectionUDFParameters() throws Throwable {
		FieldSchema tInputFieldSchema = new FieldSchema("score_1st",
				TestUtil.createSchema(
						new FieldSchema("score_tuple",
								TestUtil.createSchema(
										new FieldSchema("score", DataType.INTEGER),
										new FieldSchema("rate", DataType.INTEGER)
										),
								DataType.TUPLE)),
				DataType.BAG);

		System.out.println(ReflectionUDFParameters.parseReflectionUDFParameters("_.score_tuple.score", tInputFieldSchema));

	}

	@Test
	public void testParseReflectionUDFParameters2() throws Throwable {
		FieldSchema tInputFieldSchema = new FieldSchema("score_1st",
				TestUtil.createSchema(
						new FieldSchema("score", DataType.INTEGER),
						new FieldSchema("rate", DataType.INTEGER)
						),
				DataType.BAG);

		System.out.println(ReflectionUDFParameters.parseReflectionUDFParameters("_.score", tInputFieldSchema));
	}

	@Test
	public void testParseReflectionUDFParameters3() throws Throwable {
		// dog {(3,1),(7,0)} cat {(2,1),(8,4)}
		Tuple tInputData = TestUtil.createTuple(
				"dog",
				TestUtil.createBag(2, 3, 1, 7, 0),
				"cat",
				TestUtil.createBag(2, 2, 1, 8, 4));

		Tuple tOutputData = TestUtil.createTuple(
				"dog",
				7, 3,
				"cat",
				8, 2, 5);

		Schema tInputSchema = TestUtil.createSchema(
				new FieldSchema("name_1st", DataType.CHARARRAY),
				new FieldSchema("score_1st",
						TestUtil.createSchema(
								new FieldSchema("score_tuple",
										TestUtil.createSchema(
												new FieldSchema("score", DataType.INTEGER),
												new FieldSchema("rate", DataType.INTEGER)
												),
										DataType.TUPLE)),
						DataType.BAG),
				new FieldSchema("name_2nd", DataType.CHARARRAY),
				new FieldSchema("score_2nd",
						TestUtil.createSchema(
								new FieldSchema("score_tuple",
										TestUtil.createSchema(
												new FieldSchema("score", DataType.INTEGER),
												new FieldSchema("rate", DataType.INTEGER)
												),
										DataType.TUPLE)),
						DataType.BAG)
				);

		MulticastEvaluate tMulticastEvaluate = new MulticastEvaluate("MAX", "score_.*", "_.score_tuple.score", "MIN", "score_.*", "_.score_tuple.score", "SUM", "score_2nd", "_.score_tuple.rate");
//		MulticastEvaluate tMulticastEvaluate = new MulticastEvaluate("TupleMax", "score_2nd", "_, _.score_tuple.rate");
		tMulticastEvaluate.outputSchema(tInputSchema);
		TestUtil.assertEqualsPigObjects(tOutputData, tMulticastEvaluate.exec(tInputData));
	}

	// @Test
	// public void testRemoveAlias() throws Throwable {
	// Schema tSchema = INPUT_SCHEMA.clone();
	// ReflectionUtil.removeAlias(tSchema);
	// System.out.println("**** " + tSchema);
	// ReflectionUtil.removeTupleInBag(tSchema);
	// System.out.println("**** " + tSchema);
	//
	// mMulticastEvaluate.outputSchema(INPUT_SCHEMA);
	// TestUtil.assertEqualsPigObjects(OUTPUT_DATA, mMulticastEvaluate.exec(INPUT_DATA));
	// }
	//
	// @Test
	// public void testRemoveTupleInBag() throws Throwable {
	// Schema tSchema = INPUT_SCHEMA.clone();
	// ReflectionUtil.removeAlias(tSchema);
	// System.out.println("**** " + tSchema);
	// ReflectionUtil.removeTupleInBag(tSchema);
	// System.out.println("**** " + tSchema);
	//
	// mMulticastEvaluate.outputSchema(INPUT_SCHEMA);
	// TestUtil.assertEqualsPigObjects(OUTPUT_DATA, mMulticastEvaluate.exec(INPUT_DATA));
	// }

	// -----------------------------------------------------------------------------------------------------------------

	// @Test(expected = RuntimeException.class)
	// public void testExecExceptionDigit1() throws Throwable {
	// mAddDaySpan.exec(TestUtil.createTuple("x", 0));
	// }
	//
	// @Test(expected = RuntimeException.class)
	// public void testExecExceptionDigit8() throws Throwable {
	// mAddDaySpan.exec(TestUtil.createTuple("yyyyMMdd", 0));
	// }
	//
	// @Test(expected = IllegalArgumentException.class)
	// public void testExecExceptionDigit7() throws Throwable {
	// mAddDaySpan.exec(TestUtil.createTuple("GyyMMdd", 0));
	// }

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testSchema() throws Throwable {
		// TestUtil.assertEqualsPigObjects(
		// TestUtil.createSchema(new FieldSchema("", DataType.CHARARRAY)),
		// mMulticastEvaluate.outputSchema(INPUT_SCHEMA));
	}

	// -----------------------------------------------------------------------------------------------------------------

}
