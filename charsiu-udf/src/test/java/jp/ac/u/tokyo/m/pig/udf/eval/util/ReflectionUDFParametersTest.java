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

import jp.ac.u.tokyo.m.pig.udf.eval.util.ReflectionUDFParameters.AccessType;
import jp.ac.u.tokyo.m.pig.udf.eval.util.ReflectionUDFParameters.ColumnName;
import jp.ac.u.tokyo.m.test.TestUtil;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.BeforeClass;
import org.junit.Test;

public class ReflectionUDFParametersTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static FieldSchema mScoreFieldSchemaSensitive;
	private static FieldSchema mRateFieldSchemaSensitive;
	private static FieldSchema mScoreTupleFieldSchemaSensitive;
	private static FieldSchema mInputFieldSchemaSensitive;
	private static ArrayList<ColumnName> mOutputColumnNamesScoreSensitive;
	private static ArrayList<ColumnName> mOutputColumnNamesRateSensitive;

	@BeforeClass
	public static void initSensitive() throws FrontendException {
		mScoreFieldSchemaSensitive = new FieldSchema("score", DataType.INTEGER);
		mRateFieldSchemaSensitive = new FieldSchema("rate", DataType.INTEGER);
		mScoreTupleFieldSchemaSensitive = new FieldSchema("score_tuple",
				TestUtil.createSchema(
						mScoreFieldSchemaSensitive,
						mRateFieldSchemaSensitive
						),
				DataType.TUPLE);
		mInputFieldSchemaSensitive = new FieldSchema("score_1st",
				TestUtil.createSchema(mScoreTupleFieldSchemaSensitive),
				DataType.BAG);

		ColumnName tScoreTupleColumnNameScore = new ColumnName("score_tuple", 0, mScoreTupleFieldSchemaSensitive, AccessType.SUB_BAG);
		tScoreTupleColumnNameScore.setChild(new ColumnName("score", 0, mScoreFieldSchemaSensitive, AccessType.SUB_BAG));
		ColumnName tParentColumnNameScore = new ColumnName("", 0, mInputFieldSchemaSensitive, AccessType.FLAT);
		tParentColumnNameScore.setChild(tScoreTupleColumnNameScore);
		mOutputColumnNamesScoreSensitive = new ArrayList<ColumnName>();
		mOutputColumnNamesScoreSensitive.add(tParentColumnNameScore);

		ColumnName tScoreTupleColumnNameRate = new ColumnName("score_tuple", 0, mScoreTupleFieldSchemaSensitive, AccessType.SUB_BAG);
		tScoreTupleColumnNameRate.setChild(new ColumnName("rate", 1, mRateFieldSchemaSensitive, AccessType.SUB_BAG));
		ColumnName tParentColumnNameRate = new ColumnName("", 0, mInputFieldSchemaSensitive, AccessType.FLAT);
		tParentColumnNameRate.setChild(tScoreTupleColumnNameRate);
		mOutputColumnNamesRateSensitive = new ArrayList<ColumnName>();
		mOutputColumnNamesRateSensitive.add(tParentColumnNameRate);
	}

	@Test
	public void testParseReflectionUDFParameters_SensitiveSchema_SensitiveColumnControl() throws Throwable {
		TestUtil.assertEqualsPigObjects(mOutputColumnNamesScoreSensitive,
				ReflectionUDFParameters.parseReflectionUDFParameters("_.score_tuple.score", mInputFieldSchemaSensitive));
		TestUtil.assertEqualsPigObjects(mOutputColumnNamesRateSensitive,
				ReflectionUDFParameters.parseReflectionUDFParameters("_.score_tuple.rate", mInputFieldSchemaSensitive));
	}

	@Test
	public void testParseReflectionUDFParameters_SensitiveSchema_LooseColumnControl() throws Throwable {
		TestUtil.assertEqualsPigObjects(mOutputColumnNamesScoreSensitive,
				ReflectionUDFParameters.parseReflectionUDFParameters("_.score", mInputFieldSchemaSensitive));
		TestUtil.assertEqualsPigObjects(mOutputColumnNamesRateSensitive,
				ReflectionUDFParameters.parseReflectionUDFParameters("_.rate", mInputFieldSchemaSensitive));
	}

	// -----------------------------------------------------------------------------------------------------------------

	private static FieldSchema mScoreFieldSchemaLoose;
	private static FieldSchema mRateFieldSchemaLoose;
	private static FieldSchema mInputFieldSchemaLoose;
	private static ArrayList<ColumnName> mOutputColumnNamesScoreLoose;
	private static ArrayList<ColumnName> mOutputColumnNamesRateLoose;

	@BeforeClass
	public static void initLoose() throws FrontendException {
		mScoreFieldSchemaLoose = new FieldSchema("score", DataType.INTEGER);
		mRateFieldSchemaLoose = new FieldSchema("rate", DataType.INTEGER);
		mInputFieldSchemaLoose = new FieldSchema("score_1st",
				TestUtil.createSchema(
						mScoreFieldSchemaLoose,
						mRateFieldSchemaLoose
						),
				DataType.BAG);

		ColumnName tParentColumnNameScore = new ColumnName("", 0, mInputFieldSchemaLoose, AccessType.FLAT);
		tParentColumnNameScore.setChild(new ColumnName("score", 0, mScoreFieldSchemaLoose, AccessType.SUB_BAG));
		mOutputColumnNamesScoreLoose = new ArrayList<ColumnName>();
		mOutputColumnNamesScoreLoose.add(tParentColumnNameScore);

		ColumnName tParentColumnNameRate = new ColumnName("", 0, mInputFieldSchemaLoose, AccessType.FLAT);
		tParentColumnNameRate.setChild(new ColumnName("rate", 1, mRateFieldSchemaLoose, AccessType.SUB_BAG));
		mOutputColumnNamesRateLoose = new ArrayList<ColumnName>();
		mOutputColumnNamesRateLoose.add(tParentColumnNameRate);
	}

	@Test
	public void testParseReflectionUDFParameters_LooseSchema_LooseColumnControl() throws Throwable {
		TestUtil.assertEqualsPigObjects(mOutputColumnNamesScoreLoose,
				ReflectionUDFParameters.parseReflectionUDFParameters("_.score", mInputFieldSchemaLoose));
		TestUtil.assertEqualsPigObjects(mOutputColumnNamesRateLoose,
				ReflectionUDFParameters.parseReflectionUDFParameters("_.rate", mInputFieldSchemaLoose));
	}

	// -----------------------------------------------------------------------------------------------------------------

}
