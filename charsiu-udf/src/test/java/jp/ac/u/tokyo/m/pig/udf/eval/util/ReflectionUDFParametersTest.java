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

public class ReflectionUDFParametersTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static FieldSchema mScoreFieldSchemaSensitive;
	private static FieldSchema mRateFieldSchemaSensitive;
	private static FieldSchema mScoreTupleFieldSchemaSensitive;
	private static FieldSchema mInputFieldSchemaSensitive;
	private static ArrayList<ColumnIndexInformation> mOutputColumnIndexInformationsScoreSensitive;
	private static ArrayList<ColumnIndexInformation> mOutputColumnIndexInformationsRateSensitive;

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

		DefaultColumnIndexInformation tScoreTupleColumnIndexInformationScore = new DefaultColumnIndexInformation(0, mScoreTupleFieldSchemaSensitive, AccessType.SUB_BAG);
		tScoreTupleColumnIndexInformationScore.setChild(new DefaultColumnIndexInformation(0, mScoreFieldSchemaSensitive, AccessType.SUB_BAG));
		DefaultColumnIndexInformation tParentColumnIndexInformationScore = new DefaultColumnIndexInformation(0, mInputFieldSchemaSensitive, AccessType.FLAT);
		tParentColumnIndexInformationScore.setChild(tScoreTupleColumnIndexInformationScore);
		mOutputColumnIndexInformationsScoreSensitive = new ArrayList<ColumnIndexInformation>();
		mOutputColumnIndexInformationsScoreSensitive.add(tParentColumnIndexInformationScore);

		DefaultColumnIndexInformation tScoreTupleColumnIndexInformationRate = new DefaultColumnIndexInformation(0, mScoreTupleFieldSchemaSensitive, AccessType.SUB_BAG);
		tScoreTupleColumnIndexInformationRate.setChild(new DefaultColumnIndexInformation(1, mRateFieldSchemaSensitive, AccessType.SUB_BAG));
		DefaultColumnIndexInformation tParentColumnIndexInformationRate = new DefaultColumnIndexInformation(0, mInputFieldSchemaSensitive, AccessType.FLAT);
		tParentColumnIndexInformationRate.setChild(tScoreTupleColumnIndexInformationRate);
		mOutputColumnIndexInformationsRateSensitive = new ArrayList<ColumnIndexInformation>();
		mOutputColumnIndexInformationsRateSensitive.add(tParentColumnIndexInformationRate);
	}

	@Test
	public void testParseReflectionUDFParameters_SensitiveSchema_SensitiveColumnControl() throws Throwable {
		TestUtil.assertEqualsPigObjects(mOutputColumnIndexInformationsScoreSensitive,
				ReflectionUDFParameters.parseReflectionUDFParameters("_.score_tuple.score", mInputFieldSchemaSensitive));
		TestUtil.assertEqualsPigObjects(mOutputColumnIndexInformationsRateSensitive,
				ReflectionUDFParameters.parseReflectionUDFParameters("_.score_tuple.rate", mInputFieldSchemaSensitive));
	}

	@Test
	public void testParseReflectionUDFParameters_SensitiveSchema_LooseColumnControl() throws Throwable {
		TestUtil.assertEqualsPigObjects(mOutputColumnIndexInformationsScoreSensitive,
				ReflectionUDFParameters.parseReflectionUDFParameters("_.score", mInputFieldSchemaSensitive));
		TestUtil.assertEqualsPigObjects(mOutputColumnIndexInformationsRateSensitive,
				ReflectionUDFParameters.parseReflectionUDFParameters("_.rate", mInputFieldSchemaSensitive));
	}

	// -----------------------------------------------------------------------------------------------------------------

	private static FieldSchema mScoreFieldSchemaLoose;
	private static FieldSchema mRateFieldSchemaLoose;
	private static FieldSchema mInputFieldSchemaLoose;
	private static ArrayList<ColumnIndexInformation> mOutputColumnIndexInformationsScoreLoose;
	private static ArrayList<ColumnIndexInformation> mOutputColumnIndexInformationsRateLoose;

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

		DefaultColumnIndexInformation tParentColumnIndexInformationScore = new DefaultColumnIndexInformation(0, mInputFieldSchemaLoose, AccessType.FLAT);
		tParentColumnIndexInformationScore.setChild(new DefaultColumnIndexInformation(0, mScoreFieldSchemaLoose, AccessType.SUB_BAG));
		mOutputColumnIndexInformationsScoreLoose = new ArrayList<ColumnIndexInformation>();
		mOutputColumnIndexInformationsScoreLoose.add(tParentColumnIndexInformationScore);

		DefaultColumnIndexInformation tParentColumnIndexInformationRate = new DefaultColumnIndexInformation(0, mInputFieldSchemaLoose, AccessType.FLAT);
		tParentColumnIndexInformationRate.setChild(new DefaultColumnIndexInformation(1, mRateFieldSchemaLoose, AccessType.SUB_BAG));
		mOutputColumnIndexInformationsRateLoose = new ArrayList<ColumnIndexInformation>();
		mOutputColumnIndexInformationsRateLoose.add(tParentColumnIndexInformationRate);
	}

	@Test
	public void testParseReflectionUDFParameters_LooseSchema_LooseColumnControl() throws Throwable {
		TestUtil.assertEqualsPigObjects(mOutputColumnIndexInformationsScoreLoose,
				ReflectionUDFParameters.parseReflectionUDFParameters("_.score", mInputFieldSchemaLoose));
		TestUtil.assertEqualsPigObjects(mOutputColumnIndexInformationsRateLoose,
				ReflectionUDFParameters.parseReflectionUDFParameters("_.rate", mInputFieldSchemaLoose));
	}

	// -----------------------------------------------------------------------------------------------------------------

}
