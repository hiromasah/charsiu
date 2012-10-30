/*
 * Copyright 2012 Hiromasa Horiguchi ( The University of Tokyo )
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

package jp.ac.u.tokyo.m.pig.udf.format;

import jp.ac.u.tokyo.m.pig.udf.format.AdditionalParameterDefinitionFormat;
import jp.ac.u.tokyo.m.pig.udf.format.DefaultAdditionalParameterDefinitionFormat;
import jp.ac.u.tokyo.m.pig.udf.format.AdditionalParameterDefinitionFormat.AdditionalParameterCaster;
import junit.framework.Assert;

import org.apache.pig.data.DataType;
import org.junit.Test;

public class AdditionalParameterDefinitionFormatTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static final String ADDITIONAL_PARAMETER_DEFINITION_FORMAT_STRING =
			"num : int, location : chararray";

	// -----------------------------------------------------------------------------------------------------------------

	private void assertEqualsAPDF(String aAdditionalParameterDefinitionFormatString, String[] aExpectedAliases, byte[] aExpectedTypes) {
		String tAdditionalParameterSeparator = ":";
		String tWordSeparator = ",";
		AdditionalParameterDefinitionFormat tAPDF = new DefaultAdditionalParameterDefinitionFormat(aAdditionalParameterDefinitionFormatString, tWordSeparator, tAdditionalParameterSeparator);

		int tIndex = 0;
		for (String tAlias : tAPDF.getAliases()) {
			Assert.assertEquals(aExpectedAliases[tIndex++], tAlias);
		}
		tIndex = 0;
		for (AdditionalParameterCaster tAdditionalParameterCaster : tAPDF.getAdditionalParameterCasters()) {
			Assert.assertEquals(aExpectedTypes[tIndex++], tAdditionalParameterCaster.getType());
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testDefaultAdditionalParameterDefinitionFormatTest() {
		String[] tExpectedAliases = new String[] { "num", "location" };
		byte[] tExpectedTypes = new byte[] { DataType.INTEGER, DataType.CHARARRAY };

		assertEqualsAPDF(ADDITIONAL_PARAMETER_DEFINITION_FORMAT_STRING, tExpectedAliases, tExpectedTypes);
	}

	// -----------------------------------------------------------------------------------------------------------------

}
