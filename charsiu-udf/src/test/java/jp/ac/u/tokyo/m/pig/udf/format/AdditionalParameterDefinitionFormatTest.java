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
