package jp.ac.u.tokyo.m.pig.udf.format;

import java.util.List;

public interface AdditionalParameterDefinitionFormat {
	List<String> getAliases();

	List<AdditionalParameterCaster> getAdditionalParameterCasters();

	AdditionalParameterCaster getDefaultAdditionalParameterCaster();

	public interface AdditionalParameterCaster {
		byte getType();

		Object castParameter(String aParameterString);
	}
}
