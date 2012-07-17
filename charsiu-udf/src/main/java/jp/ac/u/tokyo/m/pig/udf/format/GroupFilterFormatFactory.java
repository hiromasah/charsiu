package jp.ac.u.tokyo.m.pig.udf.format;

public interface GroupFilterFormatFactory {
	GroupFilterFormat generateGroupFilterFormat(
			String aGroupFilterFormatString,
			String aWordSeparator, String aSubGroupOpener, String aSubGroupCloser,
			String aAdditionalParameterOpenaer, String aAdditionalParameterCloser);

}
