package jp.ac.u.tokyo.m.pig.udf.format;

import jp.ac.u.tokyo.m.pig.udf.FormatConstants;

public class StockGroupFilterFormatFactory
				extends AbstractInstanceStockHandler<String, GroupFilterFormat>
				implements GroupFilterFormatFactory {

	// -----------------------------------------------------------------------------------------------------------------

	public static final StockGroupFilterFormatFactory INSTANCE = new StockGroupFilterFormatFactory();

	private StockGroupFilterFormatFactory() {}

	// -----------------------------------------------------------------------------------------------------------------

	public GroupFilterFormat generateGroupFilterFormat(String aGroupFilterFormatString) {
		return generateGroupFilterFormat(
				aGroupFilterFormatString,
				FormatConstants.WORD_SEPARATOR,
				FormatConstants.SUB_GROUP_OPENER, FormatConstants.SUB_GROUP_CLOSER,
				FormatConstants.ADDITIONAL_PARAMETER_OPENER, FormatConstants.ADDITIONAL_PARAMETER_CLOSER);
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public GroupFilterFormat generateGroupFilterFormat(
			String aGroupFilterFormatString, String aWordSeparator,
			String aSubGroupOpener, String aSubGroupCloser,
			String aAdditionalParameterOpenaer,
			String aAdditionalParameterCloser) {
		GroupFilterFormat tGroupFilterFormat = this.get(aGroupFilterFormatString);
		if (tGroupFilterFormat == null) {
			tGroupFilterFormat =
					new DefaultGroupFilterFormat(
							aGroupFilterFormatString,
							aWordSeparator, aSubGroupOpener, aSubGroupCloser,
							aAdditionalParameterOpenaer, aAdditionalParameterCloser);
			this.put(aGroupFilterFormatString, tGroupFilterFormat);
		}
		return tGroupFilterFormat;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
