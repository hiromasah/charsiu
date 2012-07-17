package jp.ac.u.tokyo.m.pig.udf.format;

public class StockAdditionalParameterDefinitionFormatFactory
				extends AbstractInstanceStockHandler<String, AdditionalParameterDefinitionFormat>
				implements AdditionalParameterDefinitionFormatFactory {

	// -----------------------------------------------------------------------------------------------------------------

	public static StockAdditionalParameterDefinitionFormatFactory INSTANCE = new StockAdditionalParameterDefinitionFormatFactory();

	private StockAdditionalParameterDefinitionFormatFactory() {}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public synchronized AdditionalParameterDefinitionFormat generate(
			String aAdditionalParameterDefinitionFormatString,
			String aWordSeparator,
			String aAdditionalParameterSeparator) {
		AdditionalParameterDefinitionFormat tAdditionalParameterDefinitionFormat =
				this.get(aAdditionalParameterDefinitionFormatString);
		if (tAdditionalParameterDefinitionFormat == null) {
			tAdditionalParameterDefinitionFormat =
					new DefaultAdditionalParameterDefinitionFormat(
							aAdditionalParameterDefinitionFormatString,
							aWordSeparator, aAdditionalParameterSeparator);
			this.put(aAdditionalParameterDefinitionFormatString,
					tAdditionalParameterDefinitionFormat);
		}
		return tAdditionalParameterDefinitionFormat;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
