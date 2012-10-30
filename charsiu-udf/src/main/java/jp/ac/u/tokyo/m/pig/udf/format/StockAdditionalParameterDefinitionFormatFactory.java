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

public class StockAdditionalParameterDefinitionFormatFactory
		extends AbstractInstanceStockHandler<String, AdditionalParameterDefinitionFormat>
		implements AdditionalParameterDefinitionFormatFactory {

	// -----------------------------------------------------------------------------------------------------------------

	public static final StockAdditionalParameterDefinitionFormatFactory INSTANCE = new StockAdditionalParameterDefinitionFormatFactory();

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
