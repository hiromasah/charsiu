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

import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class DefaultColumnAccessor implements ColumnAccessor {

	private ReflectionUDFParameters mReflectionUDFParameters;

	public DefaultColumnAccessor(String aReflectionUDFParametersString, FieldSchema aColumnValueFieldSchema) throws FrontendException {
		mReflectionUDFParameters = new ReflectionUDFParameters(aReflectionUDFParametersString, aColumnValueFieldSchema);
	}

	@Deprecated
	@Override
	public Tuple generate(Object aColumnValue) {
		return null;
	}

	@Override
	public Schema getInputSchema() {
		return mReflectionUDFParameters.getInputSchema();
	}

	@Override
	public List<ColumnIndexInformation> getColumnIndexInformations() {
		return mReflectionUDFParameters.getColumnIndexInformations();
	}

}
