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

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class DefaultColumnEvaluator implements ColumnEvaluator {

	private final ReflectionUDFSetting mReflectUDFSetting;
	final ColumnAccessor mColumnAccessor;
	final EvalFunc<?> mUDF;

	public DefaultColumnEvaluator(ColumnAccessor aColumnAccessor, ReflectionUDFSetting aReflectUDFSetting) throws InstantiationException, IllegalAccessException, FrontendException, CloneNotSupportedException {
		mColumnAccessor = aColumnAccessor;
		mReflectUDFSetting = aReflectUDFSetting;
		mUDF = ReflectionUtil.getUDFInstance(aReflectUDFSetting.getClassName(), aColumnAccessor.getInputSchema());
	}

	DefaultColumnEvaluator(ColumnAccessor aColumnAccessor, EvalFunc<?> aUDF) {
		mColumnAccessor = aColumnAccessor;
		mReflectUDFSetting = null;
		mUDF = aUDF;
	}

	public ReflectionUDFSetting getReflectUDFSetting() {
		return mReflectUDFSetting;
	}

	@Override
	public Object evaluate(Object aInput) throws IOException {
		return mUDF.exec(mColumnAccessor.generate(aInput));
	}

	public Schema getOutputSchema() {
		return mUDF.outputSchema(mColumnAccessor.getInputSchema());
	}

}
