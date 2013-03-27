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

public class StringableColumnEvaluator implements ColumnEvaluator {

	private final ColumnEvaluator mColumnEvaluator;

	public StringableColumnEvaluator(ColumnEvaluator aColumnEvaluator) {
		mColumnEvaluator = aColumnEvaluator;
	}

	@Override
	public Object evaluate(Object aInput) throws IOException {
		return mColumnEvaluator.evaluate(aInput);
	}

	@Override
	public String toString() {
		return toString(this);
	}

	public static String toString(ColumnEvaluator aTarget) {
		if (aTarget instanceof StringableColumnEvaluator) {
			return toString(((StringableColumnEvaluator) aTarget).mColumnEvaluator);
		} else if (aTarget instanceof ThroughColumnEvaluator) {
			return "";
		} else if (aTarget instanceof DefaultColumnEvaluator) {
			DefaultColumnEvaluator tTarget = (DefaultColumnEvaluator) aTarget;
			StringBuilder tResultBuilder = new StringBuilder();
			tResultBuilder.append(StringableColumnAccessor.toString(tTarget.mColumnAccessor));
			tResultBuilder.append(MulticastEvaluationConstants.STRINGABLE_COLUMN_EVALUATOR_DELIMITER);
			tResultBuilder.append(tTarget.mUDF.getClass().getName());
			return tResultBuilder.toString();
		} else {
			throw new IllegalArgumentException("非対応の型が渡されました : " + aTarget);
		}
	}

	public static StringableColumnEvaluator parse(String aColumnColumnEvaluatorString) throws InstantiationException, IllegalAccessException {
		if (aColumnColumnEvaluatorString == null || aColumnColumnEvaluatorString.length() == 0) {
			return new StringableColumnEvaluator(ThroughColumnEvaluator.INSTANCE);
		} else {
			String[] tElements = aColumnColumnEvaluatorString.split(MulticastEvaluationConstants.STRINGABLE_COLUMN_EVALUATOR_DELIMITER);
			return new StringableColumnEvaluator(
					new DefaultColumnEvaluator(
							StringableColumnAccessor.parse(tElements[0]),
							(EvalFunc<?>) ReflectionUtil.getClassForName(tElements[1]).newInstance()));
		}
	}

}
