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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StringableUtil {

	// -----------------------------------------------------------------------------------------------------------------

	public static String toStringColumnEvaluators(List<ColumnEvaluator> aColumnEvaluators) {
		String tDelimiter = MulticastEvaluationConstants.STRINGABLE_LIST_COLUMN_EVALUATOR_DELIMITER;
		StringBuilder tResultBuilder = new StringBuilder();
		Iterator<ColumnEvaluator> tTargetIterator = aColumnEvaluators.iterator();
		if (tTargetIterator.hasNext())
			tResultBuilder.append(StringableColumnEvaluator.toString(tTargetIterator.next()));
		while (tTargetIterator.hasNext()) {
			tResultBuilder.append(tDelimiter);
			tResultBuilder.append(StringableColumnEvaluator.toString(tTargetIterator.next()));
		}
		return tResultBuilder.toString();
	}

	public static List<ColumnEvaluator> parseColumnEvaluatorsString(String aColumnEvaluatorsString) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		String[] tElements = aColumnEvaluatorsString.split(MulticastEvaluationConstants.STRINGABLE_LIST_COLUMN_EVALUATOR_DELIMITER);
		ArrayList<ColumnEvaluator> tResult = new ArrayList<ColumnEvaluator>();
		for (String tCurrentElement : tElements) {
			tResult.add(StringableColumnEvaluator.parse(tCurrentElement));
		}
		return tResult;
	}

	// -----------------------------------------------------------------------------------------------------------------

	public static String toStringColumnEvaluationSettings(List<ColumnEvaluationSetting> aColumnEvaluationSettings) {
		String tDelimiter = MulticastEvaluationConstants.STRINGABLE_LIST_COLUMN_EVALUATION_SETTING_DELIMITER;
		StringBuilder tResultBuilder = new StringBuilder();
		Iterator<ColumnEvaluationSetting> tTargetIterator = aColumnEvaluationSettings.iterator();
		if (tTargetIterator.hasNext())
			tResultBuilder.append(toStringColumnEvaluators(tTargetIterator.next().getColumnEvaluators()));
		while (tTargetIterator.hasNext()) {
			tResultBuilder.append(tDelimiter);
			tResultBuilder.append(toStringColumnEvaluators(tTargetIterator.next().getColumnEvaluators()));
		}
		return tResultBuilder.toString();
	}

	public static List<ColumnEvaluationSetting> parseColumnEvaluationSettingsString(String aColumnEvaluationSettingsString) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		String[] tElements = aColumnEvaluationSettingsString.split(MulticastEvaluationConstants.STRINGABLE_LIST_COLUMN_EVALUATION_SETTING_DELIMITER);
		ArrayList<ColumnEvaluationSetting> tResult = new ArrayList<ColumnEvaluationSetting>();
		for (String tCurrentElement : tElements) {
			tResult.add(new ColumnEvaluationSetting(parseColumnEvaluatorsString(tCurrentElement)));
		}
		return tResult;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
