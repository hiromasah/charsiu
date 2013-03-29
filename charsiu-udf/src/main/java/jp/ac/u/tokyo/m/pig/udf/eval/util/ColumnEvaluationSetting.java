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
import java.util.List;

/**
 * ColumnEvaluationSetting expresses the editing information for 1 column of the input. <br>
 * <br>
 * 入力の1カラムに対する編集情報を表現します。 <br>
 */
public class ColumnEvaluationSetting {

	// -----------------------------------------------------------------------------------------------------------------

	private final List<ColumnEvaluator> mColumnEvaluators;

	// -----------------------------------------------------------------------------------------------------------------

	public ColumnEvaluationSetting() {
		mColumnEvaluators = new ArrayList<ColumnEvaluator>();
	}

	public ColumnEvaluationSetting(List<ColumnEvaluator> aColumnEvaluators) {
		mColumnEvaluators = aColumnEvaluators;
	}

	// -----------------------------------------------------------------------------------------------------------------

	public List<ColumnEvaluator> getColumnEvaluators() {
		return mColumnEvaluators;
	}

	public void addColumnEvaluator(ColumnEvaluator aTarget) {
		mColumnEvaluators.add(aTarget);
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return StringableUtil.toStringColumnEvaluators(mColumnEvaluators);
	}

	// -----------------------------------------------------------------------------------------------------------------

}
