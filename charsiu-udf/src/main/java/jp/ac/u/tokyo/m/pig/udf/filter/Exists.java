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

package jp.ac.u.tokyo.m.pig.udf.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import jp.ac.u.tokyo.m.pig.udf.FormatConstants;
import jp.ac.u.tokyo.m.pig.udf.format.GroupFilterFormat;
import jp.ac.u.tokyo.m.pig.udf.format.StockGroupFilterFormatFactory;
import jp.ac.u.tokyo.m.string.StringUtil;

import org.apache.pig.FilterFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Exists filters only an element included in GroupFilterFormat. <br>
 * <br>
 * GroupFilterFormat に含まれる要素だけフィルタします。<br>
 */
public class Exists extends FilterFunc {

	// -----------------------------------------------------------------------------------------------------------------

	private List<String> mExclusionWords = null;

	private GroupFilterFormat mGroupFilterFormat = null;

	// -----------------------------------------------------------------------------------------------------------------

	public Exists(String aGroupFilterFormatString) {
		mGroupFilterFormat = StockGroupFilterFormatFactory.INSTANCE.generateGroupFilterFormat(aGroupFilterFormatString);
	}

	public Exists(String aGroupFilterFormatString, String aExclusionWords) {
		this(aGroupFilterFormatString);
		List<String> tExclusionWords = mExclusionWords = new ArrayList<String>();
		for (String tCurrentExclusionWords : aExclusionWords.split(FormatConstants.WORD_SEPARATOR)) {
			tCurrentExclusionWords = StringUtil.trimMultiByteCharacter(tCurrentExclusionWords);
			if (tCurrentExclusionWords.length() > 0)
				tExclusionWords.add(tCurrentExclusionWords);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Boolean exec(Tuple aInput) throws IOException {
		if (aInput == null)
			return false;

		// processing target | 処理対象
		String tTarget = DataType.toString(aInput.get(0));
		if (tTarget == null)
			return false;

		// list for effective judgments | 有効判定用
		List<String> tExpectWordList = mGroupFilterFormat.getFilterList();

		// effective word judgment | 有効ワード判定
		if (!StringUtil.startsWithWord(tTarget, tExpectWordList))
			return false;

		// invalid word judgment | 無効ワード判定
		List<String> tExclusionWords = mExclusionWords;
		if (tExclusionWords != null) {
			if (StringUtil.startsWithWord(tTarget, tExclusionWords))
				return false;
		}

		// return true if arrive here | 到達すれば true
		return true;
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> tFuncList = new ArrayList<FuncSpec>();
		tFuncList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));
		return tFuncList;
	}

	// -----------------------------------------------------------------------------------------------------------------
}
