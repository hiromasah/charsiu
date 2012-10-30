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

import java.util.ArrayList;
import java.util.List;

public class DefaultGroupFilterFormatGroupMember implements GroupFilterFormatGroupMember {

	// -----------------------------------------------------------------------------------------------------------------

	private final String mGroupMemberName;
	private final List<AdditionalParameter> mAdditionalParameters;

	// -----------------------------------------------------------------------------------------------------------------

	public DefaultGroupFilterFormatGroupMember(String aGroupMemberString,
			String aWordSeparator,
			String aSubGroupOpener, String aSubGroupCloser,
			String aAdditionalParameterOpenaer, String aAdditionalParameterCloser) {
		// string that arrived at here is following patterns | ここに到達した文字列は以下のパターン
		// SubGroupMemberName
		// SubGroupMemberName[value, value, ...]
		int tAdditionalParameterOpenerIndex = aGroupMemberString.indexOf(aAdditionalParameterOpenaer);
		// pattern "SubGroupMemberName", unexists AdditionalParameter
		// 前者パターン、AdditionalParameter 無し
		if (tAdditionalParameterOpenerIndex == -1) {
			mGroupMemberName = aGroupMemberString;
			mAdditionalParameters = null;
		}
		// pattern "SubGroupMemberName[value, value, ...]", exists AdditionalParameter
		// 後者パターン、AdditionalParameter 有り
		else {
			mGroupMemberName = aGroupMemberString.substring(0, tAdditionalParameterOpenerIndex);
			String[] tAdditionalParameterStrings = aGroupMemberString.substring(tAdditionalParameterOpenerIndex + 1, aGroupMemberString.indexOf(aAdditionalParameterCloser, tAdditionalParameterOpenerIndex)).split(aWordSeparator);
			List<AdditionalParameter> tAdditionalParameters = new ArrayList<AdditionalParameter>();
			for (String tCurrentAdditionalParameter : tAdditionalParameterStrings) {
				tAdditionalParameters.add(new DefaultAdditionalParameter(tCurrentAdditionalParameter.trim()));
			}
			mAdditionalParameters = tAdditionalParameters;
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public int getAdditionalParameterCount() {
		if (mAdditionalParameters == null)
			return 0;
		return mAdditionalParameters.size();
	}

	@Override
	public List<AdditionalParameter> getAdditionalParameters() {
		return mAdditionalParameters;
	}

	@Override
	public String getGroupMemberName() {
		return mGroupMemberName;
	}

	@Override
	public boolean hasAdditionalParameter() {
		return mAdditionalParameters != null;
	}

	@Override
	public String toString() {
		StringBuilder tResult = new StringBuilder();
		tResult.append(mGroupMemberName);
		if (hasAdditionalParameter()) {
			for (AdditionalParameter tCurrentAdditionalParameter : mAdditionalParameters) {
				tResult.append(tCurrentAdditionalParameter.toString());
			}
		}
		return tResult.toString();
	}

	// -----------------------------------------------------------------------------------------------------------------

	public class DefaultAdditionalParameter implements AdditionalParameter {
		private String mValue;

		public DefaultAdditionalParameter(String aAdditionalParameterString) {
			mValue = aAdditionalParameterString;
		}

		@Override
		public String getValue() {
			return mValue;
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

}
