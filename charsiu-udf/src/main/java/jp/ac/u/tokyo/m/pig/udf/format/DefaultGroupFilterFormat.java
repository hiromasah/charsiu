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

public class DefaultGroupFilterFormat implements GroupFilterFormat {

	// -----------------------------------------------------------------------------------------------------------------

	private final List<GroupFilterFormatGroup> mGroupList;
	private List<String> mFilterList = null;

	// -----------------------------------------------------------------------------------------------------------------

	public DefaultGroupFilterFormat(String aGroupFilterFormatString,
			String aWordSeparator, String aSubGroupOpener, String aSubGroupCloser,
			String aAdditionalParameterOpenaer, String aAdditionalParameterCloser) {
		List<String> tSplitedTarget = FormatUtil.splitGroup(aGroupFilterFormatString,
				aWordSeparator, aSubGroupOpener, aSubGroupCloser,
				aAdditionalParameterOpenaer, aAdditionalParameterCloser);
		List<GroupFilterFormatGroup> tGroupList = new ArrayList<GroupFilterFormatGroup>();
		for (String tCurrentGroup : tSplitedTarget) {
			tGroupList.add(new DefaultGroupFilterFormatGroup(tCurrentGroup,
					aWordSeparator, aSubGroupOpener, aSubGroupCloser,
					aAdditionalParameterOpenaer, aAdditionalParameterCloser));
		}
		mGroupList = tGroupList;
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public List<GroupFilterFormatGroup> getGroupList() {
		return mGroupList;
	}

	@Override
	public List<String> getFilterList() {
		if (mFilterList == null) {
			List<GroupFilterFormatGroup> tGroupList = mGroupList;
			List<String> tFilterList = new ArrayList<String>();
			int tGroupListSize = tGroupList.size();
			for (int tIndex = 0; tIndex < tGroupListSize;) {
				tFilterList.addAll(tGroupList.get(tIndex++).getGroupMemberNames());
			}
			return mFilterList = tFilterList;
		} else
			return mFilterList;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
