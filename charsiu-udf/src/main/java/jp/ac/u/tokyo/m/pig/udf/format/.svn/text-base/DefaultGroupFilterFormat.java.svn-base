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
