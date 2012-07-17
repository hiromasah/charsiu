package jp.ac.u.tokyo.m.pig.udf.format;

import java.util.ArrayList;
import java.util.List;

import jp.ac.u.tokyo.m.string.StringUtil;

public class DefaultGroupFilterFormatGroup implements GroupFilterFormatGroup {

	// -----------------------------------------------------------------------------------------------------------------

	private final String mGroupName;
	private final List<String> mGroupMemberNames;
	private final List<GroupFilterFormatGroupMember> mGroupMembers;

	// -----------------------------------------------------------------------------------------------------------------

	public DefaultGroupFilterFormatGroup(String aGroupString,
			String aWordSeparator, String aSubGroupOpener, String aSubGroupCloser,
			String aAdditionalParameterOpenaer, String aAdditionalParameterCloser) {
		int tSubGroupOpenerIndex = aGroupString.indexOf(aSubGroupOpener);
		if (tSubGroupOpenerIndex == -1) {
			mGroupMemberNames = new ArrayList<String>();
			mGroupMembers = new ArrayList<GroupFilterFormatGroupMember>();
			DefaultGroupFilterFormatGroupMember tGroupMember = new DefaultGroupFilterFormatGroupMember(
					aGroupString,
					aWordSeparator, aSubGroupOpener, aSubGroupCloser,
					aAdditionalParameterOpenaer, aAdditionalParameterCloser);
			mGroupMembers.add(tGroupMember);
			mGroupName = tGroupMember.getGroupMemberName();
			mGroupMemberNames.add(tGroupMember.getGroupMemberName());
		} else {
			mGroupName = aGroupString.substring(0, tSubGroupOpenerIndex);
			List<String> tGroupMemberStrings = FormatUtil.splitGroup(
					aGroupString.substring(tSubGroupOpenerIndex + 1, aGroupString.indexOf(aSubGroupCloser, tSubGroupOpenerIndex)),
					aWordSeparator, aSubGroupOpener, aSubGroupCloser,
					aAdditionalParameterOpenaer, aAdditionalParameterCloser);
			List<GroupFilterFormatGroupMember> tGroupMembers = new ArrayList<GroupFilterFormatGroupMember>();
			List<String> tGroupMemberNames = new ArrayList<String>();
			int tGroupMemberStringsSize = tGroupMemberStrings.size();
			for (int tIndex = 0; tIndex < tGroupMemberStringsSize;) {
				DefaultGroupFilterFormatGroupMember tGroupMember =
						new DefaultGroupFilterFormatGroupMember(
								tGroupMemberStrings.get(tIndex++),
								aWordSeparator, aSubGroupOpener, aSubGroupCloser,
								aAdditionalParameterOpenaer, aAdditionalParameterCloser);
				tGroupMembers.add(tGroupMember);
				tGroupMemberNames.add(tGroupMember.getGroupMemberName());
			}
			mGroupMembers = tGroupMembers;
			mGroupMemberNames = tGroupMemberNames;
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public List<String> getGroupMemberNames() {
		return mGroupMemberNames;
	}

	@Override
	public String getGroupName() {
		return mGroupName;
	}

	@Override
	public boolean isMember(String aTarget) {
		return StringUtil.startsWithWord(aTarget, mGroupMemberNames);
	}

	@Override
	public GroupFilterFormatGroupMember isMemberReturnGroupMember(String aTarget) {
		for (GroupFilterFormatGroupMember tCurrentGroupMember : mGroupMembers) {
			if (aTarget.startsWith(tCurrentGroupMember.getGroupMemberName())) {
				return tCurrentGroupMember;
			}
		}
		return null;
	}

	@Override
	public List<GroupFilterFormatGroupMember> getGroupMembers() {
		return mGroupMembers;
	}

	@Override
	public String toString() {
		StringBuilder tResult = new StringBuilder();
		tResult.append(mGroupName);
		tResult.append("(");
		for (GroupFilterFormatGroupMember tCurrentMember : mGroupMembers) {
			tResult.append(tCurrentMember.toString());
			tResult.append(", ");
		}
		tResult.append(")");
		return tResult.toString();
	}

	// -----------------------------------------------------------------------------------------------------------------

}
