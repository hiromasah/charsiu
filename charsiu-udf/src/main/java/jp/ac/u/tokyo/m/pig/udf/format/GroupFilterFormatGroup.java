package jp.ac.u.tokyo.m.pig.udf.format;

import java.util.List;

public interface GroupFilterFormatGroup {
	String getGroupName();

	List<String> getGroupMemberNames();

	List<GroupFilterFormatGroupMember> getGroupMembers();

	boolean isMember(String aTarget);

	/**
	 * @return null = false, !null = ture を意味する。
	 */
	GroupFilterFormatGroupMember isMemberReturnGroupMember(String aTarget);
}
