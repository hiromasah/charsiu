package jp.ac.u.tokyo.m.pig.udf.format;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import jp.ac.u.tokyo.m.pig.udf.format.GroupFilterFormat;
import jp.ac.u.tokyo.m.pig.udf.format.GroupFilterFormatGroup;
import jp.ac.u.tokyo.m.pig.udf.format.GroupFilterFormatGroupMember;
import jp.ac.u.tokyo.m.pig.udf.format.StockGroupFilterFormatFactory;
import jp.ac.u.tokyo.m.pig.udf.format.GroupFilterFormatGroupMember.AdditionalParameter;
import junit.framework.Assert;

import org.junit.Test;

public class GroupFilterFormatTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static final String GROUP_FILTER_FORMAT_STRING =
			"4legs(dog[30, jp], cat[55, us], caw[13, cl]), etc(caw[22, jp], fish[99, jp], bug[99, jp])";

	// -----------------------------------------------------------------------------------------------------------------

	private class ProtoGroupMember {
		final String mGroupMemberName;
		final String[] mAdditionalParams;

		public ProtoGroupMember(String aGroupMemberName, String... aAdditionalParams) {
			mGroupMemberName = aGroupMemberName;
			mAdditionalParams = aAdditionalParams;
		}
	}

	private LinkedHashMap<String, String[]> createGroupMembers(ProtoGroupMember... aGroupMember) {
		LinkedHashMap<String, String[]> tGroupMembers = new LinkedHashMap<String, String[]>();
		for (ProtoGroupMember tStringAndStringArraySetStructure : aGroupMember) {
			tGroupMembers.put(tStringAndStringArraySetStructure.mGroupMemberName, tStringAndStringArraySetStructure.mAdditionalParams);
		}
		return tGroupMembers;
	}

	// -----------------------------------------------------------------------------------------------------------------

	private void assertEqualsGFF(LinkedHashMap<String, LinkedHashMap<String, String[]>> aExpected, String aGroupFilterFormatString) {
		GroupFilterFormat tGFF = StockGroupFilterFormatFactory.INSTANCE.generateGroupFilterFormat(aGroupFilterFormatString);
		verifyGroupList(aExpected, tGFF);
		verifyFilterList(aExpected, tGFF);
	}

	private void verifyGroupList(LinkedHashMap<String, LinkedHashMap<String, String[]>> aExpected, GroupFilterFormat aGroupFilterFormat) {
		Iterator<Entry<String, LinkedHashMap<String, String[]>>> tExpectedIterator = aExpected.entrySet().iterator();
		List<GroupFilterFormatGroup> tGroupList = aGroupFilterFormat.getGroupList();
		for (GroupFilterFormatGroup tGFFGroup : tGroupList) {
			Entry<String, LinkedHashMap<String, String[]>> tExpectedGroup = tExpectedIterator.next();
			Assert.assertEquals(tExpectedGroup.getKey(), tGFFGroup.getGroupName());
			Iterator<Entry<String, String[]>> tExpectedGroupMemberIterator = tExpectedGroup.getValue().entrySet().iterator();
			for (GroupFilterFormatGroupMember tGroupFilterFormatGroupMember : tGFFGroup.getGroupMembers()) {
				Entry<String, String[]> tExpectedGroupMember = tExpectedGroupMemberIterator.next();
				Assert.assertEquals(tExpectedGroupMember.getKey(), tGroupFilterFormatGroupMember.getGroupMemberName());
				String[] tExpectedGroupMemberAdditionalParams = tExpectedGroupMember.getValue();
				int tAdditionaParamsIndex = 0;
				for (AdditionalParameter tAdditionalParameter : tGroupFilterFormatGroupMember.getAdditionalParameters()) {
					Assert.assertEquals(tExpectedGroupMemberAdditionalParams[tAdditionaParamsIndex++], tAdditionalParameter.getValue());
				}
			}
		}
	}

	private void verifyFilterList(LinkedHashMap<String, LinkedHashMap<String, String[]>> aExpected, GroupFilterFormat aGroupFilterFormat) {
		Iterator<Entry<String, LinkedHashMap<String, String[]>>> tExpectedIterator = aExpected.entrySet().iterator();
		List<String> tFilterList = aGroupFilterFormat.getFilterList();
		Iterator<Entry<String, String[]>> tExpectedGroupMemberIterator = null;
		for (String tFilter : tFilterList) {
			if (tExpectedGroupMemberIterator == null || !tExpectedGroupMemberIterator.hasNext()) {
				Entry<String, LinkedHashMap<String, String[]>> tExpectedGroup = tExpectedIterator.next();
				tExpectedGroupMemberIterator = tExpectedGroup.getValue().entrySet().iterator();
			}
			Entry<String, String[]> tExpectedGroupMember = tExpectedGroupMemberIterator.next();
			Assert.assertEquals(tExpectedGroupMember.getKey(), tFilter);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testDefaultGFFFactory() {
		LinkedHashMap<String, LinkedHashMap<String, String[]>> tExpected = new LinkedHashMap<String, LinkedHashMap<String, String[]>>();
		tExpected.put("4legs", createGroupMembers(
				new ProtoGroupMember("dog", "30", "jp"),
				new ProtoGroupMember("cat", "55", "us"),
				new ProtoGroupMember("caw", "13", "cl")
				));
		tExpected.put("etc", createGroupMembers(
				new ProtoGroupMember("caw", "22", "jp"),
				new ProtoGroupMember("fish", "99", "jp"),
				new ProtoGroupMember("bug", "99", "jp")
				));

		assertEqualsGFF(tExpected, GROUP_FILTER_FORMAT_STRING);
	}

	// -----------------------------------------------------------------------------------------------------------------

}
