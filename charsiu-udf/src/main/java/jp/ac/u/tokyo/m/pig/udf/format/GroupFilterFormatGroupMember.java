package jp.ac.u.tokyo.m.pig.udf.format;

import java.util.List;

public interface GroupFilterFormatGroupMember {
	String getGroupMemberName();

	int getAdditionalParameterCount();

	List<AdditionalParameter> getAdditionalParameters();

	boolean hasAdditionalParameter();

	public interface AdditionalParameter {
		public String getValue();
	}
}
