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
		// ここに到達した文字列は以下のパターン
		// SubGroupMemberName
		// SubGroupMemberName[value, value, ...]
		int tAdditionalParameterOpenerIndex = aGroupMemberString.indexOf(aAdditionalParameterOpenaer);
		// 前者パターン、AdditionalParameter 無し
		if (tAdditionalParameterOpenerIndex == -1) {
			mGroupMemberName = aGroupMemberString;
			mAdditionalParameters = null;
		}
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
