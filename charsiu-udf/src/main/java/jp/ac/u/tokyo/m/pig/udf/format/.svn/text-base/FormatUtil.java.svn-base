package jp.ac.u.tokyo.m.pig.udf.format;

import java.util.ArrayList;
import java.util.List;

public class FormatUtil {

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * aTarget をグループ単位に split します。
	 */
	public static List<String> splitGroup(String aTarget,
			String aWordSeparator, String aSubGroupOpener, String aSubGroupCloser,
			String aAdditionalParameterOpenaer, String aAdditionalParameterCloser) {
		List<String> tResultGroups = new ArrayList<String>();

		int tBeginIndex = 0;
		while (true) {
			int tSeparatorIndex = aTarget.indexOf(aWordSeparator, tBeginIndex);
			if (tSeparatorIndex == -1) {
				// ',' がなければ残りの文字列を全て追加して終了
				addTrimString(tResultGroups, aTarget.substring(tBeginIndex));
				break;
			} else {
				// 見付かったら、その直前までの文字列を追加して次へ
				String tSubstring = aTarget.substring(tBeginIndex, tSeparatorIndex);
				// 分割文字列の中に "(" が含まれるなら SubGroup モード。次の ")" までを追加
				if (tSubstring.contains(aSubGroupOpener)) {
					int tSubGroupCloserIndex = aTarget.indexOf(aSubGroupCloser, tBeginIndex) + 1;
					String tGroupWithSubGroup = aTarget.substring(tBeginIndex, tSubGroupCloserIndex);
					addTrimString(tResultGroups, tGroupWithSubGroup);
					int tNextSeparatorIndex = aTarget.indexOf(aWordSeparator, tSubGroupCloserIndex);
					if (tNextSeparatorIndex == -1)
						break;
					else {
						tBeginIndex = tNextSeparatorIndex + 1;
						continue;
					}
				}
				// 分割文字列の中に "[" が含まれ、 "]" が含まれないなら AdditionalParameter モード。次の "]" までを追加
				else if (tSubstring.contains(aAdditionalParameterOpenaer) && !tSubstring.contains(aAdditionalParameterCloser)) {
					int tAdditionalParameterCloserIndex = aTarget.indexOf(aAdditionalParameterCloser, tBeginIndex) + 1;
					String tGroupWithSubGroup = aTarget.substring(tBeginIndex, tAdditionalParameterCloserIndex);
					addTrimString(tResultGroups, tGroupWithSubGroup);
					int tNextSeparatorIndex = aTarget.indexOf(aWordSeparator, tAdditionalParameterCloserIndex);
					if (tNextSeparatorIndex == -1)
						break;
					else {
						tBeginIndex = tNextSeparatorIndex + 1;
						continue;
					}
				}
				addTrimString(tResultGroups, tSubstring);
				tBeginIndex = tSeparatorIndex + 1;
			}
		}

		return tResultGroups;
	}

	// -----------------------------------------------------------------------------------------------------------------

	private static void addTrimString(List<String> aStringList, String aString) {
		String tTrimedString = aString.trim();
		if (tTrimedString.length() == 0)
			return;
		aStringList.add(tTrimedString);
	}

	// -----------------------------------------------------------------------------------------------------------------

}
