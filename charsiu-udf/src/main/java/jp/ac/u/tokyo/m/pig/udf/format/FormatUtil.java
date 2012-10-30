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

public class FormatUtil {

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * This method splits aTarget by unit of Group. <br>
	 * <br>
	 * aTarget をグループ単位に split します。<br>
	 */
	public static List<String> splitGroup(String aTarget,
			String aWordSeparator, String aSubGroupOpener, String aSubGroupCloser,
			String aAdditionalParameterOpenaer, String aAdditionalParameterCloser) {
		List<String> tResultGroups = new ArrayList<String>();

		int tBeginIndex = 0;
		while (true) {
			int tSeparatorIndex = aTarget.indexOf(aWordSeparator, tBeginIndex);
			if (tSeparatorIndex == -1) {
				// if without aWordSeparator, add all the remaining string and finish
				// aWordSeparator がなければ残りの文字列を全て追加して終了
				addTrimString(tResultGroups, aTarget.substring(tBeginIndex));
				break;
			} else {
				// if found aWordSeparator, add substring before index and go next
				// 見付かったら、その直前までの文字列を追加して次へ
				String tSubstring = aTarget.substring(tBeginIndex, tSeparatorIndex);
				// if found "(" in substring, process SubGroup mode.
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
				// if found "[" and not found "]" in substring, process AdditionalParameter mode.
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
