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

package jp.ac.u.tokyo.m.string;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class StringUtil {

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * This method trim aTarget. Double-byte space '　' is targeted for a trim, too. <br>
	 * aTarget をトリムします。トリムの対象には全角スペース '　' も含みます。<br>
	 * 
	 * @param aTarget
	 *            トリム対象
	 * @return
	 *         trimmed string<br>
	 *         トリムした文字列<br>
	 */
	public static String trimMultiByteCharacter(String aTarget) {
		char[] tTargetValue = aTarget.toCharArray();
		int tLength = tTargetValue.length;
		int tStart = 0;
		int tEnd = tLength;
		while ((tStart < tEnd) && ((tTargetValue[tStart] <= ' ') || (tTargetValue[tStart] == '　'))) {
			tStart++;
		}
		while ((tStart < tEnd) && ((tTargetValue[tEnd - 1] <= ' ') || (tTargetValue[tEnd - 1] == '　'))) {
			tEnd--;
		}
		return ((tStart > 0) || (tEnd < tLength)) ? aTarget.substring(tStart, tEnd) : aTarget;
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * @param aTarget
	 *            target string<br>
	 *            対象文字列<br>
	 * @param aReplaceWords
	 *            substitution pattern map<br>
	 *            置換対応マップ<br>
	 * @return
	 *         String that replaced aReplaceWords(key -> value) in aTarget<br>
	 *         aTarget を aReplaceWords(key を value で置換) で置換した文字列<br>
	 */
	public static String replaceWords(String aTarget, HashMap<String, String> aReplaceWords) {
		for (Entry<String, String> tCurrentReplaceSet : aReplaceWords.entrySet()) {
			aTarget = aTarget.replaceAll(tCurrentReplaceSet.getKey(), tCurrentReplaceSet.getValue());
		}
		return aTarget;
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * @param aTarget
	 *            target string<br>
	 *            対象文字列<br>
	 * @param aExpectWordList
	 *            期待する文字列
	 * @return
	 *         If aTarget starts with the items that appear in aExpectWordList, return true.<br>
	 *         aTarget が aExpectWordList のどれかで始まっていれば true<br>
	 */
	public static boolean startsWithWord(String aTarget, List<String> aExpectWordList) {
		for (String tCurrentExpectWord : aExpectWordList) {
			if (aTarget.startsWith(tCurrentExpectWord))
				return true;
		}
		return false;
	}

	/**
	 * @param aTarget
	 *            target string<br>
	 *            対象文字列<br>
	 * @param aExpectWordList
	 *            期待する文字列<br>
	 * @return
	 *         If aTarget accords with the items that appear in aExpectWordList, return true.<br>
	 *         aTarget が aExpectWordList のどれかと一致すれば true<br>
	 */
	public static boolean equalsWord(String aTarget, List<String> aExpectWordList) {
		for (String tCurrentExpectWord : aExpectWordList) {
			if (aTarget.equals(tCurrentExpectWord))
				return true;
		}
		return false;
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * @param aTarget
	 *            target string<br>
	 *            対象文字列<br>
	 * @param aPrefix
	 *            接頭辞
	 * @param aSuffix
	 *            接尾辞
	 * @return aPrefix + aTarget + aSuffix した文字列
	 */
	public static String quoteString(String aTarget, String aPrefix, String aSuffix) {
		StringBuilder tStringBuilder = new StringBuilder();
		tStringBuilder.append(aPrefix);
		tStringBuilder.append(aTarget);
		tStringBuilder.append(aSuffix);
		return tStringBuilder.toString();
	}

	/**
	 * @param aTarget
	 *            target string<br>
	 *            対象文字列<br>
	 * @return
	 *         正規表現グループ用に "(" + aTarget + ")" した文字列<br>
	 */
	public static String quoteRegularExpressionGroup(String aTarget) {
		return quoteString(aTarget, "(", ")");
	}

	// -----------------------------------------------------------------------------------------------------------------

}
