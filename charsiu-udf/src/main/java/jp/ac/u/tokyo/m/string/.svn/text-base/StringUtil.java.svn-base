package jp.ac.u.tokyo.m.string;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class StringUtil {

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * aTarget をトリムします。トリムの対象には全角スペース '　' も含みます。
	 * 
	 * @param aTarget
	 *            トリム対象
	 * @return トリムした文字列
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
	 *            対象文字列
	 * @param aReplaceWords
	 *            置換対応マップ
	 * @return aTarget を aReplaceWords(key を value で置換) で置換した文字列
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
	 *            対象文字列
	 * @param aExpectWordList
	 *            期待する文字列
	 * @return aTarget が aExpectWordList のどれかで始まっていれば true
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
	 *            対象文字列
	 * @param aExpectWordList
	 *            期待する文字列
	 * @return aTarget が aExpectWordList のどれかと一致すれば true
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
	 *            対象文字列
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
	 *            対象文字列
	 * @return 正規表現グループ用に "(" + aTarget + ")" した文字列
	 */
	public static String quoteRegularExpressionGroup(String aTarget) {
		return quoteString(aTarget, "(", ")");
	}

	// -----------------------------------------------------------------------------------------------------------------

}
