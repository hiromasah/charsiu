package jp.ac.u.tokyo.m.dpc.specific;

import jp.ac.u.tokyo.m.string.StringUtil;

public class LoadTermUtil {

	/**
	 * load-term 表現 "[foo]" を正規表現で検出するために "\\[", "\\]" で挟んだ文字列を生成します。
	 * 
	 * @return "\\[" + aTarget + "\\]"
	 */
	public static String quoteReplaceWord(String aTarget) {
		return StringUtil.quoteString(aTarget, SpecificConstants.FILES_REPLACE_WORD_OPENER, SpecificConstants.FILES_REPLACE_WORD_CLOSER);
	}

}
