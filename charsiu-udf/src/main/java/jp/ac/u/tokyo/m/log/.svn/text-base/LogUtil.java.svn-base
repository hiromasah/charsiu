package jp.ac.u.tokyo.m.log;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;

public class LogUtil {

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * "[aPrefix]\"[aParams[0]]\" \"[aParams[1]]\"... [aSuffix]" のログを出力します。
	 */
	public static void infoMultiParam(Log aLog, String aPrefix, String aSuffix, List<? extends Object> aParams) {
		aLog.info(createMultiParamMessage(aPrefix, aSuffix, aParams));
	}

	/**
	 * "[aPrefix]\"[aParams[0]]\" \"[aParams[1]]\"... [aSuffix]" のログを出力します。
	 */
	public static void errorMultiParam(Log aLog, String aPrefix, String aSuffix, List<? extends Object> aParams, Throwable aThrowable) {
		aLog.error(createMultiParamMessage(aPrefix, aSuffix, aParams), aThrowable);
	}

	// -----------------------------------------------------------------------------------------------------------------

	public static void errorIllegalModeName(Log aLog, Object[] aEnumValues, String aModeName, RuntimeException aCause) throws IllegalArgumentException {
		LogUtil.errorMultiParam(aLog, "\n" +
				"無効なモードが指定されました : " + aModeName + "\n" +
				"有効なモードは次のものです（大/小文字問わず） : ", "", Arrays.asList(aEnumValues), null);
		throw new IllegalArgumentException("an illegal argument : \"" + aModeName + "\"", aCause);
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * @param aPrefix
	 *            接頭辞
	 * @param aSuffix
	 *            接尾辞
	 * @param aParams
	 *            列挙するアイテム
	 * @return aPrefix + (aParams 列挙) + aSuffix した文字列
	 */
	public static String createMultiParamMessage(String aPrefix, String aSuffix, List<? extends Object> aParams) {
		StringBuilder tLogTextBuilder = new StringBuilder();
		tLogTextBuilder.append(aPrefix);
		for (Object tCurrentParam : aParams) {
			tLogTextBuilder.append("\"");
			tLogTextBuilder.append(tCurrentParam.toString());
			tLogTextBuilder.append("\" ");
		}
		tLogTextBuilder.append(aSuffix);
		String tString = tLogTextBuilder.toString();
		return tString;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
