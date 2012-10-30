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

package jp.ac.u.tokyo.m.log;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;

public class LogUtil {

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Output "[aPrefix]\"[aParams[0]]\" \"[aParams[1]]\"... [aSuffix]" format log. <br>
	 * "[aPrefix]\"[aParams[0]]\" \"[aParams[1]]\"... [aSuffix]" のログを出力します。 <br>
	 */
	public static void infoMultiParam(Log aLog, String aPrefix, String aSuffix, List<? extends Object> aParams) {
		aLog.info(createMultiParamMessage(aPrefix, aSuffix, aParams));
	}

	/**
	 * Output "[aPrefix]\"[aParams[0]]\" \"[aParams[1]]\"... [aSuffix]" format log. <br>
	 * "[aPrefix]\"[aParams[0]]\" \"[aParams[1]]\"... [aSuffix]" のログを出力します。 <br>
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
	 * I make the message which enumerated aParams. <br>
	 * aParams を列挙したメッセージを作成します。 <br>
	 * 
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
