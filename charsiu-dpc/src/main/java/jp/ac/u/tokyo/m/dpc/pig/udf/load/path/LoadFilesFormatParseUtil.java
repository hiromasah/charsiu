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

package jp.ac.u.tokyo.m.dpc.pig.udf.load.path;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Pattern;

import jp.ac.u.tokyo.m.dpc.specific.SpecificConstants;
import jp.ac.u.tokyo.m.string.StringFormatConstants;

import org.apache.hadoop.conf.Configuration;

/**
 * This class parses LoadFilesFormat. <br>
 * <br>
 * LoadDPC の load対象指定文字列（LoadFilesFormat） を parse するための Util クラスです。<br>
 * 年指定"yyyy" or 年範囲指定"yyyy-yyyy" or ファイルパス直接指定 に対応します。<br>
 * 同じパスが重複しないようにします。<br>
 */
public class LoadFilesFormatParseUtil {

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * @param aLoadFilesFormat
	 *            load対象指定文字列
	 * @param aConfiguration
	 * @param aDpcDataDirectories
	 * @return
	 *         path collection included aLoadFilesFormat<br>
	 *         load対象の範囲に含まれるディレクトリパス（重複除去）<br>
	 */
	static public Collection<String> parseLoadFilesFormat(String aLoadFilesFormat, Configuration aConfiguration, List<String> aDpcDataDirectories) {
		String[] tLoadFilesFormatSplits = aLoadFilesFormat.split(SpecificConstants.LOAD_PATH_SEPARATOR);
		LinkedHashSet<String> tLoadDirectores = new LinkedHashSet<String>();
		for (String tCurrentLoadFilesFormatSplit : tLoadFilesFormatSplits) {
			parseLoadFilesFormatSplit(tLoadDirectores, tCurrentLoadFilesFormatSplit.trim(), aDpcDataDirectories);
		}
		return tLoadDirectores;
	}

	private static void parseLoadFilesFormatSplit(
			LinkedHashSet<String> aLoadDirectores, String aLoadFilesFormatSplit, List<String> aDpcDataDirectories) {
		// aLoadFilesFormatSplit :: "yyyy" or "yyyy-yyyy" or file-path
		// aLoadFilesFormatSplit :: "2010", "2008-2010" が渡される。左記のフォーマットでない場合は直接パスを指定したものと判断。
		String[] tLoadFilesFormatSplitRangeStartEnd = aLoadFilesFormatSplit.split(SpecificConstants.LOAD_PATH_RANGE_CONNECTOR);
		Pattern tPatternYear = StringFormatConstants.PATTERN_YEAR;
		if (tLoadFilesFormatSplitRangeStartEnd.length == 2) {
			// if range specification | 範囲指定の場合
			String tLoadFilesFormatSplitRangeStart = tLoadFilesFormatSplitRangeStartEnd[0].trim();
			String tLoadFilesFormatSplitRangeEnd = tLoadFilesFormatSplitRangeStartEnd[1].trim();
			if (tPatternYear.matcher(tLoadFilesFormatSplitRangeStart).matches()
					&& tPatternYear.matcher(tLoadFilesFormatSplitRangeEnd).matches()) {
				// "yyyy-yyyy" 範囲指定
				parseLoadFilesFormatRangeYear(aLoadDirectores,
						tLoadFilesFormatSplitRangeStart,
						tLoadFilesFormatSplitRangeEnd,
						aDpcDataDirectories);
			} else {
				// if tPatternYear is not "yyyy-yyyy" pattern, aLoadFilesFormatSplit is file path.
				// LoadFilesFormat の年指定でない場合は、直接ファイルまたはディレクトリが指定されたものとして、そのまま追加する。
				aLoadDirectores.add(aLoadFilesFormatSplit);
			}
		} else {
			// if not range specification | 範囲指定でない場合
			if (tPatternYear.matcher(aLoadFilesFormatSplit).matches()) {
				// "yyyy"
				addLoadDirectoresInYear(aLoadDirectores, aLoadFilesFormatSplit, aDpcDataDirectories);
			} else {
				// if tPatternYear is not "yyyy-yyyy" pattern, aLoadFilesFormatSplit is file path.
				// LoadFilesFormat の年指定でない場合は、直接ファイルまたはディレクトリが指定されたものとして、そのまま追加する。
				aLoadDirectores.add(aLoadFilesFormatSplit);
			}
		}

	}

	private static void parseLoadFilesFormatRangeYear(
			LinkedHashSet<String> aLoadDirectores,
			String tLoadFilesFormatSplitRangeStart,
			String tLoadFilesFormatSplitRangeEnd,
			List<String> aDpcDataDirectories) throws NumberFormatException {
		Integer tRangeStartYear = Integer.parseInt(tLoadFilesFormatSplitRangeStart);
		Integer tRangeEndYear = Integer.parseInt(tLoadFilesFormatSplitRangeEnd);
		while (tRangeStartYear <= tRangeEndYear) {
			addLoadDirectoresInYear(aLoadDirectores, String.valueOf(tRangeStartYear++), aDpcDataDirectories);
		}
	}

	private static void addLoadDirectoresInYear(
			LinkedHashSet<String> aLoadDirectores, String aLoadFilesFormatSplit, List<String> aDpcDataDirectories) {
		for (String tDpcDataDirectory : aDpcDataDirectories) {
			StringBuilder tDirectoryStructureBuilder = new StringBuilder();
			tDirectoryStructureBuilder.append(tDpcDataDirectory);
			tDirectoryStructureBuilder.append('/');
			tDirectoryStructureBuilder.append(aLoadFilesFormatSplit);
			tDirectoryStructureBuilder.append('/');
			aLoadDirectores.add(tDirectoryStructureBuilder.toString());
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

}
