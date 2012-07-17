package jp.ac.u.tokyo.m.dpc.pig.udf.load.path;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.regex.Pattern;

import jp.ac.u.tokyo.m.dpc.specific.SpecificConstants;
import jp.ac.u.tokyo.m.string.StringFormatConstants;

import org.apache.hadoop.conf.Configuration;

/**
 * LoadDPC の load対象指定文字列（LoadFilesFormat） を parse するための Util クラスです。
 * 年指定"yyyy" or 年範囲指定"yyyy-yyyy" or ファイルパス直接指定 に対応します。
 * 同じパスが重複しないようにします。
 */
public class LoadFilesFormatParseUtil {

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * @param aLoadFilesFormat
	 *            load対象指定文字列
	 * @param aTConfiguration
	 * @return load対象の範囲に含まれるディレクトリパス（重複除去）
	 */
	static public Collection<String> parseLoadFilesFormat(String aLoadFilesFormat, Configuration aConfiguration) {
		String[] tLoadFilesFormatSplits = aLoadFilesFormat.split(SpecificConstants.LOAD_PATH_SEPARATOR);
		String tDpcDataDirectory = aConfiguration.get(SpecificConstants.CONFIGURATION_KEY_DPC_DATA_DIRECTORY, SpecificConstants.DPC_DATA_DIRECTORY_DEFAULT);
		LinkedHashSet<String> tLoadDirectores = new LinkedHashSet<String>();
		for (String tCurrentLoadFilesFormatSplit : tLoadFilesFormatSplits) {
			parseLoadFilesFormatSplit(tLoadDirectores, tCurrentLoadFilesFormatSplit.trim(), tDpcDataDirectory);
		}
		return tLoadDirectores;
	}

	private static void parseLoadFilesFormatSplit(
			LinkedHashSet<String> aLoadDirectores, String aLoadFilesFormatSplit, String aDpcDataDirectory) {
		// aLoadFilesFormatSplit :: "2010", "2008-2010" が渡される。左記のフォーマットでない場合は直接パスを指定したものと判断。
		String[] tLoadFilesFormatSplitRangeStartEnd = aLoadFilesFormatSplit.split(SpecificConstants.LOAD_PATH_RANGE_CONNECTOR);
		Pattern tPatternYear = StringFormatConstants.PATTERN_YEAR;
		if (tLoadFilesFormatSplitRangeStartEnd.length == 2) {
			// 範囲指定の場合
			String tLoadFilesFormatSplitRangeStart = tLoadFilesFormatSplitRangeStartEnd[0].trim();
			String tLoadFilesFormatSplitRangeEnd = tLoadFilesFormatSplitRangeStartEnd[1].trim();
			if (tPatternYear.matcher(tLoadFilesFormatSplitRangeStart).matches()
					&& tPatternYear.matcher(tLoadFilesFormatSplitRangeEnd).matches()) {
				// "yyyy-yyyy" 範囲指定
				parseLoadFilesFormatRangeYear(aLoadDirectores,
						tLoadFilesFormatSplitRangeStart,
						tLoadFilesFormatSplitRangeEnd,
						aDpcDataDirectory);
			} else {
				// LoadFilesFormat の年指定でない場合は、直接ファイルまたはディレクトリが指定されたものとして、そのまま追加する。
				// - が含まれるファイルまたはディレクトリかもしれない。
				aLoadDirectores.add(aLoadFilesFormatSplit);
			}
		} else {
			// 範囲指定でない場合
			if (tPatternYear.matcher(aLoadFilesFormatSplit).matches()) {
				// "yyyy"
				addLoadDirectoresInYear(aLoadDirectores, aLoadFilesFormatSplit, aDpcDataDirectory);
			} else {
				// LoadFilesFormat の年指定でない場合は、直接ファイルまたはディレクトリが指定されたものとして、そのまま追加する。
				aLoadDirectores.add(aLoadFilesFormatSplit);
			}
		}

	}

	private static void parseLoadFilesFormatRangeYear(
			LinkedHashSet<String> aLoadDirectores,
			String tLoadFilesFormatSplitRangeStart,
			String tLoadFilesFormatSplitRangeEnd,
			String aDpcDataDirectory) throws NumberFormatException {
		Integer tRangeStartYear = Integer.parseInt(tLoadFilesFormatSplitRangeStart);
		Integer tRangeEndYear = Integer.parseInt(tLoadFilesFormatSplitRangeEnd);
		while (tRangeStartYear <= tRangeEndYear) {
			addLoadDirectoresInYear(aLoadDirectores, String.valueOf(tRangeStartYear++), aDpcDataDirectory);
		}
	}

	private static void addLoadDirectoresInYear(
			LinkedHashSet<String> aLoadDirectores, String aLoadFilesFormatSplit, String aDpcDataDirectory) {
		StringBuilder tDirectoryStructureBuilder = new StringBuilder();
		tDirectoryStructureBuilder.append(aDpcDataDirectory);
		tDirectoryStructureBuilder.append('/');
		tDirectoryStructureBuilder.append(aLoadFilesFormatSplit);
		tDirectoryStructureBuilder.append('/');
		aLoadDirectores.add(tDirectoryStructureBuilder.toString());
	}

	// -----------------------------------------------------------------------------------------------------------------

}
