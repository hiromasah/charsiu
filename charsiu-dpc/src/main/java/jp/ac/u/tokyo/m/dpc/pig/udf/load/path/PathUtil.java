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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jp.ac.u.tokyo.m.string.StringFormatConstants;

import org.apache.hadoop.fs.Path;

public class PathUtil {

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * @param aTargetFileCategory
	 *            "e" or "f" or "ff1" or "nendojou" or "d"
	 * @return definition/files/[aTargetFileCategory].files
	 */
	public static String createDefinitionFilesPath(String aTargetFileCategory) {
		StringBuilder tPathBuilder = new StringBuilder();
		tPathBuilder.append(PathConstants.RESOURCE_PATH_DEFINITION_FILES_DIRECTORY);
		tPathBuilder.append(aTargetFileCategory);
		tPathBuilder.append(PathConstants.EXTENSION_DEFINITION_FILES);
		return tPathBuilder.toString();
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * return SchemaFile path. <br>
	 * <br>
	 * aTargetFileCategory が "e" or "f" の場合で、aTargetFilePath のファイル名が IE or En, IF or Fn で始まる場合、
	 * 同じ年の同系統のファイルでもスキーマに差異があるため、それぞれのファイルに対応したスキーマファイルを返す。<br>
	 * 上記以外の場合は getSchemaFilePath と同じ動作だが、MasterSchema の年を指定する機能が無いため、ReadSchema の取得のみに利用する。<br>
	 * 
	 * @param aTargetFilePath
	 *            対象となるファイルのパス。
	 * @param aTargetFileCategory
	 *            "e" or "f" or "ff1" or "nendojou" or "d"
	 * @return 該当する .schema へのパス文字列
	 */
	public static String getReadSchemaFilePath(Path aTargetFilePath, String aTargetFileCategory) {
		String tTargetFilePathString = aTargetFilePath.toUri().getPath();
		String tTargetFileName = aTargetFilePath.getName();
		Pattern tPattern = StringFormatConstants.PATTERN_YEAR;
		Matcher tMacher = tPattern.matcher(tTargetFilePathString);
		if (tMacher.find()) {
			String tReadYear = tMacher.group(1);
			if (aTargetFileCategory.equals("e")) {
				if (tTargetFileName.startsWith("IE")) {
					return createReadTextSchemaFIlePath(tReadYear, "i/ie");
				} else if (tTargetFileName.startsWith("En")) {
					return createReadTextSchemaFIlePath(tReadYear, "n/en");
				} else {
					return getSchemaFilePath(tReadYear, aTargetFileCategory);
				}
			} else if (aTargetFileCategory.equals("f")) {
				if (tTargetFileName.startsWith("IF")) {
					return createReadTextSchemaFIlePath(tReadYear, "i/if");
				} else if (tTargetFileName.startsWith("Fn")) {
					return createReadTextSchemaFIlePath(tReadYear, "n/fn");
				} else {
					return getSchemaFilePath(tReadYear, aTargetFileCategory);
				}
			} else if (aTargetFileCategory.equals("ff1")) {
				if (tTargetFileName.startsWith("FF1")) {
					return createReadTextSchemaFIlePath(tReadYear, "ff1");
				} else {
					return getSchemaFilePath(tReadYear, aTargetFileCategory);
				}
			} else if (aTargetFileCategory.equals("d")) {
				if (tTargetFileName.startsWith("Dn")) {
					return createReadTextSchemaFIlePath(tReadYear, "d");
				} else {
					return getSchemaFilePath(tReadYear, aTargetFileCategory);
				}
			} else {
				return getSchemaFilePath(tReadYear, aTargetFileCategory);
			}
		} else {
			// aTargetFilePath に yyyy を含んでいない場合
			return null;
		}
	}

	private static String createReadTextSchemaFIlePath(String aReadYear, String aFileName) {
		StringBuilder tResultBuilder = new StringBuilder();
		tResultBuilder.append(PathConstants.RESOURCE_PATH_DEFINITION_SCHEMA_DIRECTORY);
		tResultBuilder.append(aReadYear);
		tResultBuilder.append(PathConstants.RESOURCE_PART_DEFINITION_SCHEMA_DIRECTORY_TEXT);
		tResultBuilder.append(aFileName);
		tResultBuilder.append(PathConstants.EXTENSION_DEFINITION_SCHEMA);
		return tResultBuilder.toString();
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * aTargetYear に指定された文字列が "2004" などの西暦（数字４文字）であれば
	 * definition/schema/[aYear]/[aTargetFileCategory].schema を返します。
	 * それ以外の文字列であれば aTargetYear をそのまま返します（スキーマファイルがフルパス指定されたと判断）。
	 */
	public static String getSchemaFilePath(String aTargetYear, String aTargetFileCategory) {
		if (StringFormatConstants.PATTERN_YEAR.matcher(aTargetYear).matches()) {
			StringBuilder tResultBuilder = new StringBuilder();
			tResultBuilder.append(PathConstants.RESOURCE_PATH_DEFINITION_SCHEMA_DIRECTORY);
			tResultBuilder.append(aTargetYear);
			tResultBuilder.append("/");
			tResultBuilder.append(aTargetFileCategory);
			tResultBuilder.append(PathConstants.EXTENSION_DEFINITION_SCHEMA);
			return tResultBuilder.toString();
		} else {
			return aTargetYear;
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

}
