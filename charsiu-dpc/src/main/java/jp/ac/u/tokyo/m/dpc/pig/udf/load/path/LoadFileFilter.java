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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jp.ac.u.tokyo.m.dpc.specific.LoadTermUtil;
import jp.ac.u.tokyo.m.string.StringUtil;

import org.apache.hadoop.fs.FileStatus;

/**
 * This class filters files from specific file name pattern and SUBMIT_NUMBER. <br>
 * <br>
 * 特定のファイル名パターンと SUBMIT_NUMBER から、読み込むファイルをフィルタします。 <br>
 */
public class LoadFileFilter {

	// -----------------------------------------------------------------------------------------------------------------

	private LinkedHashMap<DirectoryNameElements, Pattern> mFileNamePatterns;

	// -----------------------------------------------------------------------------------------------------------------

	public LoadFileFilter(List<String> aFilePatternStrings, LinkedHashMap<String, String> aReplaceWords) {
		LinkedHashMap<String, String> tReplaceWordsQuoted = new LinkedHashMap<String, String>();
		for (Entry<String, String> tEntry : aReplaceWords.entrySet()) {
			tReplaceWordsQuoted.put(LoadTermUtil.quoteReplaceWord(tEntry.getKey()),
					StringUtil.quoteRegularExpressionGroup(tEntry.getValue()));
		}
		LinkedHashMap<DirectoryNameElements, Pattern> tFileNamePatterns = mFileNamePatterns = new LinkedHashMap<DirectoryNameElements, Pattern>();
		int tSize = aFilePatternStrings.size();
		for (int tIndex = 0; tIndex < tSize;) {
			DirectoryNameElements tDirectoryNameElements = new DirectoryNameElements(aFilePatternStrings.get(tIndex++), aReplaceWords, tReplaceWordsQuoted);
			tFileNamePatterns.put(tDirectoryNameElements, Pattern.compile(tDirectoryNameElements.getCurrentContentName()));
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	public void filterSubmitNumber(List<FileStatus> aFileStatusList,
			Map<String, FileStatusWithVersion> aResultContainsFixNumberPath,
			List<FileStatus> aResultPath) {
		for (FileStatus tCurrentFileStatus : aFileStatusList) {
			filterSubmitNumber(tCurrentFileStatus, aResultContainsFixNumberPath, aResultPath);
		}
	}

	private void filterSubmitNumber(FileStatus aFileStatus,
			Map<String, FileStatusWithVersion> aResultContainsFixNumberPath,
			List<FileStatus> aResultPath) {
		String tName = aFileStatus.getPath().getName();
		for (Entry<DirectoryNameElements, Pattern> tCurrentFileNamePattern : mFileNamePatterns.entrySet()) {
			Matcher tMatcher = tCurrentFileNamePattern.getValue().matcher(tName);
			if (tMatcher.matches()) {
				DirectoryNameElements tCurrentDirectoryContents = tCurrentFileNamePattern.getKey();
				if (tCurrentDirectoryContents.isReplaced(PathConstants.SUBMIT_NUMBER)) {
					// HOSPITAL_CODE, yyyyMM, SUBMIT_NUMBER are reserved word.
					// HOSPITAL_CODE, yyyyMM, SUBMIT_NUMBER を扱えるように。
					LinkedHashMap<String, String> tIdentities = new LinkedHashMap<String, String>();
					int tIndex = 1;
					for (String tReplaceWord : tCurrentDirectoryContents.getReplaceWordsSequence()) {
						tIdentities.put(tReplaceWord, tMatcher.group(tIndex++));
					}
					// register aFileStatus of newest SUBMIT_NUMBER (key = "HOSPITAL_CODE + yyyyMM")
					// HOSPITAL_CODE + yyyyMM を鍵として、最も新しい SUBMIT_NUMBER の aFileStatus を登録
					String tStatusKey = tIdentities.get(PathConstants.HOSPITAL_CODE) + tIdentities.get(PathConstants.yyyyMM);
					// 既に tStatusKey が有れば、バージョン比較して追加
					FileStatusWithVersion tFileStatusVersion = aResultContainsFixNumberPath.get(tStatusKey);
					if (tFileStatusVersion == null) {
						aResultContainsFixNumberPath.put(tStatusKey, new FileStatusWithVersion(aFileStatus, tIdentities.get(PathConstants.SUBMIT_NUMBER)));
					} else {
						if (tFileStatusVersion.getVersion().compareTo(tIdentities.get(PathConstants.SUBMIT_NUMBER)) < 0) {
							aResultContainsFixNumberPath.put(tStatusKey, new FileStatusWithVersion(aFileStatus, tIdentities.get(PathConstants.SUBMIT_NUMBER)));
						}
					}
				} else
					aResultPath.add(aFileStatus);
			} else
				continue;
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

}
