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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import jp.ac.u.tokyo.m.string.StringUtil;

import org.apache.hadoop.fs.Path;

/**
 * 1 instance is in charge of 1 File/Directory. <br>
 * This class structure "child -> parent ...". <br>
 * <br>
 * 1インスタンスは 1 File/Directory を担当し、子 -> 親 ... の数珠繋ぎ構造を持ちます。 <br>
 */
public class DirectoryNameElements {
	private final DirectoryNameElements mParentDirectory;
	private final String mCurrentContentNameOriginal;
	private final String mCurrentContentName;
	/**
	 * appearance order of replace-word | replace-word の出現順序
	 */
	private final List<String> mReplaceWordsSequence;

	public List<String> getReplaceWordsSequence() {
		return mReplaceWordsSequence;
	}

	public DirectoryNameElements(String aFileNameAndParentDirectoriesString, LinkedHashMap<String, String> aReplaceWordsOriginal, LinkedHashMap<String, String> aReplaceWordsQuoted) {
		mReplaceWordsSequence = new ArrayList<String>();
		int tIndexOfLastDirectoryEnd = aFileNameAndParentDirectoriesString.lastIndexOf(Path.SEPARATOR);
		if (tIndexOfLastDirectoryEnd == -1) {
			mCurrentContentNameOriginal = aFileNameAndParentDirectoriesString;
			setReplaceWordSequence(aFileNameAndParentDirectoriesString, aReplaceWordsOriginal, mReplaceWordsSequence);
			mCurrentContentName = StringUtil.replaceWords(aFileNameAndParentDirectoriesString, aReplaceWordsQuoted);
			mParentDirectory = null;
		} else {
			String tCurrentContentNameOriginal = aFileNameAndParentDirectoriesString.substring(tIndexOfLastDirectoryEnd + 1);
			mCurrentContentNameOriginal = tCurrentContentNameOriginal;
			setReplaceWordSequence(tCurrentContentNameOriginal, aReplaceWordsOriginal, mReplaceWordsSequence);
			mCurrentContentName = StringUtil.replaceWords(
					tCurrentContentNameOriginal,
					aReplaceWordsQuoted);
			mParentDirectory = new DirectoryNameElements(aFileNameAndParentDirectoriesString.substring(0, tIndexOfLastDirectoryEnd), aReplaceWordsOriginal, aReplaceWordsQuoted);
		}
	}

	public void setReplaceWordSequence(String aTarget,
			LinkedHashMap<String, String> aReplaceWords,
			List<String> aResultReplaceWordsSequence) {
		// record the appearance order of aReplaceWords | aReplaceWords の出現順序を保存
		TreeMap<Integer, String> tReplaceWordIndexes = new TreeMap<Integer, String>();
		for (Entry<String, String> tCurrentReplaceWord : aReplaceWords.entrySet()) {
			String tKey = tCurrentReplaceWord.getKey();
			int tCurrentReplaceWordIndex = aTarget.indexOf(tKey);
			if (tCurrentReplaceWordIndex >= 0) {
				tReplaceWordIndexes.put(tCurrentReplaceWordIndex, tKey);
			}
		}
		for (Entry<Integer, String> tCurrentReplaceWord : tReplaceWordIndexes.entrySet()) {
			aResultReplaceWordsSequence.add(tCurrentReplaceWord.getValue());
		}
	}

	public String getCurrentContentNameOriginal() {
		return mCurrentContentNameOriginal;
	}

	public String getCurrentContentName() {
		return mCurrentContentName;
	}

	public boolean hasParentDirectory() {
		return mParentDirectory != null;
	}

	public DirectoryNameElements getparentDirectory() {
		return mParentDirectory;
	}

	public boolean isReplaced(String aTarget) {
		return mReplaceWordsSequence.contains(aTarget);
	}

	@Override
	public boolean equals(Object aTarget) {
		if (aTarget instanceof DirectoryNameElements) {
			return mCurrentContentNameOriginal.equals(((DirectoryNameElements) aTarget).getCurrentContentNameOriginal());
		} else
			return false;
	}
}
