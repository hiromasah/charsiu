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

package jp.ac.u.tokyo.m.ini;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.LinkedHashMap;

/**
 * This class expresses INI including the Section. <br>
 * This class has the following specifications. <br>
 * * ";" separator is unsupported. <br>
 * * line comment after "key = value" is unsupported. <br>
 * * When a section is not defined, DEFAULT_SECTION_NAME becomes the section name. <br>
 * * The section of the same name does not assume it. <br>
 * <br>
 * Section を含む INI を表現します。 <br>
 * 以下のような仕様があります。 <br>
 * ・":" で key value を区切るのはサポート外。 <br>
 * ・key = value の後に1行コメントを書くのはサポート外。 <br>
 * ・セクションが定義されていない場合は DEFAULT_SECTION_NAME がセクション名になる。 <br>
 * ・同じ名前のセクションは想定しない。 <br>
 */
public class Ini {

	// -----------------------------------------------------------------------------------------------------------------

	public static final String COMMENT_LINE_START_SHARP = "#";
	public static final String COMMENT_LINE_START_SEMICOLON = ";";

	public static final String PARAMETER_SEPARATOR = "=";

	public static final String SECTION_NAME_OPENER = "[";
	public static final String SECTION_NAME_CLOSER = "]";

	public static final String DEFAULT_SECTION_NAME = "default";

	// -----------------------------------------------------------------------------------------------------------------

	private LinkedHashMap<String, Section> mSections = new LinkedHashMap<String, Section>();

	private String mCurrentSectionName = DEFAULT_SECTION_NAME;

	// -----------------------------------------------------------------------------------------------------------------

	public Ini() {}

	public Ini(InputStream aInputStream) throws IOException {
		load(aInputStream);
	}

	public Ini(Reader aReader) throws IOException {
		load(aReader);
	}

	// -----------------------------------------------------------------------------------------------------------------

	public synchronized void load(Reader aReader) throws IOException {
		loadFile(new BufferedReader(aReader));
	}

	public synchronized void load(InputStream aInputStream) throws IOException {
		loadFile(new BufferedReader(new InputStreamReader(aInputStream, "UTF-8")));
	}

	private void loadFile(BufferedReader aLineReader) throws IOException {
		try {
			LinkedHashMap<String, Section> tSections = mSections;
			tSections.clear();

			String tSectionName = null;
			LinkedHashMap<String, String> tParameters = new LinkedHashMap<String, String>();

			while (true) {
				String tLine = aLineReader.readLine();
				// termination | 終端
				if (tLine == null) {
					// add last section | 最後のセクションを追加
					if (tSectionName != null) {
						tSections.put(tSectionName, new Section(tSectionName, tParameters));
					}
					break;
				}

				String tTrimedLine = tLine.trim();
				// empty or comment line | 空行、コメント行
				if (tTrimedLine.length() == 0
						|| tTrimedLine.startsWith(COMMENT_LINE_START_SHARP)
						|| tTrimedLine.startsWith(COMMENT_LINE_START_SEMICOLON)) {
					continue;
				}

				// before found section or default section | セクション検出前、またはデフォルトセクション
				if (tSectionName == null) {
					// found section | セクション名が定義されていた場合
					if (tTrimedLine.startsWith(SECTION_NAME_OPENER)) {
						tSectionName = parseSectionName(tTrimedLine);
						continue;
					}
					// not found section | セクション名が出現していないのに値が定義されていた場合
					else {
						// set default section name | セクション名をデフォルト値に
						tSectionName = DEFAULT_SECTION_NAME;
						tParameters = new LinkedHashMap<String, String>();
						addParameter(tParameters, tTrimedLine);
						continue;
					}
				}
				// in tSectionName section or found next section | tSectionName のセクション内、もしくは次セクション検出時
				else {
					// if defined section, start next section and instantiation current section. | セクション名が定義されていた場合、次セクション開始、現セクションを Section インスタンス化
					if (tTrimedLine.startsWith(SECTION_NAME_OPENER)) {
						// instantiation current section | 現セクションを Section インスタンスに
						tSections.put(tSectionName, new Section(tSectionName, tParameters));
						// start next section | 次セクション
						tSectionName = parseSectionName(tTrimedLine);
						tParameters = new LinkedHashMap<String, String>();
						continue;
					}
					// if defined key value | 値が定義されていた場合
					else {
						addParameter(tParameters, tTrimedLine);
						continue;
					}
				}
			}
		} finally {
			if (aLineReader != null)
				aLineReader.close();
		}
	}

	private String parseSectionName(String aTarget) {
		int tOpenerIndex = aTarget.indexOf(SECTION_NAME_OPENER);
		int tCloserIndex = aTarget.lastIndexOf(SECTION_NAME_CLOSER);
		return aTarget.substring(tOpenerIndex + 1, tCloserIndex).trim();
	}

	private void addParameter(LinkedHashMap<String, String> aParametersOut, String aTarget) {
		int tSeparetorIndex = aTarget.indexOf(PARAMETER_SEPARATOR);
		String tKey = aTarget.substring(0, tSeparetorIndex).trim();
		String tValue = aTarget.substring(tSeparetorIndex + 1).trim();
		aParametersOut.put(tKey, tValue);
	}

	// -----------------------------------------------------------------------------------------------------------------

	public void setCurrentSection(String aSectionName) {
		mCurrentSectionName = aSectionName;
	}

	public String getCurrentSectionName() {
		return mCurrentSectionName;
	}

	// -----------------------------------------------------------------------------------------------------------------

	public LinkedHashMap<String, Section> getSections() {
		return mSections;
	}

	public Section getSection(String aSectionName) {
		return mSections.get(aSectionName);
	}

	public int getSectionCount() {
		return mSections.size();
	}

	public String getParameter(String aSectionName, String aKey) {
		Section tSection = mSections.get(aSectionName);
		if (tSection == null)
			return null;
		else
			return tSection.getParameter(aKey);
	}

	public String getParameter(String aSectionName, String aKey, String aDefaultValue) {
		String tParameter = getParameter(aSectionName, aKey);
		return tParameter == null ? aDefaultValue : tParameter;
	}

	public String getCurrentSectionParameter(String aKey) {
		Section tSection = mSections.get(mCurrentSectionName);
		if (tSection == null)
			return null;
		else
			return tSection.getParameter(aKey);
	}

	public String getCurrentSectionParameter(String aKey, String aDefaultValue) {
		String tParameter = getCurrentSectionParameter(aKey);
		return tParameter == null ? aDefaultValue : tParameter;
	}

	// -----------------------------------------------------------------------------------------------------------------

	public class Section {
		private final String mSectionName;
		private final LinkedHashMap<String, String> mMembers;

		public Section(String aSectionName, LinkedHashMap<String, String> aMembers) {
			mSectionName = aSectionName;
			mMembers = aMembers;
		}

		public String getSectionName() {
			return mSectionName;
		}

		public LinkedHashMap<String, String> getMembers() {
			return mMembers;
		}

		public boolean isSectionName(String aSectionName) {
			return mSectionName.equals(aSectionName);
		}

		public String getParameter(String aKey) {
			return mMembers.get(aKey);
		}

		public int getParameterCount() {
			return mMembers.size();
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

}
