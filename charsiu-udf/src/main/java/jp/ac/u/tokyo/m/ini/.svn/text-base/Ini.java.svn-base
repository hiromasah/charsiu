package jp.ac.u.tokyo.m.ini;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.LinkedHashMap;

/**
 * Section を含む INI を表現します。
 * 以下のような仕様があります。
 * ・":" で key value を区切るのはサポート外。
 * ・key = value の後に1行コメントを書くのはサポート外。
 * ・セクションが定義されていない場合は DEFAULT_SECTION_NAME がセクション名になる。
 * ・同じ名前のセクションは想定しない。
 */
public class Ini {

	// -----------------------------------------------------------------------------------------------------------------

	public static String COMMENT_LINE_START_SHARP = "#";
	public static String COMMENT_LINE_START_SEMICOLON = ";";

	public static String PARAMETER_SEPARATOR = "=";

	public static String SECTION_NAME_OPENER = "[";
	public static String SECTION_NAME_CLOSER = "]";

	public static String DEFAULT_SECTION_NAME = "default";

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
				// 終端
				if (tLine == null) {
					// 最後のセクションを追加
					if (tSectionName != null) {
						tSections.put(tSectionName, new Section(tSectionName, tParameters));
					}
					break;
				}

				String tTrimedLine = tLine.trim();
				// 空行、コメント行
				if (tTrimedLine.length() == 0
						|| tTrimedLine.startsWith(COMMENT_LINE_START_SHARP)
						|| tTrimedLine.startsWith(COMMENT_LINE_START_SEMICOLON)) {
					continue;
				}

				// セクション検出前、またはデフォルトセクション
				if (tSectionName == null) {
					// セクション名が定義されていた場合
					if (tTrimedLine.startsWith(SECTION_NAME_OPENER)) {
						tSectionName = parseSectionName(tTrimedLine);
						continue;
					}
					// セクション名が出現していないのに値が定義されていた場合
					else {
						// セクション名をデフォルト値に
						tSectionName = DEFAULT_SECTION_NAME;
						tParameters = new LinkedHashMap<String, String>();
						addParameter(tParameters, tTrimedLine);
						continue;
					}
				}
				// tSectionName のセクション内、もしくは次セクション検出時
				else {
					// セクション名が定義されていた場合、次セクション開始、現セクションを Section インスタンス化
					if (tTrimedLine.startsWith(SECTION_NAME_OPENER)) {
						// 現セクションを Section インスタンスに
						tSections.put(tSectionName, new Section(tSectionName, tParameters));
						// 次セクション
						tSectionName = parseSectionName(tTrimedLine);
						tParameters = new LinkedHashMap<String, String>();
						continue;
					}
					// 値が定義されていた場合
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
