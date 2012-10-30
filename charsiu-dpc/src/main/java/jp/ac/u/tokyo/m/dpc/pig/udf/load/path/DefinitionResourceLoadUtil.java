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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping.DPCColumnSchema;
import jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping.DPCSchema;
import jp.ac.u.tokyo.m.ini.Ini;
import jp.ac.u.tokyo.m.pig.udf.DefinitionConstants;
import jp.ac.u.tokyo.m.string.StringFormatConstants;

public class DefinitionResourceLoadUtil {

	// -----------------------------------------------------------------------------------------------------------------

	private static DefinitionResourceLoadUtil mInstance = new DefinitionResourceLoadUtil();

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * This method reads a resource file. This method return available line list that excluded a comment line and an empty line.<br>
	 * <br>
	 * jar 中のリソースファイルを読み込んで、コメント行と空行を除外した有効行だけをリストに納めて返す。<br>
	 */
	public static List<String> loadResource(String aResourcePath) throws IOException {
		ArrayList<String> tResult = new ArrayList<String>();
		BufferedReader tResourceBufferedReader = null;
		InputStream tResourceAsStream = mInstance.getClass().getClassLoader().getResourceAsStream(aResourcePath);
		if (tResourceAsStream == null)
			throw new FileNotFoundException(aResourcePath);
		try {
			tResourceBufferedReader = new BufferedReader(
					new InputStreamReader(tResourceAsStream, StringFormatConstants.TEXT_FORMAT_UTF8));
			String tCommentLineStartSemicolon = Ini.COMMENT_LINE_START_SEMICOLON;
			String tCommentLineStartSharp = Ini.COMMENT_LINE_START_SHARP;
			while (true) {
				String tLine = tResourceBufferedReader.readLine();
				if (tLine == null) {
					break;
				}
				String tTrimedLine = tLine.trim();
				if (tTrimedLine.length() == 0
						|| tTrimedLine.startsWith(tCommentLineStartSemicolon)
						|| tTrimedLine.startsWith(tCommentLineStartSharp)) {
					continue;
				}
				tResult.add(tTrimedLine);
			}
		} finally {
			if (tResourceBufferedReader != null)
				tResourceBufferedReader.close();
		}
		return tResult;
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * This method is ".schema" file loader. <br>
	 * Comment line starts with "#" or ";". <br>
	 * Separator character of name and type is ":" <br>
	 * <br>
	 * .schema 形式のファイルに特化した load メソッド。<br>
	 * 結果は DPCSchema として返す。<br>
	 * コメント行は # か ;（セミコロン） で始まるもの。<br>
	 * リソースは jar 中に納められていること。<br>
	 * name と type の区切り文字は :（コロン）。<br>
	 */
	public static DPCSchema loadSchemaResource(String aResourcePath) throws IOException {
		ArrayList<DPCColumnSchema> tResultColumnSchemaList = new ArrayList<DPCColumnSchema>();
		InputStream tResourceInputStream = null;
		BufferedReader tResourceBufferedReader = null;
		tResourceInputStream = mInstance.getClass().getClassLoader().getResourceAsStream(aResourcePath);
		if (tResourceInputStream == null)
			throw new FileNotFoundException(aResourcePath);
		try {
			tResourceBufferedReader = new BufferedReader(new InputStreamReader(tResourceInputStream, StringFormatConstants.TEXT_FORMAT_UTF8));
			while (true) {
				String tLine = tResourceBufferedReader.readLine();
				if (tLine == null) {
					break;
				}
				String tTrimedLine = tLine.trim();
				if (tTrimedLine.length() == 0
						|| tTrimedLine.startsWith(Ini.COMMENT_LINE_START_SEMICOLON)
						|| tTrimedLine.startsWith(Ini.COMMENT_LINE_START_SHARP)) {
					continue;
				}
				try {
					String[] tSchemaTypeName = tTrimedLine.split(DefinitionConstants.DEFINITION_SCHEMA_ALIAS_TYPE_SEPARATOR);
					tResultColumnSchemaList.add(new DPCColumnSchema(tSchemaTypeName[0].trim(), tSchemaTypeName[1].trim()));
				} catch (Exception e) {
					throw new RuntimeException(aResourcePath + " にスキーマの設定不備が有ります。該当行 : " + tTrimedLine, e);
				}
			}
		} finally {
			if (tResourceBufferedReader != null)
				tResourceBufferedReader.close();
		}
		return new DPCSchema(tResultColumnSchemaList);
	}

	// -----------------------------------------------------------------------------------------------------------------

}
