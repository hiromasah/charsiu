/*
 * Copyright 2012-2013 Hiromasa Horiguchi ( The University of Tokyo )
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

package jp.ac.u.tokyo.m.pig.udf.load;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import jp.ac.u.tokyo.m.ini.Ini;
import jp.ac.u.tokyo.m.pig.udf.DefinitionConstants;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LoadSchemaUtil {

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * This method is ".schema" file loader. <br>
	 * Comment line starts with "#" or ";". <br>
	 * Separator character of name and type is ":" <br>
	 * <br>
	 * .schema 形式のファイルに特化した load メソッド。<br>
	 * 結果は RowSchema として返す。<br>
	 * コメント行は # か ;（セミコロン） で始まるもの。<br>
	 * name と type の区切り文字は :（コロン）。<br>
	 * 
	 * @param aFileSystem
	 * @param aSchemaFilePath
	 * @param aEncoding
	 * @throws IOException
	 */
	public static RowSchema loadSchemaFile(FileSystem aFileSystem, Path aSchemaFilePath, String aEncoding) throws IOException {
		ArrayList<ColumnSchema> tResultColumnSchemaList = new ArrayList<ColumnSchema>();
		InputStream tResourceInputStream = null;
		BufferedReader tResourceBufferedReader = null;
		tResourceInputStream = aFileSystem.open(aSchemaFilePath);
		try {
			tResourceBufferedReader = new BufferedReader(new InputStreamReader(tResourceInputStream, aEncoding));
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
					tResultColumnSchemaList.add(new ColumnSchema(tSchemaTypeName[0].trim(), tSchemaTypeName[1].trim()));
				} catch (Exception e) {
					throw new RuntimeException(aSchemaFilePath + " にスキーマの設定不備が有ります。該当行 : " + tTrimedLine, e);
				}
			}
		} finally {
			if (tResourceBufferedReader != null)
				tResourceBufferedReader.close();
		}
		return new RowSchema(tResultColumnSchemaList);
	}

	// -----------------------------------------------------------------------------------------------------------------

}
