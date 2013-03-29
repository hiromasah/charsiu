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

package jp.ac.u.tokyo.m.dpc.specific;

public interface SpecificConstants {

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * key of the DPC data storage base directory <br>
	 * Hadoop の conf に設定する DPCデータ格納基底ディレクトリ の key <br>
	 */
	public static final String CONFIGURATION_KEY_DPC_DATA_DIRECTORY = "charsiu.dpc.data.dir";

	/**
	 * default value of the DPC data storage base directory <br>
	 * FS上のデフォルトの DPCデータ格納基底ディレクトリ <br>
	 */
	public static final String DPC_DATA_DIRECTORY_DEFAULT = "s3://dpcemr.data/dpc/data";

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * LoadDPC のロード対象指定文字列における 区切り文字
	 */
	public static final String LOAD_PATH_SEPARATOR = ",";

	/**
	 * LoadDPC のロード対象指定文字列における 範囲定義接続文字
	 */
	public static final String LOAD_PATH_RANGE_CONNECTOR = "-";

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * ファイルパス中の load-term の開始文字列
	 */
	public static final String FILES_REPLACE_WORD_OPENER = "\\[";
	/**
	 * ファイルパス中の load-term の終了文字列
	 */
	public static final String FILES_REPLACE_WORD_CLOSER = "\\]";

	// -----------------------------------------------------------------------------------------------------------------

}
