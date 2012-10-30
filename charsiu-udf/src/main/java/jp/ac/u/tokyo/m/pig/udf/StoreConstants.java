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

package jp.ac.u.tokyo.m.pig.udf;

public interface StoreConstants {

	// -----------------------------------------------------------------------------------------------------------------

	public static final String STORE_DEFAULT_COLUMN_NAME = "COLUMN_";
	public static final String STORE_DELIMITER_FIELD = "\t";
	public static final String STORE_DELIMITER_RECORD = "\n";
	public static final String STORE_SCHEMA_ALIAS_TYPE_SEPARATOR = ":";
	public static final String STORE_FILE_NAME_HEADER = ".header";
	public static final String STORE_FILE_NAME_SCHEMA = ".schema";

	// -----------------------------------------------------------------------------------------------------------------

}
