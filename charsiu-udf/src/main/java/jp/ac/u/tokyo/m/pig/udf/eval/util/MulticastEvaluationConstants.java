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

package jp.ac.u.tokyo.m.pig.udf.eval.util;

import java.util.HashMap;

public class MulticastEvaluationConstants {

	public static enum AccessType {
		FLAT,
		SUB_BAG
	}
	static final HashMap<String, String> UDF_REFLECT_MAPPING = new HashMap<String, String>();
	static {
		// Pig built in Functions
		UDF_REFLECT_MAPPING.put("MIN", "org.apache.pig.builtin.MIN");
		UDF_REFLECT_MAPPING.put("MAX", "org.apache.pig.builtin.MAX");
		UDF_REFLECT_MAPPING.put("SUM", "org.apache.pig.builtin.SUM");
		UDF_REFLECT_MAPPING.put("AVG", "org.apache.pig.builtin.AVG");
		UDF_REFLECT_MAPPING.put("SIZE", "org.apache.pig.builtin.SIZE");
		UDF_REFLECT_MAPPING.put("COUNT", "org.apache.pig.builtin.COUNT");

		// charsiu functions
//		UDF_REFLECT_MAPPING.put("TupleFirst", "jp.ac.u.tokyo.m.pig.udf.eval.math.TupleFirst");
//		UDF_REFLECT_MAPPING.put("TupleMax", "jp.ac.u.tokyo.m.pig.udf.eval.math.TupleMax");
//		UDF_REFLECT_MAPPING.put("TupleMin", "jp.ac.u.tokyo.m.pig.udf.eval.math.TupleMin");
	}

	public static final String REFLECTION_UDF_PARAMETERS_ROOT_COLUMN = "_";
	public static final String REFLECTION_UDF_PARAMETERS_ACCESSOR = "\\.";
	public static final String REFLECTION_UDF_PARAMETERS_SEPARATOR = ",";
	
	public static final String STRINGABLE_COLUMN_INDEX_INFORMATION_HEAD_MARKER_REGEX = "\\*";
	public static final String STRINGABLE_COLUMN_INDEX_INFORMATION_HEAD_MARKER = "*";
	public static final String STRINGABLE_COLUMN_INDEX_INFORMATION_DELIMITER = ",";
	public static final String STRINGABLE_COLUMN_ACCESSOR_DELIMITER = "#";
	public static final String STRINGABLE_COLUMN_EVALUATOR_DELIMITER = "@";
	public static final String STRINGABLE_LIST_COLUMN_EVALUATOR_DELIMITER = "&";
	public static final String STRINGABLE_LIST_COLUMN_EVALUATION_SETTING_DELIMITER = "%";

}
