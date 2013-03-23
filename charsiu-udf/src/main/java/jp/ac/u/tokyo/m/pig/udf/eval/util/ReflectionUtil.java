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

import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class ReflectionUtil {

	@SuppressWarnings({ "unchecked" })
	public static <EvalFunc> Class<EvalFunc> getClassForName(String className) {
		try {
			return (Class<EvalFunc>) Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public static void removeAlias(Schema aTarget) {
		List<FieldSchema> tFields = aTarget.getFields();
		for (FieldSchema tFieldSchema : tFields) {
			tFieldSchema.alias = null;
			switch (tFieldSchema.type) {
			case DataType.BAG:
				removeAlias(tFieldSchema.schema);
				break;
			case DataType.TUPLE:
				removeAlias(tFieldSchema.schema);
				break;
			default:
				break;
			}
		}
	}

	public static void removeTupleInBag(Schema aTarget) {
		List<FieldSchema> tFields = aTarget.getFields();
		for (FieldSchema tFieldSchema : tFields) {
			switch (tFieldSchema.type) {
			case DataType.BAG:
				FieldSchema tElementInBag = tFieldSchema.schema.getFields().get(0);
				if (tElementInBag.type == DataType.TUPLE) {
					Schema tSchema = new Schema();
					for (FieldSchema tTupleField : tElementInBag.schema.getFields()) {
						tSchema.add(tTupleField);
					}
					tFieldSchema.schema = tSchema;
				} else
					removeAlias(tFieldSchema.schema);
				break;
			default:
				break;
			}
		}
	}

}
