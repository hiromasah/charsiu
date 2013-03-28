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

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class ReflectionUtil {

	// -----------------------------------------------------------------------------------------------------------------

	@SuppressWarnings({ "unchecked", "hiding" })
	public static <EvalFunc> Class<EvalFunc> getClassForName(String className) throws ClassNotFoundException {
		return (Class<EvalFunc>) Class.forName(className);
	}

	// -----------------------------------------------------------------------------------------------------------------

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

	// -----------------------------------------------------------------------------------------------------------------

	public static EvalFunc<?> getUDFInstance(String aUDFClassName, Schema aInputSchema) throws InstantiationException, IllegalAccessException, CloneNotSupportedException, FrontendException, ClassNotFoundException {
		Class<EvalFunc<?>> tMasterUDFClass = getClassForName(aUDFClassName);
		EvalFunc<?> tMasterUDF = tMasterUDFClass.newInstance();
		EvalFunc<?> tUDF = null;
		Schema tInputSchema = aInputSchema.clone();
		removeAlias(tInputSchema);
		List<FuncSpec> tArgToFuncMapping = tMasterUDF.getArgToFuncMapping();
		if (tArgToFuncMapping == null) {
			tUDF = tMasterUDF;
		} else {
			for (FuncSpec tFuncSpec : tArgToFuncMapping) {
				if (tFuncSpec.getInputArgsSchema().equals(tInputSchema)) {
					Class<EvalFunc<?>> tUDFClass = getClassForName(tFuncSpec.getClassName());
					tUDF = tUDFClass.newInstance();
					break;
				}
			}
			// 1周で合致しないまら {()} が問題の可能性が有るので、スキーマを調整してもう一度
			if (tUDF == null) {
				removeTupleInBag(tInputSchema);
				for (FuncSpec tFuncSpec : tArgToFuncMapping) {
					if (tFuncSpec.getInputArgsSchema().equals(tInputSchema)) {
						Class<EvalFunc<?>> tUDFClass = getClassForName(tFuncSpec.getClassName());
						tUDF = tUDFClass.newInstance();
						break;
					}
				}
			}
			// XXX 単一の引数をとる UDF しか想定していない実装。 TupleMax などに対応するなら複数引数を考慮する実装にする必要あり。
			// tFuncSpec.getInputArgsSchema() == {()} の場合
			if (tUDF == null) {
				for (FuncSpec tFuncSpec : tArgToFuncMapping) {
					if (tFuncSpec.getInputArgsSchema().getField(0).type == DataType.BAG) {
						if (tFuncSpec.getInputArgsSchema().getField(0).schema == null
								|| (tFuncSpec.getInputArgsSchema().getField(0).schema.getField(0).type == DataType.TUPLE
								&& tFuncSpec.getInputArgsSchema().getField(0).schema.getField(0).schema.size() == 0)) {
							// empty Bag schema : {()}
							if (Schema.equals(tFuncSpec.getInputArgsSchema(), tInputSchema, true, true)) {
								Class<EvalFunc<?>> tUDFClass = getClassForName(tFuncSpec.getClassName());
								tUDF = tUDFClass.newInstance();
								break;
							}
						}
					}
				}
			}

			if (tUDF == null)
				throw new IllegalArgumentException(aUDFClassName + " undefined input schema of " + aInputSchema);
		}
		return tUDF;
	}

	// -----------------------------------------------------------------------------------------------------------------

	public static String getSupportedUDFs(){
		StringBuilder tResultBuilder = new StringBuilder();
		for (String tElement : MulticastEvaluationConstants.UDF_REFLECT_MAPPING.keySet()) {
			tResultBuilder.append(tElement);
			tResultBuilder.append(", ");
		}
		return tResultBuilder.toString();
	}
	
	// -----------------------------------------------------------------------------------------------------------------

}
