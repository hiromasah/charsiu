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

import java.io.IOException;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class DefaultColumnEvaluator implements ColumnEvaluator {

	private final ReflectionUDFSetting mReflectUDFSetting;
	private final ColumnAccessor mColumnAccessor;
	private final EvalFunc<?> mUDF;

	public DefaultColumnEvaluator(ColumnAccessor aColumnAccessor, ReflectionUDFSetting aReflectUDFSetting) throws InstantiationException, IllegalAccessException, FrontendException, CloneNotSupportedException {
		mColumnAccessor = aColumnAccessor;
		mReflectUDFSetting = aReflectUDFSetting;

		Class<EvalFunc<?>> tMasterUDFClass = ReflectionUtil.getClassForName(aReflectUDFSetting.getmClassName());
		EvalFunc<?> tMasterUDF = tMasterUDFClass.newInstance();
		EvalFunc<?> tUDF = null;
		// func-spec = null なら無視していい
		// 拒否もしたい
		// この方法だとPigクエリ的な構文補助機能が使えない（動かして初めてミスに気づく。これは地味にネック。スキーマ検証段階で処理されるためかろうじで大丈夫か。）
		Schema tInputSchema = aColumnAccessor.getInputSchema().clone();
		ReflectionUtil.removeAlias(tInputSchema);
		List<FuncSpec> tArgToFuncMapping = tMasterUDF.getArgToFuncMapping();
		if (tArgToFuncMapping == null) {
			tUDF = tMasterUDF;
		} else {
			for (FuncSpec tFuncSpec : tArgToFuncMapping) {
				// XXX Pig はどうやって FuncMapping を利用しているんだろう？
				// if (Schema.equals(tFuncSpec.getInputArgsSchema(), tInputSchema, false, false)) {
				if (tFuncSpec.getInputArgsSchema().equals(tInputSchema)) {
					Class<EvalFunc<?>> tUDFClass = ReflectionUtil.getClassForName(tFuncSpec.getClassName());
					tUDF = tUDFClass.newInstance();
					break;
				}
			}
			// 1周でダメなら {()} が問題の可能性が有るので、スキーマを調整してもう一度
			if (tUDF == null) {
				ReflectionUtil.removeTupleInBag(tInputSchema);
				for (FuncSpec tFuncSpec : tArgToFuncMapping) {
					if (tFuncSpec.getInputArgsSchema().equals(tInputSchema)) {
						Class<EvalFunc<?>> tUDFClass = ReflectionUtil.getClassForName(tFuncSpec.getClassName());
						tUDF = tUDFClass.newInstance();
						break;
					}
				}
			}
				
			// TODO 例外メッセージ（UDF の FuncMapping に InputSchema が登録されていない）
			if (tUDF == null)
				throw new IllegalArgumentException();
		}
		mUDF = tUDF;
	}

	public ReflectionUDFSetting getReflectUDFSetting() {
		return mReflectUDFSetting;
	}

	@Override
	public Object evaluate(Object aInput) throws IOException {
		return mUDF.exec(mColumnAccessor.generate(aInput));
	}

	// TODO 出力スキーマ操作（UDF名とか付けて返せるはず）
	public Schema getOutputSchema() {
		return mUDF.outputSchema(mColumnAccessor.getInputSchema());
	}

}
