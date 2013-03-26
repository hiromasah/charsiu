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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import jp.ac.u.tokyo.m.pig.udf.AliasConstants;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * (english)<br>
 * <br>
 * 渡された Tuple 中の複数の Bag への UDF 処理の記述をサポートします。 <br>
 */
public class MulticastEvaluate extends EvalFunc<Tuple> {

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * 「どんな処理」を「どのカラム」にするかという情報
	 */
	private final List<ReflectionUDFSetting> mReflectUDFSettings;

	/**
	 * 「カラム毎」に「どんな処理」をするかという情報
	 */
	private static List<ColumnEvaluationSetting> mColumnEvaluationSettings;

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * @param aArgs
	 *            aArgs are ('\<UDF\>', '\<bag column regex\>', '\<column control\>', '\<alias suffix\>'[, ... ])
	 */
	public MulticastEvaluate(String... aArgs) {
		super();
		int tArgLength = aArgs.length;
		if (tArgLength % 4 != 0)
			throw new IllegalArgumentException("引数の数が不正です : " + tArgLength);
		List<ReflectionUDFSetting> tReflectUDFSettings = mReflectUDFSettings = new ArrayList<ReflectionUDFSetting>();
		for (int i = 0; i < tArgLength; i++) {
			String tShortClassName = aArgs[i];
			String tClassName = MulticastEvaluationConstants.UDF_REFLECT_MAPPING.get(tShortClassName);
			// TODO 例外メッセージ（2行目に対応しているクラス一覧を表示。自動化。）
			if (tClassName == null)
				throw new IllegalArgumentException("unknown UDF : " + tShortClassName + "\n" +
						"MulticastEvaluate supported : MIN, MAX, SUM, AVG, SIZE, COUNT");
			tReflectUDFSettings.add(new ReflectionUDFSetting(tClassName, aArgs[++i], aArgs[++i], aArgs[++i]));
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Tuple exec(Tuple aInput) throws IOException {
		Tuple tTargetTuple = getTurgetTuple(aInput);

		Iterator<ColumnEvaluationSetting> ColumnEvaluationSettingsIterator = mColumnEvaluationSettings.iterator();
		ArrayList<Object> tProtoTuple = new ArrayList<Object>();
		for (Object tColumnValue : tTargetTuple.getAll()) {
			if (ColumnEvaluationSettingsIterator.hasNext()) {
				ColumnEvaluationSetting tSetting = ColumnEvaluationSettingsIterator.next();
				for (ColumnEvaluator tColumnEvaluator : tSetting.getColumnEvaluators()) {
					tProtoTuple.add(tColumnEvaluator.evaluate(tColumnValue));
				}
			}
		}
		return TupleFactory.getInstance().newTupleNoCopy(tProtoTuple);
	}

	/**
	 * aInput の構造が Tuple( ... ) または Tuple(Tuple( ... )) のどちらでも Tuple( ... ) を選択する。
	 * 
	 * @param aInput
	 * @return
	 */
	private Tuple getTurgetTuple(Tuple aInput) {
		Tuple tTargetTuple = aInput;
		if (aInput.size() == 1) {
			try {
				tTargetTuple = DataType.toTuple(aInput.get(0));
			} catch (ExecException ee) {}
		}
		return tTargetTuple;
	}

	// -----------------------------------------------------------------------------------------------------------------

	// TODO 可能なら実装する
	// @Override
	// public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
	// List<FuncSpec> tFuncList = new ArrayList<FuncSpec>();
	// Schema tSchema = new Schema();
	// tSchema.add(new Schema.FieldSchema(null, DataType.BAG));
	// tFuncList.add(new FuncSpec(this.getClass().getName(), tSchema));
	// return tFuncList;
	// }

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Schema outputSchema(Schema aInput) {
		// このカラムをどう変換するか、という情報にまとめる
		// インスタンスの使いまわしとかはリファクタ時に対応

		Schema tTargetSchema = getTargetSchema(aInput);
		Schema tInnerTupleSchema = new Schema();

		List<FieldSchema> tInputFields = tTargetSchema.getFields();
		List<ReflectionUDFSetting> tReflectUDFSettings = mReflectUDFSettings;
		// レコードの最上位カラム数と一致します。
		List<ColumnEvaluationSetting> tColumnEvaluationSettings = new ArrayList<ColumnEvaluationSetting>();
		// カラム毎のイテレーション
		for (FieldSchema tFieldSchema : tInputFields) {
			boolean tEvaluation = false;
			ColumnEvaluationSetting tSetting = new ColumnEvaluationSetting();
			// 評価方法毎のイテレーション
			for (ReflectionUDFSetting tReflectUDFSetting : tReflectUDFSettings) {
				String tCurrentFieldAlias = tFieldSchema.alias;
				if (tReflectUDFSetting.matchesColumnRegex(tCurrentFieldAlias)) {
					tEvaluation = true;
					try {
						DefaultColumnEvaluator tDefaultColumnEvaluator = new DefaultColumnEvaluator(
								new DefaultColumnAccessor(tReflectUDFSetting.getUDFArgumentFormat(), tFieldSchema),
								tReflectUDFSetting);
						tSetting.addColumnEvaluator(tDefaultColumnEvaluator);
						FieldSchema tCurrentOutputFieldSchema = tDefaultColumnEvaluator.getOutputSchema().getFields().get(0);
						tCurrentOutputFieldSchema.alias = tCurrentFieldAlias + "_" + tReflectUDFSetting.getAliasSuffix();
						tInnerTupleSchema.add(tCurrentOutputFieldSchema);
					} catch (Throwable e) {
						// TODO 例外処理
						throw new RuntimeException(e);
					}
					// System.out.println(tReflectUDFSetting + " // matches : " + tFieldSchema.alias);
				}
			}
			// 評価が設定されていないカラムはスルー評価器を設定する
			if (!tEvaluation) {
				// スルー評価器を設定
				// かつ。OutputSchema情報を確定できる
				tSetting.addColumnEvaluator(ThroughColumnEvaluator.INSTANCE);
				tInnerTupleSchema.add(tFieldSchema);
			}
			tColumnEvaluationSettings.add(tSetting);
		}
		mColumnEvaluationSettings = tColumnEvaluationSettings;

		Schema tOutputSchema = new Schema();
		try {
			tOutputSchema.add(
					new FieldSchema(AliasConstants.MULTICAST_EVALUATE_ALIAS_TOP,
							tInnerTupleSchema,
							DataType.TUPLE));
		} catch (FrontendException e) {
			// TODO 例外処理
			throw new RuntimeException(e);
		}
		return tOutputSchema;
	}

	/**
	 * aInput の構造が Tuple( ... ) または Tuple(Tuple( ... )) のどちらでも Tuple( ... ) を選択する。
	 * 
	 * @param aInput
	 * @return
	 */
	private Schema getTargetSchema(Schema aInput) {
		Schema tTargetSchema = aInput;
		try {
			if (aInput.size() == 1 && aInput.getField(0).type == DataType.TUPLE) {
				tTargetSchema = aInput.getField(0).schema;
			}
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}
		return tTargetSchema;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
