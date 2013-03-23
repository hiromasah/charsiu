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

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * (english)<br>
 * <br>
 * 渡された Tuple 中の複数の Bag への UDF 処理の記述をサポートします。 <br>
 */
public class MulticastEvaluate extends EvalFunc<Tuple> {

	// -----------------------------------------------------------------------------------------------------------------

	private final List<ReflectionUDFSetting> mReflectUDFSettings;
	private static List<ColumnEvaluationSetting> mColumnEvaluationSettings;

	// -----------------------------------------------------------------------------------------------------------------

	// TODO args : ('<UDF>', '<bag column regex>', '<column control>', '<alias suffix>')
	public MulticastEvaluate(String... aArgs) {
		super();
		int tArgLength = aArgs.length;
		// TODO 例外メッセージ
		if (tArgLength % 3 != 0)
			throw new IllegalArgumentException();
		List<ReflectionUDFSetting> tReflectUDFSettings = mReflectUDFSettings = new ArrayList<ReflectionUDFSetting>();
		for (int i = 0; i < tArgLength; i++) {
			String tShortClassName = aArgs[i];
			String tClassName = MulticastEvaluationConstants.UDF_REFLECT_MAPPING.get(tShortClassName);
			// TODO 例外メッセージ（2行目に対応しているクラス一覧を表示）
			if (tClassName == null)
				throw new IllegalArgumentException("unknown UDF : " + tShortClassName);
			tReflectUDFSettings.add(new ReflectionUDFSetting(tClassName, aArgs[++i], aArgs[++i]));
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	// TODO 構造 (*) と ((*)) の両対応。外部でタプルを渡したい場合も有る。InnerGroup後などが該当する。
	@Override
	public Tuple exec(Tuple aInput) throws IOException {
		Iterator<ColumnEvaluationSetting> ColumnEvaluationSettingsIterator = mColumnEvaluationSettings.iterator();
		ArrayList<Object> tProtoTuple = new ArrayList<Object>();
		for (Object tColumnValue : aInput.getAll()) {
			if(ColumnEvaluationSettingsIterator.hasNext()){
				ColumnEvaluationSetting tSetting = ColumnEvaluationSettingsIterator.next();
				for (ColumnEvaluator tColumnEvaluator : tSetting.getColumnEvaluators()) {
					tProtoTuple.add(tColumnEvaluator.evaluate(tColumnValue));
				}
			}
		}
		return TupleFactory.getInstance().newTupleNoCopy(tProtoTuple);
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

	// TODO ここで「対象のカラム」「対象のカラムの型」を判断し、「対応するUDF」を特定する
	// TODO alias 付与
	// Tuple( ... ) => Tuple( ... )
	@Override
	public Schema outputSchema(Schema aInput) {
		// このカラムをどう変換するか、という情報にまとめるのが良い
		// MAX でも int, double とかあるかもだし
		// インスタンスの使いまわしとかはリファクタ時に対応

		// カラムへのアクセス情報 FLAT, SUB_BAG
		// ColumnAccesser : Object -> Tuple

		Schema tOutputSchema = new Schema();

		List<FieldSchema> tInputFields = aInput.getFields();
		List<ReflectionUDFSetting> tReflectUDFSettings = mReflectUDFSettings;
		// レコードの最上位カラム数と一致します。
		List<ColumnEvaluationSetting> tColumnEvaluationSettings = new ArrayList<ColumnEvaluationSetting>();
		// カラム毎のイテレーション
		for (FieldSchema tFieldSchema : tInputFields) {
			boolean tEvaluation = false;
			ColumnEvaluationSetting tSetting = new ColumnEvaluationSetting();
			// 評価方法毎のイテレーション
			for (ReflectionUDFSetting tReflectUDFSetting : tReflectUDFSettings) {
				if (tReflectUDFSetting.matchesColumnRegex(tFieldSchema.alias)) {
					tEvaluation = true;
					try {
						DefaultColumnEvaluator tDefaultColumnEvaluator = new DefaultColumnEvaluator(
								new DefaultColumnAccessor(tReflectUDFSetting.getmUDFArgumentFormat(), tFieldSchema),
								tReflectUDFSetting);
						tSetting.addColumnEvaluator(tDefaultColumnEvaluator);
						tOutputSchema.add(tDefaultColumnEvaluator.getOutputSchema().getFields().get(0));
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
				tOutputSchema.add(tFieldSchema);
			}
			tColumnEvaluationSettings.add(tSetting);
		}
		mColumnEvaluationSettings = tColumnEvaluationSettings;

		return tOutputSchema;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
