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

package jp.ac.u.tokyo.m.pig.udf.eval.group;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

import jp.ac.u.tokyo.m.log.LogUtil;
import jp.ac.u.tokyo.m.pig.udf.AliasConstants;
import jp.ac.u.tokyo.m.pig.udf.FormatConstants;
import jp.ac.u.tokyo.m.pig.udf.format.GroupFilterFormat;
import jp.ac.u.tokyo.m.pig.udf.format.GroupFilterFormatGroup;
import jp.ac.u.tokyo.m.pig.udf.format.StockGroupFilterFormatFactory;
import jp.ac.u.tokyo.m.string.StringUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;

/**
 * Groups the Bag in accordance with the description in GroupFilterFormat.<br>
 * 
 * Bag をグルーピングします。<br>
 * グルーピングには GroupFilterFormat を利用します。<br>
 */
public class InnerGroup extends EvalFunc<Tuple> {

	// -----------------------------------------------------------------------------------------------------------------

	private static Log mLog = LogFactory.getLog(InnerGroup.class);

	// -----------------------------------------------------------------------------------------------------------------

	private GroupFilterFormat mGroupFilterFormat = null;
	private ArrayList<String> mTargetJoinedValueBagNames;

	private String mKeyOriginalColumnNum;
	private int mOriginalColumnNum;
	private String mKeyJoinedValueBagIndexes;
	private int[] mJoinedValueBagIndexes;
	private String mKeyJoinedValueBagSizes;
	private int[] mJoinedValueBagSizes;

	private ModeOfTakeOutJoinedValue mModeOfTakeOutJoinedValue;

	private enum ModeOfTakeOutJoinedValue {
		OWN_GROUP,
		OWN_GROUP_WITH_GROUPNAME,
		ALL_GROUP,
	}

	// -----------------------------------------------------------------------------------------------------------------

	public InnerGroup(String aGroupFilterFormatString) {
		this(aGroupFilterFormatString, null);
	}

	public InnerGroup(String aGroupFilterFormatString, String aModeOfTakeOutJoinedValueString) {
		this(aGroupFilterFormatString, aModeOfTakeOutJoinedValueString, null);
	}

	public InnerGroup(String aGroupFilterFormatString, String aModeOfTakeOutJoinedValueString, String aTargetJoinedValueBagNamesString) {
		init(aGroupFilterFormatString);
		selectModeOfTakeOutJoinedValue(aModeOfTakeOutJoinedValueString, ModeOfTakeOutJoinedValue.OWN_GROUP);
		initTargetJoinedValueBagNames(aTargetJoinedValueBagNamesString);
	}

	// -----------------------------------------------------------------------------------------------------------------

	private void init(String aGroupFilterFormatString) {
		mGroupFilterFormat = StockGroupFilterFormatFactory.INSTANCE.generateGroupFilterFormat(aGroupFilterFormatString);
		// Schema information transfer | スキーマ情報引継
		Properties tProperties = UDFContext.getUDFContext().getUDFProperties(this.getClass());
		StringBuilder tKeyBuilder = new StringBuilder();
		tKeyBuilder.append(this.getClass().getName());
		tKeyBuilder.append(".");
		tKeyBuilder.append(aGroupFilterFormatString.hashCode());
		tKeyBuilder.append(".");
		String tKeyPrefix = tKeyBuilder.toString();
		String tKeyOriginalColumnNum = mKeyOriginalColumnNum = tKeyPrefix + "colums";
		mOriginalColumnNum = Integer.parseInt(tProperties.getProperty(tKeyOriginalColumnNum, "0"));
		String tKeyJoinedValueBagIndexes = mKeyJoinedValueBagIndexes = tKeyPrefix + "joined-value-bag-indexes";
		String tKeyJoinedValueBagSizes = mKeyJoinedValueBagSizes = tKeyPrefix + "joined-value-bag-sizes";
		String[] tJoinedValueBagIndexesString = tProperties.getProperty(tKeyJoinedValueBagIndexes, "").split(FormatConstants.WORD_SEPARATOR);
		if (tJoinedValueBagIndexesString[0] != "") {
			int tJoinedValueBagIndexesLength = tJoinedValueBagIndexesString.length;
			int[] tJoinedValueBagIndexes = mJoinedValueBagIndexes = new int[tJoinedValueBagIndexesLength];
			for (int tIndex = tJoinedValueBagIndexesLength - 1; tIndex >= 0; tIndex--) {
				tJoinedValueBagIndexes[tIndex] = Integer.parseInt(tJoinedValueBagIndexesString[tIndex]);
			}
		}
		String[] tJoinedValueBagSizesString = tProperties.getProperty(tKeyJoinedValueBagSizes, "").split(FormatConstants.WORD_SEPARATOR);
		if (tJoinedValueBagSizesString[0] != "") {
			int tJoinedValueBagSizesLength = tJoinedValueBagSizesString.length;
			int[] tJoinedValueBagSizes = mJoinedValueBagSizes = new int[tJoinedValueBagSizesLength];
			for (int tIndex = tJoinedValueBagSizesLength - 1; tIndex >= 0; tIndex--) {
				tJoinedValueBagSizes[tIndex] = Integer.parseInt(tJoinedValueBagSizesString[tIndex]);
			}
		}
	}

	private void selectModeOfTakeOutJoinedValue(String aModeOfTakeOutJoinedValueString, ModeOfTakeOutJoinedValue aDefaultMode) {
		if (aModeOfTakeOutJoinedValueString == null) {
			mModeOfTakeOutJoinedValue = aDefaultMode;
			return;
		}
		try {
			mModeOfTakeOutJoinedValue = ModeOfTakeOutJoinedValue.valueOf(aModeOfTakeOutJoinedValueString.toUpperCase());
		} catch (RuntimeException e) {
			LogUtil.errorIllegalModeName(mLog, ModeOfTakeOutJoinedValue.values(), aModeOfTakeOutJoinedValueString, e);
		}
	}

	private void initTargetJoinedValueBagNames(String aTargetJoinedValueBagNamesString) {
		ArrayList<String> tTargetJoinedValueBagNames = mTargetJoinedValueBagNames = new ArrayList<String>();
		if (aTargetJoinedValueBagNamesString == null) {
			tTargetJoinedValueBagNames.add(AliasConstants.VALUE_JOIN_OUT_ALIAS);
			return;
		}
		String[] tSplitedTargetJoinedValueBagNames = aTargetJoinedValueBagNamesString.split(FormatConstants.WORD_SEPARATOR);
		for (String tCurrentTargetJoinedValueBagName : tSplitedTargetJoinedValueBagNames) {
			tCurrentTargetJoinedValueBagName = tCurrentTargetJoinedValueBagName.trim();
			if (tCurrentTargetJoinedValueBagName.length() > 0)
				tTargetJoinedValueBagNames.add(tCurrentTargetJoinedValueBagName);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Tuple exec(Tuple aInput) throws IOException {
		if (aInput == null)
			return null;

		TupleFactory tTupleFactory = TupleFactory.getInstance();
		BagFactory tBagFactory = DefaultBagFactory.getInstance();

		// processing target | 処理対象
		DataBag tTarget = DataType.toBag(aInput.get(0));
		DataBag tGroupColumn = DataType.toBag(aInput.get(1));

		// InnerGroup container | InnerGroup の器
		GroupFilterFormat tGroupFilterFormat = mGroupFilterFormat;
		List<GroupFilterFormatGroup> tGroupList = tGroupFilterFormat.getGroupList();
		LinkedHashMap<GroupFilterFormatGroup, List<Tuple>> tInnerGroupMap = new LinkedHashMap<GroupFilterFormatGroup, List<Tuple>>();
		for (GroupFilterFormatGroup tCurrentGFFGroup : tGroupList) {
			tInnerGroupMap.put(tCurrentGFFGroup, new ArrayList<Tuple>());
		}

		// Grouping
		Iterator<Tuple> tTargetIterator = tTarget.iterator();
		Iterator<Tuple> tGroupColumnIterator = tGroupColumn.iterator();
		int tOriginalColumnNum = mOriginalColumnNum;
		while (tGroupColumnIterator.hasNext()) {
			String tCurrentGroupColumnValue = (String) tGroupColumnIterator.next().get(0);
			Tuple tCurrentTargetTupleOrigin = tTargetIterator.next();
			// The contents of Bag influence it mutually for some reason when Pig executes eval UDF which appointed Bag in parallel.
			// As coping, I record the number of the columns from a schema of original Bag at the time of outputSchema, and only the number succeeds a column.
			// The interference of the value disappears in this at least UDF implementing. It is all right to execute InnerGroup in parallel.
			//
			// NOTE なぜだか Pig は Bag を指定した eval UDF を並列に動かすと、Bag の中身が相互に影響しあう。
			// 対処として、outputSchema の時点で元の Bag のスキーマからカラム数を記録し、その数だけカラムを引き継ぐ。
			// 少なくともこの実装をする UDF 同士では値の干渉はなくなる。InnerGroup を並列に実行するのは大丈夫。
			ArrayList<Object> tOriginBagColumns = new ArrayList<Object>();
			Iterator<Object> tColumnIterator = tCurrentTargetTupleOrigin.getAll().iterator();
			for (int tColumnCount = 0; tColumnCount < tOriginalColumnNum; tColumnCount++) {
				if (tColumnIterator.hasNext())
					tOriginBagColumns.add(tColumnIterator.next());
				else
					tOriginBagColumns.add(null);
			}
			int[] tJoinedValueBagIndexes = mJoinedValueBagIndexes;
			int[] tJoinedValueBagSizes = mJoinedValueBagSizes;
			// When target JoinedValueBag does not exist, overwrite tModeOfTakeOutJoinedValue in ALL_GROUP.
			// 対象とする JoinedValueBag が存在しない場合は tModeOfTakeOutJoinedValue を ALL_GROUP で上書きする。
			ModeOfTakeOutJoinedValue tModeOfTakeOutJoinedValue =
					tJoinedValueBagIndexes == null ? ModeOfTakeOutJoinedValue.ALL_GROUP : mModeOfTakeOutJoinedValue;
			for (GroupFilterFormatGroup tCurrentGFFGroup : tGroupList) {
				if (tCurrentGFFGroup.isMember(tCurrentGroupColumnValue)) {
					tInnerGroupMap.get(tCurrentGFFGroup).add(tTupleFactory.newTupleNoCopy(
							composeTransformedTuple(tCurrentGFFGroup, tModeOfTakeOutJoinedValue, tOriginBagColumns,
									tJoinedValueBagIndexes, tJoinedValueBagSizes)));
				}
			}
		}

		// create return value | 戻り値作成
		ArrayList<Object> tResultTupleList = new ArrayList<Object>();
		for (GroupFilterFormatGroup tCurrentGFFGroup : tGroupList) {
			tResultTupleList.add(tCurrentGFFGroup.getGroupName());
			tResultTupleList.add(tBagFactory.newDefaultBag(tInnerGroupMap.get(tCurrentGFFGroup)));
		}
		return tTupleFactory.newTupleNoCopy(tResultTupleList);
	}

	private List<Object> composeTransformedTuple(GroupFilterFormatGroup aCurrentGFFGroup,
			ModeOfTakeOutJoinedValue aModeOfTakeOutJoinedValue, List<Object> aOriginBagColumns,
			int[] aJoinedValueBagIndexes, int[] aJoinedValueBagSizes) throws ExecException {
		switch (aModeOfTakeOutJoinedValue) {
		case OWN_GROUP:
			// 「 Bag{ Tuple( group_name : chararray, ... ) } 」
			// convert above structure into following structure | 上記構造を下記構造に変換
			// 「 ... 」
			// element 0 is unnecessary and adds element after 1 | 要素(0) は不要、要素(1) 以降を追加
			return composeTransformedTuple(aCurrentGFFGroup, aOriginBagColumns, aJoinedValueBagIndexes, aJoinedValueBagSizes, 1);
		case OWN_GROUP_WITH_GROUPNAME:
			// 「 Bag{ Tuple( group_name : chararray, ... ) } 」
			// convert above structure into following structure | 上記構造を下記構造に変換
			// 「 group_name : chararray, ... 」
			// adds element after 0 | 要素(0) 以降を追加
			return composeTransformedTuple(aCurrentGFFGroup, aOriginBagColumns, aJoinedValueBagIndexes, aJoinedValueBagSizes, 0);
		default:
			return aOriginBagColumns;
		}
	}

	private List<Object> composeTransformedTuple(GroupFilterFormatGroup aCurrentGFFGroup,
			List<Object> aOriginBagColumns, int[] aJoinedValueBagIndexes,
			int[] aJoinedValueBagSizes, int aOffsetValue) throws ExecException {
		List<Object> tResultTuple = new ArrayList<Object>();
		String tTargetGroupName = aCurrentGFFGroup.getGroupName();
		int tJoinedValueBagIndexesIndex = 0;
		int tOriginBagColumnsSize = aOriginBagColumns.size();
		int tJoinedValueBagIndexesIndexLimit = aJoinedValueBagIndexes.length - 1;
		for (int tIndex = 0; tIndex < tOriginBagColumnsSize; tIndex++) {
			if (tIndex == aJoinedValueBagIndexes[tJoinedValueBagIndexesIndex]) {
				// if JoinedValueBag position | JoinedValueBag の位置なら
				DataBag tCurrentTargetJoinedValueBag = (DataBag) aOriginBagColumns.get(tIndex);
				Iterator<Tuple> tCurrentTargetJoinedValueBagIterator = tCurrentTargetJoinedValueBag.iterator();
				Tuple tCurrentGroupJoinedValueTuple = null;
				while (tCurrentTargetJoinedValueBagIterator.hasNext()) {
					Tuple tCurrentTargetJoinedValueTuple = tCurrentTargetJoinedValueBagIterator.next();
					String tCurrentTargetJoinedValueTupleGroupName = (String) tCurrentTargetJoinedValueTuple.get(0);
					if (tTargetGroupName.equals(tCurrentTargetJoinedValueTupleGroupName)) {
						tCurrentGroupJoinedValueTuple = tCurrentTargetJoinedValueTuple;
						break;
					} else
						continue;
				}
				if (tCurrentGroupJoinedValueTuple == null) {
					// When Tuple belonging to this tTargetGroupName does not exist, set null
					// この tTargetGroupName に所属する Tuple が存在しない場合、 null 埋め
					int tCurrentBagSize = aJoinedValueBagSizes[tJoinedValueBagIndexesIndex];
					for (int tTupleIndex = 0; tTupleIndex < tCurrentBagSize; tTupleIndex++) {
						tResultTuple.add(null);
					}
				} else {
					List<Object> tTupleElementList = tCurrentGroupJoinedValueTuple.getAll();
					int tCurrentBagSize = aJoinedValueBagSizes[tJoinedValueBagIndexesIndex] + aOffsetValue;
					int tTupleElementListSize = tTupleElementList.size();
					for (int tTupleElementIndex = aOffsetValue; tTupleElementIndex < tCurrentBagSize; tTupleElementIndex++) {
						if (tTupleElementIndex < tTupleElementListSize) {
							tResultTuple.add(tTupleElementList.get(tTupleElementIndex));
						} else {
							tResultTuple.add(null);
						}
					}
				}
				if (tJoinedValueBagIndexesIndex < tJoinedValueBagIndexesIndexLimit)
					tJoinedValueBagIndexesIndex++;
			} else {
				tResultTuple.add(aOriginBagColumns.get(tIndex));
			}
		}
		return tResultTuple;
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> tFuncList = new ArrayList<FuncSpec>();
		Schema tSchema = new Schema();
		tSchema.add(new Schema.FieldSchema(null, DataType.BAG));
		tSchema.add(new Schema.FieldSchema(null, DataType.BAG));
		tFuncList.add(new FuncSpec(this.getClass().getName(), tSchema));
		return tFuncList;
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Schema outputSchema(Schema aInput) {
		Schema tResultSchema = new Schema();

		List<FieldSchema> tInputFields = aInput.getFields();
		FieldSchema tInputTarget = tInputFields.get(0);
		Schema tInputTargetSchema = tInputTarget.schema;
		try {
			// record necessary schema information at processing | 処理時に必要なスキーマ情報を記録する
			Schema tTransformedTargetTupleSchema = commitSchemaInformation(tInputTargetSchema);

			Schema tInnerSchema = new Schema();

			String tAliasGroupNamePrefix = AliasConstants.INNER_GROUP_OUT_ALIAS_GROUP_NAME_PREFIX;
			String tAliasGroupBagPrefix = AliasConstants.INNER_GROUP_OUT_ALIAS_GROUP_BAG_PREFIX;
			byte tDataTypeChararray = DataType.CHARARRAY;
			byte tDataTypeBag = DataType.BAG;

			List<GroupFilterFormatGroup> tGroupList = mGroupFilterFormat.getGroupList();
			for (GroupFilterFormatGroup tCurrentGFFGroup : tGroupList) {
				// Group Name | グループ名
				tInnerSchema.add(new FieldSchema(tAliasGroupNamePrefix + tCurrentGFFGroup.getGroupName()
						, tDataTypeChararray));
				// Structure of Bag of each group that is structure of input Bag | 各グループの Bag の構造、つまり入力 Bag の構造
				tInnerSchema.add(new FieldSchema(
						tAliasGroupBagPrefix + tCurrentGFFGroup.getGroupName()
						, tTransformedTargetTupleSchema, tDataTypeBag));
			}

			tResultSchema.add(new FieldSchema(AliasConstants.INNER_GROUP_OUT_ALIAS_TOP
					, tInnerSchema, DataType.TUPLE));
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}

		return tResultSchema;
	}

	/**
	 * Construction of output Schema depending on Mode <br>
	 * Mode に応じた 出力Schema の構築 <br>
	 */
	private Schema commitSchemaInformation(Schema aInputTargetSchema) throws FrontendException {
		// record the number of input Bag columns for exec | exec のために元Bagのカラム数を記録
		Properties tProperties = UDFContext.getUDFContext().getUDFProperties(this.getClass());
		// Possibly this may become needless after pig-0.9.
		// XXX pig-0.8系 から 0.9系 間の仕様変更に対応。もしかしたらこれが不要になってるかも知れない。
		Schema tInputTargetTupleSchema = aInputTargetSchema.getFields().get(0).schema;
		tProperties.setProperty(mKeyOriginalColumnNum, String.valueOf(tInputTargetTupleSchema.size()));
		// record columns num and position of input Bag "<joined_value> : Bag{ Tuple( group_name : chararray ) }" for exec
		// exec のために元Bagの <joined_value> : Bag{ Tuple( group_name : chararray ) } の 位置・カラム数 を記録。
		ModeOfTakeOutJoinedValue tModeOfTakeOutJoinedValue = mModeOfTakeOutJoinedValue;
		if (tModeOfTakeOutJoinedValue == ModeOfTakeOutJoinedValue.ALL_GROUP)
			return aInputTargetSchema;
		StringBuilder tKeyJoinedValueBagIndexesStringBuilder = new StringBuilder();
		StringBuilder tKeyJoinedValueBagSizesStringBuilder = new StringBuilder();
		List<FieldSchema> tInputTargetTupleFields = tInputTargetTupleSchema.getFields();
		int tInputTargetTupleFieldsSize = tInputTargetTupleFields.size();
		ArrayList<String> tTargetJoinedValueBagNames = mTargetJoinedValueBagNames;
		String tValueJoinOutAliasInnerGroupName = AliasConstants.VALUE_JOIN_OUT_ALIAS_INNER_GROUP_NAME;
		// rebuild a schema of aInputTargetSchema depending on Mode
		// aInputTargetSchema のスキーマを Mode に応じて再構築
		Schema tTransformedTargetTupleSchema = new Schema();
		for (int tIndex = 0; tIndex < tInputTargetTupleFieldsSize; tIndex++) {
			FieldSchema tCurrentInputTargetTupleFieldSchema = tInputTargetTupleFields.get(tIndex);
			if (StringUtil.equalsWord(tCurrentInputTargetTupleFieldSchema.alias, tTargetJoinedValueBagNames)) {
				// acquire alias of Field hoping that it is group_name of the "Bag{ Tuple( gourp_name : chararray ) }"
				// Bag{ Tuple( gourp_name : chararray ) } の group_name であることを期待するフィールドの alias を取得
				String tFirstFieldName = tCurrentInputTargetTupleFieldSchema.schema.getFields().get(0).schema.getFields().get(0).alias;
				if (tFirstFieldName.equals(tValueJoinOutAliasInnerGroupName)) {
					tKeyJoinedValueBagIndexesStringBuilder.append(tIndex);
					tKeyJoinedValueBagIndexesStringBuilder.append(',');
					tKeyJoinedValueBagSizesStringBuilder.append(
							composeTransformedSchema(tModeOfTakeOutJoinedValue, tTransformedTargetTupleSchema, tCurrentInputTargetTupleFieldSchema));
					tKeyJoinedValueBagSizesStringBuilder.append(',');
				} else {
					tTransformedTargetTupleSchema.add(tCurrentInputTargetTupleFieldSchema);
				}
			} else {
				tTransformedTargetTupleSchema.add(tCurrentInputTargetTupleFieldSchema);
			}
		}
		if (tKeyJoinedValueBagIndexesStringBuilder.length() > 0) {
			tProperties.setProperty(mKeyJoinedValueBagIndexes,
					tKeyJoinedValueBagIndexesStringBuilder.substring(0, tKeyJoinedValueBagIndexesStringBuilder.length() - 1));
			tProperties.setProperty(mKeyJoinedValueBagSizes,
					tKeyJoinedValueBagSizesStringBuilder.substring(0, tKeyJoinedValueBagSizesStringBuilder.length() - 1));
		}
		return tTransformedTargetTupleSchema;
	}

	/**
	 * @return
	 *         Number of element of aInputTargetTupleFieldSchema after the conversion <br>
	 *         変換後の aInputTargetTupleFieldSchema の要素数 <br>
	 */
	private int composeTransformedSchema(ModeOfTakeOutJoinedValue aModeOfTakeOutJoinedValue, Schema aTransformedTargetTupleSchema, FieldSchema aInputTargetTupleFieldSchema) {
		switch (aModeOfTakeOutJoinedValue) {
		case OWN_GROUP:
			// 「 Bag{ Tuple( group_name : chararray, ... ) } 」
			// convert above structure into following structure | 上記構造を下記構造に変換
			// 「 ... 」
			// element 0 is unnecessary and adds element after 1 | 要素(0) は不要、要素(1) 以降を追加
			return appendSchemas(aTransformedTargetTupleSchema, aInputTargetTupleFieldSchema, 1);
		case OWN_GROUP_WITH_GROUPNAME:
			// 「 Bag{ Tuple( group_name : chararray, ... ) } 」
			// convert above structure into following structure | 上記構造を下記構造に変換
			// 「 group_name : chararray, ... 」
			// adds element after 0 | 要素(0) 以降を追加
			return appendSchemas(aTransformedTargetTupleSchema, aInputTargetTupleFieldSchema, 0);
		default:
			return 0;
		}
	}

	private int appendSchemas(Schema aTransformedTargetTupleSchema, FieldSchema aInputTargetTupleFieldSchema, int aOffsetValue) {
		List<FieldSchema> tList = aInputTargetTupleFieldSchema.schema.getFields().get(0).schema.getFields();
		int tSize = tList.size();
		for (int tIndex = aOffsetValue; tIndex < tSize; tIndex++) {
			aTransformedTargetTupleSchema.add(tList.get(tIndex));
		}
		return tSize - aOffsetValue;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
