package jp.ac.u.tokyo.m.pig.udf.eval.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import jp.ac.u.tokyo.m.pig.udf.AliasConstants;
import jp.ac.u.tokyo.m.pig.udf.FormatConstants;
import jp.ac.u.tokyo.m.pig.udf.format.AdditionalParameterDefinitionFormat;
import jp.ac.u.tokyo.m.pig.udf.format.GroupFilterFormat;
import jp.ac.u.tokyo.m.pig.udf.format.GroupFilterFormatGroup;
import jp.ac.u.tokyo.m.pig.udf.format.GroupFilterFormatGroupMember;
import jp.ac.u.tokyo.m.pig.udf.format.StockAdditionalParameterDefinitionFormatFactory;
import jp.ac.u.tokyo.m.pig.udf.format.StockGroupFilterFormatFactory;
import jp.ac.u.tokyo.m.pig.udf.format.AdditionalParameterDefinitionFormat.AdditionalParameterCaster;
import jp.ac.u.tokyo.m.pig.udf.format.GroupFilterFormatGroupMember.AdditionalParameter;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * GroupFilterFormat, AdditionalParameterDefinitionFormat に定義された付加パラメータを取得します。
 * 追加パラメータ自体は GroupFilterFormat に記述します。
 * 追加パラメータには AdditionalParameterDefinitionFormat を利用してスキーマを決定します。
 * 
 * 渡された値が所属する GroupFilterFormat のグループを検出し、
 * ヒットした分の AdditionalParameter を Bag にまとめて出力します。
 */
public class ValueJoin extends EvalFunc<DataBag> {

	// -----------------------------------------------------------------------------------------------------------------

	private GroupFilterFormat mGroupFilterFormat = null;
	private AdditionalParameterDefinitionFormat mAdditionalParameterDefinitionFormat = null;

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * この構成の場合は group_name のみ出力します。
	 */
	public ValueJoin(String aGroupFilterFormatString) {
		mGroupFilterFormat = StockGroupFilterFormatFactory.INSTANCE.generateGroupFilterFormat(aGroupFilterFormatString);
	}

	/**
	 * group_name と AdditionalParameterDefinitionFormat に指定されるスキーマの値を出力します。
	 */
	public ValueJoin(String aGroupFilterFormatString, String aAdditionalParameterDefinitionFormatString) {
		this(aGroupFilterFormatString);
		mAdditionalParameterDefinitionFormat = StockAdditionalParameterDefinitionFormatFactory.INSTANCE.generate(
				aAdditionalParameterDefinitionFormatString,
				FormatConstants.WORD_SEPARATOR, FormatConstants.ALIAS_TYPE_SEPARATOR);
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public DataBag exec(Tuple aInput) throws IOException {
		TupleFactory tTupleFactory = TupleFactory.getInstance();
		BagFactory tBagFactory = DefaultBagFactory.getInstance();

		ArrayList<Tuple> tProtoBag = new ArrayList<Tuple>();

		if (aInput == null)
			return tBagFactory.newDefaultBag(tProtoBag);

		// 判定対象
		String tTarget = DataType.toString(aInput.get(0));
		if (tTarget == null)
			return tBagFactory.newDefaultBag(tProtoBag);

		boolean tExistsAdditionalParameterDefinitionFormat = true;
		List<AdditionalParameterCaster> tAdditionalParameterCasters = null;
		if (mAdditionalParameterDefinitionFormat != null)
			tAdditionalParameterCasters = mAdditionalParameterDefinitionFormat.getAdditionalParameterCasters();
		else
			tExistsAdditionalParameterDefinitionFormat = false;
		List<GroupFilterFormatGroup> tGroupList = mGroupFilterFormat.getGroupList();
		// グループ数分のループ
		for (GroupFilterFormatGroup tCurrentGFFGroup : tGroupList) {
			GroupFilterFormatGroupMember tHitGroupMember = tCurrentGFFGroup.isMemberReturnGroupMember(tTarget);
			// hit していたら。true の代わりに hit したインスタンスを返す。
			if (tHitGroupMember != null) {
				ArrayList<Object> tProtoTuple = new ArrayList<Object>();
				tProtoTuple.add(tCurrentGFFGroup.getGroupName());
				// AdditionalParameterDefinitionFormat が指定されていない場合は group_name だけの状態を出力
				if (tExistsAdditionalParameterDefinitionFormat) {
					// まず取得。null である可能性もある。
					List<AdditionalParameter> tAdditionalParameters = tHitGroupMember.getAdditionalParameters();
					if (tAdditionalParameters == null) {
						for (AdditionalParameterCaster tCurrentAdditionalParameterCaster : tAdditionalParameterCasters) {
							// 値が記述されていなくて、型だけ有る場合、null を追加
							tProtoTuple.add(tCurrentAdditionalParameterCaster.castParameter(null));
						}
					} else {
						// 値が有るなら、値の Iterator を作る
						Iterator<AdditionalParameter> tAdditionalParametersIterator = tAdditionalParameters.iterator();
						for (AdditionalParameterCaster tCurrentAdditionalParameterCaster : tAdditionalParameterCasters) {
							// 型に対応する値が有るなら
							if (tAdditionalParametersIterator.hasNext()) {
								tProtoTuple.add(tCurrentAdditionalParameterCaster.castParameter(
										tAdditionalParametersIterator.next().getValue()));
							}
							// 型に対応する値がなければ null を追加
							else {
								tProtoTuple.add(tCurrentAdditionalParameterCaster.castParameter(null));
							}
						}
					}
				}
				tProtoBag.add(tTupleFactory.newTupleNoCopy(tProtoTuple));
			}
			// 同一グループ中では１度のヒットで終わり、次のグループへ。
		}

		return tBagFactory.newDefaultBag(tProtoBag);
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> tFuncList = new ArrayList<FuncSpec>();
		tFuncList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));
		return tFuncList;
	}

	// -----------------------------------------------------------------------------------------------------------------

	// APDF から決定
	@Override
	public Schema outputSchema(Schema aInput) {
		// Bag{ ( group_name:chararray, APDF から決まるスキーマ ) }
		Schema tBagSchema = new Schema();
		Schema tTupleSchema = new Schema();
		// 1カラム目は group_name
		tTupleSchema.add(new FieldSchema(AliasConstants.VALUE_JOIN_OUT_ALIAS_INNER_GROUP_NAME, DataType.CHARARRAY));
		// 2カラム目以降は AdditionalParameterDefinitionFormat に依存
		if (mAdditionalParameterDefinitionFormat != null) {
			AdditionalParameterDefinitionFormat tAdditionalParameterDefinitionFormat = mAdditionalParameterDefinitionFormat;
			List<AdditionalParameterCaster> tAdditionalParameterCasters = tAdditionalParameterDefinitionFormat.getAdditionalParameterCasters();
			List<String> tAliases = tAdditionalParameterDefinitionFormat.getAliases();
			Iterator<AdditionalParameterCaster> tAdditionalParameterCastersIterator = tAdditionalParameterCasters.iterator();
			Iterator<String> tAliasesIterator = tAliases.iterator();
			while (tAdditionalParameterCastersIterator.hasNext() && tAliasesIterator.hasNext()) {
				tTupleSchema.add(new FieldSchema(
						tAliasesIterator.next(),
						tAdditionalParameterCastersIterator.next().getType()));
			}
		}

		try {
			tBagSchema.add(new FieldSchema(AliasConstants.VALUE_JOIN_OUT_ALIAS, tTupleSchema, DataType.BAG));
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}
		return tBagSchema;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
