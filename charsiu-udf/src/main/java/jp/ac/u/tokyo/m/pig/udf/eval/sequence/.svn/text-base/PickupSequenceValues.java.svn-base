package jp.ac.u.tokyo.m.pig.udf.eval.sequence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import jp.ac.u.tokyo.m.log.LogUtil;
import jp.ac.u.tokyo.m.pig.udf.AliasConstants;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class PickupSequenceValues extends EvalFunc<DataBag> {

	// -----------------------------------------------------------------------------------------------------------------

	private static Log mLog = LogFactory.getLog(PickupSequenceValues.class);

	// -----------------------------------------------------------------------------------------------------------------

	private OutputMode mOutputMode;

	private enum OutputMode {
		ALL_VALUE_PACKET_BAG,
		ALL_VALUE_FLAT,
		FIRST_VALUE_FLAT,
	}

	// -----------------------------------------------------------------------------------------------------------------

	public PickupSequenceValues() {
		this(null);
	}

	public PickupSequenceValues(String aOutputModeString) {
		selectOutputMode(aOutputModeString, OutputMode.ALL_VALUE_PACKET_BAG);
	}

	// -----------------------------------------------------------------------------------------------------------------

	private void selectOutputMode(String aOutputModeString, OutputMode aDefaultMode) {
		if (aOutputModeString == null) {
			mOutputMode = aDefaultMode;
			return;
		}
		try {
			mOutputMode = OutputMode.valueOf(aOutputModeString.toUpperCase());
		} catch (RuntimeException e) {
			LogUtil.errorIllegalModeName(mLog, OutputMode.values(), aOutputModeString, e);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public DataBag exec(Tuple aInput) throws IOException {
		// 無効値
		if (aInput == null)
			return DefaultBagFactory.getInstance().newDefaultBag();

		// 判定対象
		DataBag tTargetBag = DataType.toBag(aInput.get(0));
		DataBag tValueBag = DataType.toBag(aInput.get(1));
		Long tStartValue = DataType.toLong(aInput.get(2));

		return DefaultBagFactory.getInstance().newDefaultBag(composeProtoBag(tTargetBag, tValueBag, tStartValue));
	}

	private ArrayList<Tuple> composeProtoBag(DataBag aTargetBag, DataBag aValueBag, Long aStartValue) throws ExecException {
		if (aStartValue == null)
			return new ArrayList<Tuple>();
		HashMap<Long, ArrayList<Tuple>> tTargetBagMap = createValueBaseBagMap(aTargetBag, aValueBag);
		switch (mOutputMode) {
		case ALL_VALUE_PACKET_BAG:
		default:
			// sequence_values : Bag{ sequence_member_bag : Bag { Tuple( <InputTuple> ) } }
			return composeProtoBagModeAllValuePacketBag(tTargetBagMap, aStartValue);
		case ALL_VALUE_FLAT:
			// sequence_values : Bag{ Tuple( <InputTuple> ) }
			return composeProtoBagModeAllValueFlat(tTargetBagMap, aStartValue);
		case FIRST_VALUE_FLAT:
			// sequence_values : Bag{ Tuple( <InputTuple> ) }
			return composeProtoBagModeFirstValueFlat(tTargetBagMap, aStartValue);
		}
	}

	private HashMap<Long, ArrayList<Tuple>> createValueBaseBagMap(DataBag aTargetBag, DataBag aValueBag) throws ExecException {
		Iterator<Tuple> tTargetBagIterator = aTargetBag.iterator();
		Iterator<Tuple> tDateBagIterator = aValueBag.iterator();

		HashMap<Long, ArrayList<Tuple>> tTargetBagMap = new HashMap<Long, ArrayList<Tuple>>();
		while (tDateBagIterator.hasNext()) {
			Long tKey = DataType.toLong(tDateBagIterator.next().get(0));
			Tuple tValue = tTargetBagIterator.next();
			if (tKey == null)
				continue;
			ArrayList<Tuple> tList = tTargetBagMap.get(tKey);
			if (tList == null) {
				tList = new ArrayList<Tuple>();
				tList.add(tValue);
				tTargetBagMap.put(tKey, tList);
			} else {
				tList.add(tValue);
			}
		}
		return tTargetBagMap;
	}

	private ArrayList<Tuple> composeProtoBagModeAllValuePacketBag(HashMap<Long, ArrayList<Tuple>> aTargetBagMap, Long aStartValue) throws ExecException {
		ArrayList<Tuple> tProtoBag = new ArrayList<Tuple>();
		TupleFactory tTupleFactory = TupleFactory.getInstance();
		while (true) {
			ArrayList<Tuple> tCurrentProtoBag = aTargetBagMap.get(aStartValue++);
			if (tCurrentProtoBag == null)
				break;
			Tuple tCurrentTuple = tTupleFactory.newTuple(DefaultBagFactory.getInstance().newDefaultBag(tCurrentProtoBag));
			tProtoBag.add(tCurrentTuple);
		}
		return tProtoBag;
	}

	private ArrayList<Tuple> composeProtoBagModeAllValueFlat(HashMap<Long, ArrayList<Tuple>> aTargetBagMap, Long aStartValue) throws ExecException {
		ArrayList<Tuple> tProtoBag = new ArrayList<Tuple>();
		while (true) {
			ArrayList<Tuple> tCurrentProtoBag = aTargetBagMap.get(aStartValue++);
			if (tCurrentProtoBag == null)
				break;
			tProtoBag.addAll(tCurrentProtoBag);
		}
		return tProtoBag;
	}

	private ArrayList<Tuple> composeProtoBagModeFirstValueFlat(HashMap<Long, ArrayList<Tuple>> aTargetBagMap, Long aStartValue) throws ExecException {
		ArrayList<Tuple> tProtoBag = new ArrayList<Tuple>();
		while (true) {
			ArrayList<Tuple> tCurrentProtoBag = aTargetBagMap.get(aStartValue++);
			if (tCurrentProtoBag == null)
				break;
			tProtoBag.add(tCurrentProtoBag.get(0));
		}
		return tProtoBag;
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> tFuncList = new ArrayList<FuncSpec>();
		addFuncSpec(tFuncList, DataType.LONG);
		addFuncSpec(tFuncList, DataType.INTEGER);
		return tFuncList;
	}

	private void addFuncSpec(List<FuncSpec> aFuncList, byte aArg3Type) {
		Schema tSchema = new Schema();
		tSchema = new Schema();
		tSchema.add(new Schema.FieldSchema(null, DataType.BAG));
		tSchema.add(new Schema.FieldSchema(null, DataType.BAG));
		tSchema.add(new Schema.FieldSchema(null, aArg3Type));
		aFuncList.add(new FuncSpec(this.getClass().getName(), tSchema));
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Schema outputSchema(Schema aInput) {
		List<FieldSchema> tInputFields = aInput.getFields();
		FieldSchema tInputTarget = tInputFields.get(0);

		Schema tBagSchema = new Schema();
		try {
			Schema tInputTargetSchema = tInputTarget.schema.getFields().get(0).schema;
			switch (mOutputMode) {
			case ALL_VALUE_PACKET_BAG:
			default:
				// sequence_values : Bag{ sequence_member_bag : Bag { Tuple( <InputTuple> ) } }
				Schema tInnerBagSchema = new Schema();
				tInnerBagSchema.add(new FieldSchema(AliasConstants.SEQUENCE_VALUES_OUT_ALIAS_INNER, tInputTargetSchema, DataType.BAG));
				tBagSchema.add(new FieldSchema(AliasConstants.SEQUENCE_VALUES_OUT_ALIAS_TOP, tInnerBagSchema, DataType.BAG));
				break;
			case ALL_VALUE_FLAT:
			case FIRST_VALUE_FLAT:
				// sequence_values : Bag{ Tuple( <InputTuple> ) }
				tBagSchema.add(new FieldSchema(AliasConstants.SEQUENCE_VALUES_OUT_ALIAS_TOP, tInputTargetSchema, DataType.BAG));
				break;
			}
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}

		return tBagSchema;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
