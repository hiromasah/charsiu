package jp.ac.u.tokyo.m.pig.udf.eval.span;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import jp.ac.u.tokyo.m.calendar.CalendarUtil;
import jp.ac.u.tokyo.m.pig.udf.AliasConstants;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class AddDaySpan extends EvalFunc<String> {

	// -----------------------------------------------------------------------------------------------------------------

	private final DaySpanMap mDaySpanMap;

	// -----------------------------------------------------------------------------------------------------------------

	public AddDaySpan() {
		mDaySpanMap = CacheDaySpanHashMap.INSTANCE;
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public String exec(Tuple aInput) throws IOException {
		// 無効値
		if (aInput == null)
			return null;

		// 判定対象
		String tBaseDate = DataType.toString(aInput.get(0));
		if (tBaseDate == null || tBaseDate.length() == 0)
			return null;
		Integer tTargetSpan = DataType.toInteger(aInput.get(1));
		if (tTargetSpan == null)
			return null;

		return mDaySpanMap.addDaySpan(CalendarUtil.getWesternCalendarFormatDate(tBaseDate), tTargetSpan);
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> tFuncList = new ArrayList<FuncSpec>();
		ArrayList<FieldSchema> tFiledSchemas = new ArrayList<FieldSchema>();
		tFiledSchemas.add(new FieldSchema(null, DataType.CHARARRAY));
		tFiledSchemas.add(new FieldSchema(null, DataType.INTEGER));
		tFuncList.add(new FuncSpec(this.getClass().getName(), new Schema(tFiledSchemas)));
		return tFuncList;
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Schema outputSchema(Schema aInput) {
		Schema tResultSchema = new Schema();
		tResultSchema.add(new FieldSchema(AliasConstants.ADD_DAY_SPAN_OUT_ALIAS, DataType.CHARARRAY));
		return tResultSchema;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
