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

public class GetYearSpan extends EvalFunc<Integer> {

	// -----------------------------------------------------------------------------------------------------------------

	private final YearSpanMap mYearSpanMap;

	// -----------------------------------------------------------------------------------------------------------------

	public GetYearSpan() {
		mYearSpanMap = CacheYearSpanHashMap.INSTANCE;
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Integer exec(Tuple aInput) throws IOException {
		// 無効値
		if (aInput == null)
			return null;

		// 判定対象
		String tBaseDate = DataType.toString(aInput.get(0));
		if (tBaseDate == null || tBaseDate.length() == 0)
			return null;
		String tTargetDate = DataType.toString(aInput.get(1));
		if (tTargetDate == null || tTargetDate.length() == 0)
			return null;

		return mYearSpanMap.getYearSpan(CalendarUtil.getWesternCalendarFormatDate(tBaseDate)
				, CalendarUtil.getWesternCalendarFormatDate(tTargetDate));
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> tFuncList = new ArrayList<FuncSpec>();
		Schema tSchema = new Schema();
		tSchema = new Schema();
		tSchema.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		tSchema.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		tFuncList.add(new FuncSpec(this.getClass().getName(), tSchema));
		return tFuncList;
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Schema outputSchema(Schema aInput) {
		Schema tResultSchema = new Schema();
		tResultSchema.add(new FieldSchema(AliasConstants.GET_YEAR_SPAN_OUT_ALIAS, DataType.INTEGER));
		return tResultSchema;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
