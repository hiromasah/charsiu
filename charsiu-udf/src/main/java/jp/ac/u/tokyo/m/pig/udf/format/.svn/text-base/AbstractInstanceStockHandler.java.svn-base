package jp.ac.u.tokyo.m.pig.udf.format;

import java.util.LinkedHashMap;

import jp.ac.u.tokyo.m.pig.udf.BehaviorConstants;

public abstract class AbstractInstanceStockHandler<KEY, VALUE> {

	// -----------------------------------------------------------------------------------------------------------------

	private LinkedHashMap<KEY, VALUE> mInstanceStock = new LinkedHashMap<KEY, VALUE>();

	// -----------------------------------------------------------------------------------------------------------------

	protected void put(KEY aKey, VALUE aValue) {
		LinkedHashMap<KEY, VALUE> tInstanceStock = mInstanceStock;
		tInstanceStock.put(aKey, aValue);
		// 上限処理
		if (tInstanceStock.size() > BehaviorConstants.INSTANCE_STOCK_SIZE) {
			tInstanceStock.remove(tInstanceStock.keySet().iterator().next());
		}
	}

	protected VALUE get(KEY aKey) {
		return mInstanceStock.get(aKey);
	}

	protected LinkedHashMap<KEY, VALUE> getInstanceList() {
		return mInstanceStock;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
