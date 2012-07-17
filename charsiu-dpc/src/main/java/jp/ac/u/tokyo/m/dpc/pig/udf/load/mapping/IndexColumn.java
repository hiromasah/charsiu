package jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping;

public class IndexColumn implements ResultColumn {

	private final int mIndex;
	private final ColumnDataCaster mCaster;

	public IndexColumn(int aIndex, ColumnDataCaster aCaster) {
		mIndex = aIndex;
		mCaster = aCaster;
	}

	@Override
	public Object getValue(String[] aValues) {
		try {
			return mCaster.cast(aValues[mIndex]);
		} catch (ArrayIndexOutOfBoundsException e) {
			return null;
		}
	}

}
