package jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping;

public abstract class AbstractNumberCaster implements ColumnDataCaster {

	@Override
	public Object cast(String aDataString) {
		try {
			return castNumber(aDataString);
		} catch (NumberFormatException e) {
			if (aDataString.trim().length() == 0)
				return null;
			else
				throw e;
		}
	}

	abstract Object castNumber(String aDataString);

}
