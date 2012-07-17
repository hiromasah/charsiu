package jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping;

public class DoubleCaster extends AbstractNumberCaster {

	public static DoubleCaster INSTANCE = new DoubleCaster();

	private DoubleCaster() {};

	@Override
	public Object castNumber(String aDataString) {
		return Double.parseDouble(aDataString);
	}

}
