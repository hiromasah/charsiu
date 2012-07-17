package jp.ac.u.tokyo.m.data.type;

import org.apache.pig.data.DataType;

public class TypeStringCasterPigToPigTypeByte extends AbstractPigTypeStringCaster<Byte> {

	public static final TypeStringCasterPigToPigTypeByte INSTANCE = new TypeStringCasterPigToPigTypeByte();

	private TypeStringCasterPigToPigTypeByte() {}

	@Override
	public Byte caseDouble() {
		return DataType.DOUBLE;
	}

	@Override
	public Byte caseFloat() {
		return DataType.FLOAT;
	}

	@Override
	public Byte caseInt() {
		return DataType.INTEGER;
	}

	@Override
	public Byte caseLong() {
		return DataType.LONG;
	}

	@Override
	public Byte caseString() {
		return DataType.CHARARRAY;
	}

}
