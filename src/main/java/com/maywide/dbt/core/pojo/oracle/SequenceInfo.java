package com.maywide.dbt.core.pojo.oracle;

public class SequenceInfo {
    private String oracleSeqName;
    private String column;
    private long maxValue;

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public long getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(long maxValue) {
        this.maxValue = maxValue;
    }

    public String getOracleSeqName() {
        return oracleSeqName;
    }

    public void setOracleSeqName(String oracleSeqName) {
        this.oracleSeqName = oracleSeqName;
    }
}
