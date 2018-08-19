package com.maywide.dbt.core.pojo.oracle;

public class IndexInfo {
    private boolean NON_UNIQUE;//是否不唯一
    private String INDEX_NAME;//索引名
    private String COLUMN_NAME;//列明

    public boolean isNON_UNIQUE() {
        return NON_UNIQUE;
    }

    public void setNON_UNIQUE(boolean NON_UNIQUE) {
        this.NON_UNIQUE = NON_UNIQUE;
    }

    public String getINDEX_NAME() {
        return INDEX_NAME;
    }

    public void setINDEX_NAME(String INDEX_NAME) {
        this.INDEX_NAME = INDEX_NAME;
    }

    public String getCOLUMN_NAME() {
        return COLUMN_NAME;
    }

    public void setCOLUMN_NAME(String COLUMN_NAME) {
        this.COLUMN_NAME = COLUMN_NAME;
    }
}
