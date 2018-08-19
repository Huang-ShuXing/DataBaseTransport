package com.maywide.dbt.core.pojo.oracle;

public class PrimaryKey {
    private String TABLE_NAME ;//表
    private String COLUMN_NAME ;//列
    private short KEY_SEQ;//序列
    private String PK_NAME;//主键名

    public String getTABLE_NAME() {
        return TABLE_NAME;
    }

    public void setTABLE_NAME(String TABLE_NAME) {
        this.TABLE_NAME = TABLE_NAME;
    }

    public String getCOLUMN_NAME() {
        return COLUMN_NAME;
    }

    public void setCOLUMN_NAME(String COLUMN_NAME) {
        this.COLUMN_NAME = COLUMN_NAME;
    }

    public short getKEY_SEQ() {
        return KEY_SEQ;
    }

    public void setKEY_SEQ(short KEY_SEQ) {
        this.KEY_SEQ = KEY_SEQ;
    }

    public String getPK_NAME() {
        return PK_NAME;
    }

    public void setPK_NAME(String PK_NAME) {
        this.PK_NAME = PK_NAME;
    }
}
