package com.maywide.dbt.core.pojo.oracle;

public class ColumnInfo {
    private String TABLE_CAT ;//表类别（可能为空）
    private String TABLE_SCHEM;//表模式（可能为空）,在oracle中获取的是命名空间,其它数据库未知
    private String TABLE_NAME; //表名

    private String COLUMN_NAME;  //列名
    private int DATA_TYPE ; //对应的java.sql.Types的SQL类型(列类型ID)

    private  String TYPE_NAME;  //java.sql.Types类型名称(列类型名称)
    private int COLUMN_SIZE;  //列大小
    private int DECIMAL_DIGITS;  //小数位数
    private int NUM_PREC_RADIX;  //基数（通常是10或2） --未知
    /**
     *  0 (columnNoNulls) - 该列不允许为空
     *  1 (columnNullable) - 该列允许为空
     *  2 (columnNullableUnknown) - 不确定该列是否为空
     */
    private int NULLABLE;  //是否允许为null
    private String REMARKS;  //列描述
    private String COLUMN_DEF;  //默认值
    private int CHAR_OCTET_LENGTH;    // 对于 char 类型，该长度是列中的最大字节数
    private int ORDINAL_POSITION;   //表中列的索引（从1开始）
    /**
     * ISO规则用来确定某一列的是否可为空(等同于NULLABLE的值:[ 0:'YES'; 1:'NO'; 2:''; ])
     * YES -- 该列可以有空值;
     * NO -- 该列不能为空;
     * 空字符串--- 不知道该列是否可为空
     */
    private String IS_NULLABLE;

    public String getTABLE_CAT() {
        return TABLE_CAT;
    }

    public void setTABLE_CAT(String TABLE_CAT) {
        this.TABLE_CAT = TABLE_CAT;
    }

    public String getTABLE_SCHEM() {
        return TABLE_SCHEM;
    }

    public void setTABLE_SCHEM(String TABLE_SCHEM) {
        this.TABLE_SCHEM = TABLE_SCHEM;
    }

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

    public int getDATA_TYPE() {
        return DATA_TYPE;
    }

    public void setDATA_TYPE(int DATA_TYPE) {
        this.DATA_TYPE = DATA_TYPE;
    }

    public String getTYPE_NAME() {
        return TYPE_NAME;
    }

    public void setTYPE_NAME(String TYPE_NAME) {
        this.TYPE_NAME = TYPE_NAME;
    }

    public int getCOLUMN_SIZE() {
        return COLUMN_SIZE;
    }

    public void setCOLUMN_SIZE(int COLUMN_SIZE) {
        this.COLUMN_SIZE = COLUMN_SIZE;
    }

    public int getDECIMAL_DIGITS() {
        return DECIMAL_DIGITS;
    }

    public void setDECIMAL_DIGITS(int DECIMAL_DIGITS) {
        this.DECIMAL_DIGITS = DECIMAL_DIGITS;
    }

    public int getNUM_PREC_RADIX() {
        return NUM_PREC_RADIX;
    }

    public void setNUM_PREC_RADIX(int NUM_PREC_RADIX) {
        this.NUM_PREC_RADIX = NUM_PREC_RADIX;
    }

    public int getNULLABLE() {
        return NULLABLE;
    }

    public void setNULLABLE(int NULLABLE) {
        this.NULLABLE = NULLABLE;
    }

    public String getREMARKS() {
        return REMARKS;
    }

    public void setREMARKS(String REMARKS) {
        this.REMARKS = REMARKS;
    }

    public String getCOLUMN_DEF() {
        return COLUMN_DEF;
    }

    public void setCOLUMN_DEF(String COLUMN_DEF) {
        this.COLUMN_DEF = COLUMN_DEF;
    }

    public int getCHAR_OCTET_LENGTH() {
        return CHAR_OCTET_LENGTH;
    }

    public void setCHAR_OCTET_LENGTH(int CHAR_OCTET_LENGTH) {
        this.CHAR_OCTET_LENGTH = CHAR_OCTET_LENGTH;
    }

    public int getORDINAL_POSITION() {
        return ORDINAL_POSITION;
    }

    public void setORDINAL_POSITION(int ORDINAL_POSITION) {
        this.ORDINAL_POSITION = ORDINAL_POSITION;
    }

    public String getIS_NULLABLE() {
        return IS_NULLABLE;
    }

    public void setIS_NULLABLE(String IS_NULLABLE) {
        this.IS_NULLABLE = IS_NULLABLE;
    }
}
