package com.maywide.dbt.core.pojo.oracle;

import java.util.List;

public class TableInfo {
    private String TABLE_NAME ;
    private String TABLE_TYPE ;
    private String REMARKS;
    private String SECHEMA;

    private List<IndexInfo> indexInfoList;
    private List<PrimaryKey> primaryKeyList;
    private List<ColumnInfo> columnInfoList;

    private SequenceInfo sequenceInfo;

    public String getTABLE_NAME() {
        return TABLE_NAME;
    }

    public void setTABLE_NAME(String TABLE_NAME) {
        this.TABLE_NAME = TABLE_NAME;
    }

    public String getTABLE_TYPE() {
        return TABLE_TYPE;
    }

    public void setTABLE_TYPE(String TABLE_TYPE) {
        this.TABLE_TYPE = TABLE_TYPE;
    }

    public String getREMARKS() {
        return REMARKS;
    }

    public void setREMARKS(String REMARKS) {
        this.REMARKS = REMARKS;
    }

    public String getSECHEMA() {
        return SECHEMA;
    }

    public void setSECHEMA(String SECHEMA) {
        this.SECHEMA = SECHEMA;
    }

    public List<IndexInfo> getIndexInfoList() {
        return indexInfoList;
    }

    public void setIndexInfoList(List<IndexInfo> indexInfoList) {
        this.indexInfoList = indexInfoList;
    }

    public List<PrimaryKey> getPrimaryKeyList() {
        return primaryKeyList;
    }

    public void setPrimaryKeyList(List<PrimaryKey> primaryKeyList) {
        this.primaryKeyList = primaryKeyList;
    }

    public List<ColumnInfo> getColumnInfoList() {
        return columnInfoList;
    }

    public void setColumnInfoList(List<ColumnInfo> columnInfoList) {
        this.columnInfoList = columnInfoList;
    }

    public SequenceInfo getSequenceInfo() {
        return sequenceInfo;
    }

    public void setSequenceInfo(SequenceInfo sequenceInfo) {
        this.sequenceInfo = sequenceInfo;
    }

    @Override
    public String toString(){

        StringBuffer sb = new StringBuffer("\n *************表{"+TABLE_NAME+"} 信息 ***************\n");
        sb.append("表{"+TABLE_NAME+"} 类型 {"+TABLE_TYPE+"} 备注{"+REMARKS+"} \n")
                .append(" ----------列信息 ------------\n ");
        if(columnInfoList!=null && !columnInfoList.isEmpty()){
            for (ColumnInfo columnInfo : columnInfoList) {
                sb.append("列名 {"+ columnInfo.getCOLUMN_NAME()+"}, 类型{" + columnInfo.getTYPE_NAME()+"} ," +
                        " 长度 {"+columnInfo.getCOLUMN_SIZE()+"} , 小数精度{"+columnInfo.getDECIMAL_DIGITS()+"} ," +
                        " 是否为null{" + columnInfo.getNULLABLE()+"}, 备注{"+ columnInfo.getREMARKS()+"} ,默认值{" +columnInfo.getCOLUMN_DEF()+"} \n");

            }
        }
        sb.append("  ----------索引信息 ------------\n");
        if(indexInfoList!=null && !indexInfoList.isEmpty()){
            for (IndexInfo index : indexInfoList) {
                sb.append("索引名 {"+ index.getINDEX_NAME()+"}, 列名{" + index.getCOLUMN_NAME()+"} ,是否为空{"+index.isNON_UNIQUE()+"} \n" );
            }
        }
        sb.append(" ----------主键信息 ------------\n");
        if(primaryKeyList!=null && !primaryKeyList.isEmpty()){
            for (PrimaryKey pk : primaryKeyList) {
                sb.append("主键 {"+pk.getPK_NAME()+"}, 列名{" + pk.getCOLUMN_NAME()+"} ,序列号{"+pk.getKEY_SEQ()+"} \n" );
            }
        }

        sb.append(" ----------序列信息 ------------\n");
        if(sequenceInfo != null){
            sb.append(" 序列名:{"+sequenceInfo.getColumn()+"},下一个值{"+sequenceInfo.getMaxValue()+"}");
        }

        return sb.toString();
    }
}
