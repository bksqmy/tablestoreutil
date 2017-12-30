package com.mo9.microservice.tablestore.dao;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.google.gson.internal.LinkedHashTreeMap;
import com.mo9.microservice.annotation.PrimaryKey;
import com.mo9.microservice.annotation.TsEntity;
import com.mo9.microservice.common.BeanUtil;
import com.mo9.microservice.common.StringUtil;
import com.mo9.microservice.tablestore.helper.TsHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.lang.reflect.Field;
import java.util.*;

/**
 * @author wtwei .
 * @date 2017/11/15 .
 * @time 18:58 .
 */

@Repository
public class TsBaseDao<T> {
    private Log logger = LogFactory.getLog(TsBaseDao.class);
    
    @Resource
    private SyncClient client;
    
    
    public T save(T o){
        Class<?> clazz = o.getClass();
        
        TsEntity tsEntityAnn = clazz.getAnnotation(TsEntity.class);
        
        if (tsEntityAnn == null){
            throw new RuntimeException("要保存的TS实体未添加TsEntity注解");
        }
        
        String tableName = tsEntityAnn.tableName();
        if (StringUtil.isEmpty(tableName)){
            tableName = clazz.getSimpleName();
        }

        Map<String, Object> keyMap = new LinkedHashTreeMap<String, Object>();
        
        Map<String, Object> columns = new HashMap<String, Object>();;

        try {
            for (Field field : clazz.getDeclaredFields()) {
                field.setAccessible(true);
                PrimaryKey pkAnn = field.getAnnotation(PrimaryKey.class);
                if (pkAnn != null){ //是主键
                    keyMap.put(getKeyName(pkAnn, field), field.get(o));
                }else {
                    columns.put(field.getName(), field.get(o));
                }
                
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        
        com.alicloud.openservices.tablestore.model.PrimaryKey primaryKey = TsHelper.primaryKeyBuilder(keyMap);

        RowPutChange rowPutChange = new RowPutChange(tableName, primaryKey);
        
        this.setRowFields(rowPutChange, columns);

        client.putRow(new PutRowRequest(rowPutChange));
        return o;
    }
    

    /**
     * 单数据查询
     * @param entity
     * @return
     */
    public T find(T entity){
        Class<?> clazz = entity.getClass();

        TsEntity tsEntityAnn = clazz.getAnnotation(TsEntity.class);

        if (tsEntityAnn == null){
            throw new RuntimeException("要查询的TS实体未添加TsEntity注解, " + entity.getClass().getName());
        }

        String tableName = tsEntityAnn.tableName();
        if (StringUtil.isEmpty(tableName)){
            tableName = clazz.getSimpleName();
        }
        
        Map<String, Object> keyMap = new LinkedHashTreeMap<String, Object>();
        
        Object partKeyValue = null; //分区键值
        String partKeyName = null; // 分区键名

        try {
            for (Field field : clazz.getDeclaredFields()) {
                field.setAccessible(true);
                PrimaryKey pkAnn = field.getAnnotation(PrimaryKey.class);
                if (pkAnn != null){
                    if (StringUtil.isEmpty(field.get(entity))){
                        throw new RuntimeException("单行数据查询必须指定全部主键列的值，列名 " + field.getName() + " 未指定值。");
                    }
                    if (pkAnn.partition()){
                        partKeyName = getKeyName(pkAnn, field);
                        partKeyValue = field.get(entity);
                    }
                    keyMap.put(getKeyName(pkAnn, field), field.get(entity));
                }

            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        
        if (StringUtil.isEmpty(partKeyName)){
            throw new RuntimeException("要查询的实体未指定主键列, " + entity.getClass().getName());
        }
        if (StringUtil.isEmpty(partKeyValue)){
            throw new RuntimeException("要查询的实体未指定主键列的值, " + entity.getClass().getName());
        }

        // 读一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, TsHelper.primaryKeyBuilder(keyMap));
        criteria.setMaxVersions(1000);

        GetRowRequest getRowRequest = new GetRowRequest(criteria);

        GetRowResponse getRowResponse = client.getRow(getRowRequest);
        Row row = getRowResponse.getRow();

        
        Map<String, Object> columnsMap = new HashMap();
        if (row != null){
            for (Column column : row.getColumns()) {
                columnsMap.put(column.getName(), getJavaValue(column.getValue()));
            }
        }

        try {
            entity = (T) BeanUtil.map2bean(columnsMap, clazz.newInstance());
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        return entity;
    }

    /**
     * 范围查询
     * @return
     */
    public List<T> findRange(T startEntity, T endEntity){
        Class<?> startEntityClass = startEntity.getClass();
        Class<?> endEntityClass = endEntity.getClass();

        TsEntity startEntityClassAnnotation = startEntityClass.getAnnotation(TsEntity.class);
        TsEntity endEntityClassAnnotation = endEntityClass.getAnnotation(TsEntity.class);

        if (startEntityClassAnnotation == null){
            throw new RuntimeException("要查询的TS实体未添加TsEntity注解, " + startEntityClassAnnotation.getClass().getName());
        }
        if (endEntityClassAnnotation == null){
            throw new RuntimeException("要查询的TS实体未添加TsEntity注解, " + endEntityClassAnnotation.getClass().getName());
        }


        String startTableName = startEntityClassAnnotation.tableName();
        if (StringUtil.isEmpty(startTableName)){
            startTableName = startEntityClass.getSimpleName();
        }

        String endTableName = endEntityClassAnnotation.tableName();
        if (StringUtil.isEmpty(endTableName)){
            endTableName = endEntityClass.getSimpleName();
        }
        
        if (!startTableName.equals(endTableName)){
            throw new RuntimeException("StartEntity 和 EndEntity的表名必须一样");
        }


        com.alicloud.openservices.tablestore.model.PrimaryKey startPK;
        com.alicloud.openservices.tablestore.model.PrimaryKey endPK;
        try {

            startPK = getStartPrimaryKey(startEntity);
            endPK = getEndPrimaryKey(endEntity);
        }catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new RuntimeException("更新时，组装PrimaryKey失败。");
        }


        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(startTableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPK);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPK);
        rangeRowQueryCriteria.setMaxVersions(1000);
        
        GetRangeResponse getRangeResponse = client.getRange(new GetRangeRequest(rangeRowQueryCriteria));
        List<Row> rows = getRangeResponse.getRows();

        List resultBeans = new ArrayList<Object>();
        for (Row row : rows) {
            if (row != null){
                try {
                    resultBeans.add(TsHelper.row2Bean(row, startEntity.getClass().newInstance()));
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException("查询结果row转换成bean对象失败。");
                } 
            }
        }
        
        return resultBeans;
    }

    /**
     * 更新单行数据
     * @param updateEntity
     */
    public void update(T updateEntity){
        Class<?> clazz = updateEntity.getClass();

        TsEntity tsEntityAnn = clazz.getAnnotation(TsEntity.class);

        if (tsEntityAnn == null){
            throw new RuntimeException("要更新的TS实体未添加TsEntity注解, " + updateEntity.getClass().getName());
        }

        String tableName = tsEntityAnn.tableName();
        if (StringUtil.isEmpty(tableName)){
            tableName = clazz.getSimpleName();
        }

        Map<String, Object> keyMap = new LinkedHashTreeMap<String, Object>();
        Map<String, ColumnValue> updateColumnsMap = new LinkedHashTreeMap<String, ColumnValue>();

        Object partKeyValue = null; //分区键值
        String partKeyName = null; // 分区键名
        
        

        try {
            for (Field field : clazz.getDeclaredFields()) {
                field.setAccessible(true);
                PrimaryKey pkAnn = field.getAnnotation(PrimaryKey.class);
                if (pkAnn != null){
                    if (StringUtil.isEmpty(field.get(updateEntity))){
                        throw new RuntimeException("更新数据必须指定全部主键列的值，列名 " + field.getName() + " 未指定值。");
                    }
                    if (pkAnn.partition()){
                        partKeyName = getKeyName(pkAnn, field);
                        partKeyValue = field.get(updateEntity);
                    }
                    keyMap.put(getKeyName(pkAnn, field), field.get(updateEntity));
                }else {
                    updateColumnsMap.put(field.getName(), this.getTsColumnValue(field.get(updateEntity)));
                }

            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        if (StringUtil.isEmpty(partKeyName)){
            throw new RuntimeException("要更新的实体未指定主键列, " + updateEntity.getClass().getName());
        }
        if (StringUtil.isEmpty(partKeyValue)){
            throw new RuntimeException("要更新的实体未指定主键列的值, " + updateEntity.getClass().getName());
        }
        RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, TsHelper.primaryKeyBuilder(keyMap));

        for (Map.Entry<String, ColumnValue> entry : updateColumnsMap.entrySet()) {
            if(StringUtil.isEmpty(entry.getValue())){
                continue;
            }
            rowUpdateChange.put(entry.getKey(), entry.getValue());
        }

        client.updateRow(new UpdateRowRequest(rowUpdateChange));
    }

    
    private com.alicloud.openservices.tablestore.model.PrimaryKey getStartPrimaryKey(T entity) throws IllegalAccessException {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        for (Field field : entity.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            PrimaryKey pkAnn = field.getAnnotation(PrimaryKey.class);
            if (pkAnn != null ) {
                if (field.get(entity) == null){
                    primaryKeyBuilder.addPrimaryKeyColumn(getKeyName(pkAnn, field), PrimaryKeyValue.INF_MIN);
                }else {
                    primaryKeyBuilder.addPrimaryKeyColumn(getKeyName(pkAnn, field), PrimaryKeyValue.fromString((String) field.get(entity)));
                }
            }
        }
        
        return primaryKeyBuilder.build();
    }

    private com.alicloud.openservices.tablestore.model.PrimaryKey getEndPrimaryKey(T entity) throws IllegalAccessException {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        for (Field field : entity.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            PrimaryKey pkAnn = field.getAnnotation(PrimaryKey.class);
            if (pkAnn != null ) {
                if (field.get(entity) == null){
                    primaryKeyBuilder.addPrimaryKeyColumn(getKeyName(pkAnn, field), PrimaryKeyValue.INF_MAX);
                }else {
                    primaryKeyBuilder.addPrimaryKeyColumn(getKeyName(pkAnn, field), PrimaryKeyValue.fromString((String) field.get(entity)));
                }
            }
        }

        return primaryKeyBuilder.build();
    }
    
    

    /**
     * 获取主键列名，如果未指定列名，则默认使用实体的filed名称
     * @param pkAnn
     * @param field
     * @return
     */
    private String getKeyName(PrimaryKey pkAnn, Field field){
        if (StringUtil.isNotEmpty(pkAnn.value())){
            return  pkAnn.value();
        }else {
            return field.getName();
        }
    }
    
    private Object getJavaValue(ColumnValue columnValue){
        if (columnValue.getType() == ColumnType.STRING){
            return columnValue.asString();
        }else if (columnValue.getType() == ColumnType.INTEGER){
            return columnValue.asLong();
        }else if (columnValue.getType() == ColumnType.DOUBLE){
            return columnValue.asDouble();
        }else if (columnValue.getType() == ColumnType.BINARY){
            return columnValue.asBinary();
        }else if (columnValue.getType() == ColumnType.BOOLEAN){
            return columnValue.asBoolean();
        }
        return null;
    }
    
    private ColumnValue getTsColumnValue(Object value){
        if (value == null){
            return null;
        }
        if (value instanceof String){
            return ColumnValue.fromString(String.valueOf(value));
        }else if (value instanceof Integer){
            return ColumnValue.fromLong(new Long(Integer.toString((Integer) value)));
        }else if (value instanceof Double){
            return ColumnValue.fromDouble((Double) value);
        }else if (value instanceof Boolean){
            return ColumnValue.fromBoolean((Boolean) value);
        }else if (value instanceof Byte){
            return ColumnValue.fromBinary((byte[]) value);
        }else if (value instanceof Long){
            return ColumnValue.fromLong((Long) value);
        }else {
            logger.error("----------不支持的ColumnValue类型：" + String.valueOf(value));
        }
        
        return null;
    }

    /**
     * 设置属性列
     * @param rowPutChange
     * @param fields
     */
    private void setRowFields(RowPutChange rowPutChange, Map<String, Object> fields){
        if (fields == null || fields.size() == 0) return;

        for(Map.Entry<String,Object> fieldMap : fields.entrySet()){
            Object obj = fieldMap.getValue();
            if(obj instanceof String){
                rowPutChange.addColumn(new Column(fieldMap.getKey(), ColumnValue.fromString(obj.toString())));
            }else if(obj instanceof Integer){
                rowPutChange.addColumn(new Column(fieldMap.getKey(), ColumnValue.fromLong((Integer)obj)));
            }else if(obj instanceof byte[]){
                rowPutChange.addColumn(new Column(fieldMap.getKey(), ColumnValue.fromBinary((byte[])obj)));
            }else if(obj instanceof Boolean){
                rowPutChange.addColumn(new Column(fieldMap.getKey(), ColumnValue.fromBoolean((Boolean)obj)));
            }else if(obj instanceof Double){
                rowPutChange.addColumn(new Column(fieldMap.getKey(), ColumnValue.fromDouble((Double)obj)));
            }
        }
    }
    
    
}
