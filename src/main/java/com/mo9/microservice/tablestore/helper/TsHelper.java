package com.mo9.microservice.tablestore.helper;

import com.alicloud.openservices.tablestore.model.*;
import com.mo9.microservice.common.BytesHexTransform;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

/**
 * @author wtwei .
 * @date 2017/11/15 .
 * @time 18:19 .
 */
public class TsHelper {

    /**
     * 对传入的字符串数据进行MD5加密
     * @param source	字符串数据
     * @return   加密以后的数据
     */
    public static String MD5(String source){
        if (source == null) return null;
        MessageDigest md = null;
        byte[] bt = null;
        try {
            bt = source.getBytes("UTF-8");
            md = MessageDigest.getInstance("MD5");
            md.update(bt);
            return BytesHexTransform.bytesToHexString(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }catch (UnsupportedEncodingException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        return null;
    }

    public static String MD5With_(String source){
        if (source == null) return null;
        StringBuilder sb = new StringBuilder();
        MessageDigest md = null;
        byte[] bt = null;
        try {
            bt = source.getBytes("UTF-8");
            md = MessageDigest.getInstance("MD5");
            md.update(bt);
            return sb.append(BytesHexTransform.bytesToHexString(md.digest())).append("_").append(source).toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }catch (UnsupportedEncodingException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        return null;
    }

    public static PrimaryKey primaryKeyBuilder(Map<String, Object> keyMap){
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();

        for (Map.Entry<String, Object> entry : keyMap.entrySet()) {
            primaryKeyBuilder.addPrimaryKeyColumn(entry.getKey(), coverKeyValue(entry.getValue()));
        }
        
        return primaryKeyBuilder.build();

    }
    
    private static PrimaryKeyValue coverKeyValue(Object keyValue){
        if(keyValue instanceof String){
            return PrimaryKeyValue.fromString((String) keyValue);
        }else if(keyValue instanceof Integer){
            return PrimaryKeyValue.fromLong(Long.valueOf((Integer) keyValue));
        }else if(keyValue instanceof byte[]){
            return PrimaryKeyValue.fromBinary((byte[]) keyValue);
        }else {
            throw new RuntimeException("PrimaryKeyValue 不支持的类型转换" + keyValue.getClass().getName());
        }
    }

    public static Object row2Bean(Row row, Object bean){
        if (bean == null) {
            throw new RuntimeException("dto对象未初始化" + bean.getClass().getName());
        }
        Class clazz = bean.getClass();

        try {
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                //主键列赋值
                for (PrimaryKeyColumn primaryKeyColumn : row.getPrimaryKey().getPrimaryKeyColumns()) {
                    if (primaryKeyColumn.getName().equals(field.getName())){
                        field.set(bean, primaryKeyColumn.getValue().asString());
                    }
                }

                for (Column column : row.getColumns()) {
                    if (column.getName().equals(field.getName())){
                        ColumnValue value = column.getValue();
                        ColumnType type = value.getType();
                        if (type == ColumnType.STRING){
                            try {
                                field.set(bean, value.asString());
                            }catch (Exception e){
                                try {
                                    field.set(bean, new Integer(value.asString()));
                                } catch (Exception e1) {
                                    e.printStackTrace();
                                }
                            }
                            
                        }else if (type == ColumnType.BOOLEAN){
                            field.set(bean, value.asBoolean());
                        }else if (type == ColumnType.DOUBLE){
                            field.set(bean, value.asDouble());
                        }else if (type == ColumnType.BINARY){
                            field.set(bean, value.asBinary());
                        }else if (type == ColumnType.INTEGER){
                            if (field.getType().getSimpleName().equals("Integer") || field.getType().getSimpleName().equals("int")){
                                field.set(bean, new Integer(String.valueOf(value.asLong())));
                            }else {
                                field.set(bean, value.asLong());
                            }
                        }
                    }
                }
                
            }
            
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("row2bean failed.", e);
        }

        return bean;
    }
}
