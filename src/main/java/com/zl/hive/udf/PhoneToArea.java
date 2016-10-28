package com.zl.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;

/**
 *
 *  自定义的工具类，实现UDF类，编写public类型的evaluate方法。
 *  evaluate的参数，为将来调用的参数个数及类型
 *
 * Created by jacky on 2016/10/27.
 */
public class PhoneToArea extends UDF {

    private static HashMap<String, String> areaMap = new HashMap<>();

    static {
        areaMap.put("1388", "beijing");
        areaMap.put("1389", "tianjing");
        areaMap.put("1399", "nanjing");
        areaMap.put("1366", "shanghai");
    }

    /**
     * 一定要定义为public，才能被hive调用
     * @param phoneNumber
     * @return
     */
    public String evaluate(String phoneNumber) {

        String result = areaMap.get(phoneNumber).substring(0, 4) == null ? (phoneNumber +
                "    huoxing") : (phoneNumber + "   " + areaMap.get(phoneNumber));
        return result;

    }
}
