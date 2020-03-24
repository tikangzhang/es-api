package com.laozhang.entrance;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.laozhang.es.oper.crud.impl.DeleteByQuery;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DeleteByQueryTest {

    public static void main(String[] args) {
        try {


            JSONObject jsonObject = new JSONObject();
            JSONObject query = new JSONObject();
            JSONObject match = new JSONObject();
            Date date = new Date();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            match.put("startLogTime",dateFormat.parse("2019-11-28 08:00:00"));
            query.put("match",match);
            jsonObject.put("query",query);
            System.err.println(jsonObject.toJSONString());
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /**
     * 删除临时表中的数据，当前班别
     * @param startDateTime
     * @param endDateTime
     * @return
     * @throws IOException
     */
    public int deleteUserRankTempByClassType(String startDateTime, String endDateTime) throws Exception {
        JSONObject jsonObject = new JSONObject();
        JSONObject query = new JSONObject();
        JSONObject match = new JSONObject();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        match.put("startLogTime",dateFormat.parse(startDateTime));
        query.put("match",match);
        jsonObject.put("query",query);
        System.err.println(jsonObject.toJSONString());
        DeleteByQuery deleteByQueryExecutor = new DeleteByQuery("cims_user_rank", "userRank").delete(jsonObject.toJSONString());
        JSONObject responseJsonObject = JSON.parseObject(deleteByQueryExecutor.getJson());
        System.err.println(responseJsonObject.toJSONString());
        return responseJsonObject.getInteger("deleted");
    }
}
