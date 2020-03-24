package com.laozhang.entrance;

import com.laozhang.es.oper.crud.impl.UpdateByQuery;

public class UpdateByQueryTest {

    public static void main(String[] args) {

    }

    public void update() throws Exception {

        String scriptStr = "{\n" +
                "  \"script\": {\n" +
                "    \"source\": \"ctx._source.ip = 192.168.1.1\",\n" +
                "    \"lang\": \"painless\"\n" +
                "  },\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"cncNo\": \"A01\"\n" +
                "    }\n" +
                "  }\n" +
                "}";

        UpdateByQuery executor = new UpdateByQuery("cims_user_rank", "userRank");
        String response = executor.update(scriptStr).getJson();
        System.out.println(response);
    }
}
