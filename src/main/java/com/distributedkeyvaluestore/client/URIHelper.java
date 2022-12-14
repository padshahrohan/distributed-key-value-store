package com.distributedkeyvaluestore.client;

import java.net.URI;

public class URIHelper {

    private URIHelper() {

    }

    public static URI createURI(String hostName) {
        return URI.create("http://" + hostName + ":8080");
    }

}
