package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

/**
 * Created by shahsid104 on 4/30/2017.
 */

public class Message implements Serializable {
    String key;
    String value;
    int query;

    Message(){}

    Message(String key, String value, int query)
    {
        this.key = key;
        this.value = value;
        this.query = query;
    }
}
