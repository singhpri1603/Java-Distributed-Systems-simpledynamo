package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by priyanka on 19/04/16.
 */
public class Message implements Serializable {
    String tag;
    String origPort;
    String mulPort;
    String key;
    String value;
    String filename;
    HashMap<String, String> values =new HashMap<String, String>();
    HashMap<String, String> backup =new HashMap<String, String>();
}
