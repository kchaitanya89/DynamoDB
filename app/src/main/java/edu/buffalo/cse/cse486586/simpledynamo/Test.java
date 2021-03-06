package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Chaitanya on 4/20/15.
 */

public class Test{
    static CircularLinkedList chord ;
    public static void main(String[] args) {
        try{
            TreeMap<String,String> map = new TreeMap<>();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception");
        }
    }

    private static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private static String findCoordinator(String msg) {
        String coordinator = null;
        try {
            String hashedMsg = genHash(msg);

            Node current = chord.root;
            do {
                System.out.print(current.value.port + "-" + current.value.hash + ",");

                if(hashedMsg.compareTo(current.value.hash)>0 && hashedMsg.compareTo(current.next.value.hash)<=0){
                    coordinator = current.next.toString();
                }else if(hashedMsg.compareTo(current.value.hash)<=0 && current == chord.root){
                    coordinator = current.value.port;
                }else if(hashedMsg.compareTo(current.value.hash)>0 && current.next == chord.root){
                    coordinator = chord.root.value.port;
                }
                current = current.next;
            } while (current != chord.root);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return coordinator;
    }
}