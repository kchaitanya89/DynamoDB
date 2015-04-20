package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by Chaitanya on 4/18/15.
 */
public class CircularLinkedList {

    Node root  = null;
    int  count = 0;

    public static void main(String[] args) {

        CircularLinkedList cll = new CircularLinkedList();
        //Checking ring formation
        cll.insert("5556", "208f7f72b198dadd244e61801abe1ec3a4857bc9");
        cll.insert("5560", "c25ddd596aa7c81fa12378fa725f706d54325d12");
        cll.insert("5554", "33d6357cfaaf0f72991b0ecd8c56da066613c089");
        cll.insert("5558", "abf0fd8db03e5ecb199a9b82929e9db79b909643");
        cll.insert("5562", "177ccecaec32c54b82d5aaafc18a2dadb753e3b1");

        Node current = cll.root;

        do {
            System.out.println(current.value.port);
            current = current.next;
        } while (current != cll.root);

        cll.printList();
    }

    void insert(String port, String hash) {
        count++;
        Port n = new Port(port, hash);
        //firstly if it is null
        if (root == null) {
            root = new Node(port, hash);
            return;
        } else if (root.next == root) {
            //add this node
            root.next = new Node(port, hash);
            root.next.next = root;
            root = root.value.hash.compareTo(n.hash) < 0 ? root : root.next;
            return;
        } else if (0 < root.value.hash.compareTo(n.hash)) {
            Node current = root;
            while (current.next != root) {
                current = current.next;
            }
            current.next = new Node(port, hash);
            current.next.next = root;
            root = current.next;
            return;
        }
        Node current = root;
        while (current.next != root && current.next.value.hash.compareTo(n.hash) <= 0) {
            current = current.next;
        }
        Node currentNext = current.next;
        current.next = new Node(port, hash);
        current.next.next = currentNext;
    }

    public void printList() {
        if (root == null) {
            return;
        }
        Node current = root;
        do {
            System.out.print(current.value.port + "-" + current.value.hash + ",");
            current = current.next;
        } while (current != root);
    }
}

class Node {
    Port    value;
    Node    next;

    public Node(String port, String hash) {
        value = new Port(port, hash);
        next = this;
    }

    @Override
    public String toString() {
        return value.toString();
    }
}

class Port implements Comparable {
    String port;
    String hash;

    Port(String port, String hash) {
        this.port = port;
        this.hash = hash;
    }

    @Override
    public int compareTo(Object another) {
        if (another instanceof Port) {
            return this.hash.compareTo(((Port) another).hash);
        } else {
            return -1;
        }
    }

    @Override
    public String toString() {
        return port;
    }
}
