package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class SimpleDynamoProvider extends ContentProvider {

    private static final int    SERVER_PORT = 11108;
    private static final String DEBUG       = "DEBUG";
    private static final String LOG         = "LOG";
    private static final String EXCEPTION   = "EXCEPTION";
    private static final String MASTER_AVD  = "5554";
    private static final String MASTER_PORT = "11108";
    private static final String SEPARATOR   = ":";

    private static final byte MSG_TYPE_NODE_POSITION_REQ  = 0;
    private static final byte MSG_TYPE_NODE_POSITION_RESP = 1;
    private String port;
    private String avdPort;

    private String hashedPort;
    private int insertReplyCounter = 0;

    boolean notifyRequiredQuery;
    boolean notifyRequiredDelete;
    boolean notifyRequiredInsert;

    private static final String TYPE_INSERT         = "INSERT";
    private static final String TYPE_INSERT_REPLY   = "INSERT_REPLY";
    private static final String TYPE_QUERY          = "QUERY";
    private static final String TYPE_QUERY_REPLY    = "QUERY_REPLY";
    private static final String TYPE_NW_QUERY       = "NW_QUERY";
    private static final String TYPE_NW_QUERY_REPLY = "NW_QUERY_REPLY";
    private static final String TYPE_DELETE         = "DELETE";
    private static final String TYPE_DELETE_REPLY   = "DELETE_REPLY";
    private static final String TYPE_NW_DELETE      = "NW_DELETE";

    Node               successor;
    CircularLinkedList chord;

    Lock      lock;
    Condition condition;
    boolean   isServerCreated;

    Object queryMonitor;
    int    queryReplyCounter;
    Object insertReplyMonitor;
    Object networkQueryMonitor;
    int    networkQueryReplyCounter;
    Object deleteMonitor;
    int    deleteReplyCounter;

    MatrixCursor sharedMatrixCursor = null;

    String[] columns = new String[]{"key", "value"};

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        int count = 0;
        Log.d(DEBUG, "Delete request received with selection " + selection);
        try {
            if (selection.equals("\"*\"")) {
                deleteNetworkContent();
                count++;
            } else {
                Node coordinatorNode = findCoordinator(selection);
                Log.d(DEBUG,
                      "coordinatorNode for the delete req with selection -" + selection + " is " + coordinatorNode
                              .toString());
                Log.d(DEBUG, "coordinatorNode.next.value.port -" + coordinatorNode.next.value.port);
                Log.d(DEBUG, "coordinatorNode.next.next.value.port -" + coordinatorNode.next.next.value.port);

                deleteReplyCounter = 0;
                deleteMonitor = new Object();
                notifyRequiredDelete = true;

                new Thread(new ClientTask(coordinatorNode.value.port,
                                          TYPE_DELETE + SEPARATOR + avdPort + SEPARATOR + selection)).start();
                new Thread(new ClientTask(coordinatorNode.next.value.port,
                                          TYPE_DELETE + SEPARATOR + avdPort + SEPARATOR + selection)).start();
                new Thread(new ClientTask(coordinatorNode.next.next.value.port,
                                          TYPE_DELETE + SEPARATOR + avdPort + SEPARATOR + selection)).start();
                try {
                    synchronized (deleteMonitor) {
                        if (notifyRequiredDelete) {
                            deleteMonitor.wait();
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            Log.d(DEBUG, e.getMessage(), e);
        }
        return count;
    }

    private void deleteNetworkContent() {
        new Thread(new ClientTask(successor.toString(), TYPE_NW_DELETE + SEPARATOR + avdPort));
    }

    private int deleteSpecificFile(String selection) {
        int count = 0;
        Log.d(DEBUG, "Delete request made with selection = " + selection + " to port " + avdPort);
        Log.d(DEBUG,
              "Trying to delete the file at path = " + getContext().getFilesDir() + File.separator + selection);
        File fileToBeDeleted = new File(getContext().getFilesDir() + File.separator + selection);
        Log.d(DEBUG, "File exists = " + fileToBeDeleted.isFile());

        if (fileToBeDeleted.delete()) {
            count++;
        }

        Log.d(DEBUG, "Delete count = " + count);
        return count;
    }

    private int deleteLocalContent() {
        int count = 0;
        Log.d(DEBUG, "Delete request made with @. Deleting all local content");
        Log.d(DEBUG, "Iterating through local files and deleting");

        for (File currentLocalFile : getContext().getFilesDir().listFiles()) {
            boolean delete = currentLocalFile.delete();
            if (delete) {
                count++;
            }
        }
        Log.d(DEBUG, "Delete count for @ - " + count);
        return count;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean onCreate() {
        try {
            Log.d(DEBUG, "Content provider created");

            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context
                                                                                            .TELEPHONY_SERVICE);
            avdPort = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            port = String.valueOf((Integer.parseInt(avdPort) * 2));

            Log.d(DEBUG, "Telephony manager setup done.");

            //TODO handle exception appropriately
            try {
                hashedPort = genHash(avdPort);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            Log.d(DEBUG, "avdPort = " + avdPort);
            Log.d(DEBUG, "port = " + port);

            chord = new CircularLinkedList();
            chord.insert("5554", genHash("5554"));
            chord.insert("5556", genHash("5556"));
            chord.insert("5558", genHash("5558"));
            chord.insert("5560", genHash("5560"));
            chord.insert("5562", genHash("5562"));

            Log.d(DEBUG, "Chord created");

            Node current = chord.root;

            do {
                if (current.value.port.equals(avdPort)) {
                    break;
                }
                current = current.next;
            } while (current != chord.root);

            successor = current.next;

            Log.d(DEBUG,
                  "Preference list for the current node - " + avdPort + ", " + successor.value.port + ", " +
                          "" + current.next.next.value.port);
            Log.d(DEBUG, "=================================");
            Thread serverThread = new Thread(new ServerTask());
            serverThread.start();

        } catch (Exception e) {
            //TODO - remove this after testing
            Log.e(DEBUG, e.getMessage(), e);
        }
        return false;
    }

    private Node findCoordinator(String msg) {
        Node coordinator = null;
        try {
            String hashedMsg = genHash(msg);

            Node current = chord.root;
            do {
                System.out.print(current.value.port + "-" + current.value.hash + ",");

                if (hashedMsg.compareTo(current.value.hash) > 0 && hashedMsg.compareTo(current.next.value.hash) <= 0) {
                    coordinator = current.next;
                } else if (hashedMsg.compareTo(current.value.hash) <= 0 && current == chord.root) {
                    coordinator = chord.root;
                } else if (hashedMsg.compareTo(current.value.hash) > 0 && current.next == chord.root) {
                    coordinator = chord.root;
                }
                current = current.next;
            } while (current != chord.root);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return coordinator;
    }

    @Override
    synchronized public Uri insert(Uri uri, ContentValues values) {
        try {
            Log.d(DEBUG, "Inserting - " + values.toString());

            String key = values.get("key").toString();
            String value = values.get("value").toString();

            try {
                String hashedKey = genHash(key);

                Node coordinator = findCoordinator(key);

                Log.d(DEBUG, "Hashed value for key - " + key + " is " + hashedKey);

                Log.d(DEBUG, "From Node " + avdPort + " sending insert request for " + key + "-" + value + " to nodes "
                        + coordinator.value.port + ", " + coordinator.next.value.port + ", "
                        + coordinator.next.next.value.port);

                insertReplyCounter = 0;
                insertReplyMonitor = new Object();
                notifyRequiredInsert = true;

                new Thread(new ClientTask(coordinator.value.port,
                                          TYPE_INSERT + SEPARATOR + key + SEPARATOR + value + SEPARATOR +
                                                  avdPort)).start();
                new Thread(new ClientTask(coordinator.next.value.port,
                                          TYPE_INSERT + SEPARATOR + key + SEPARATOR + value + SEPARATOR +
                                                  avdPort)).start();
                new Thread(new ClientTask(coordinator.next.next.value.port,
                                          TYPE_INSERT + SEPARATOR + key + SEPARATOR + value + SEPARATOR +
                                                  avdPort)).start();
                try {
                    synchronized (insertReplyMonitor) {
                        if (notifyRequiredInsert) {
                            insertReplyMonitor.wait();
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } catch (NoSuchAlgorithmException e) {
                Log.e(DEBUG, e.getMessage(), e);
            }

        } catch (Exception e) {
            //TODO - remove this after testing
            Log.e(DEBUG, e.getMessage(), e);
        }
        return null;
    }

    private File writeToLocalFile(String key, String value) {

        File file = new File(getContext().getFilesDir(), key);
        try (
                FileOutputStream outputStream = new FileOutputStream(file);
        ) {
            outputStream.write(value.getBytes());
            Log.d(DEBUG, "Wrote the key-value to the server");

            outputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return file;
    }

    @Override
    synchronized public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        try {
            Log.d(DEBUG, "Querying with selection = " + selection);

            sharedMatrixCursor = null;
            if (selection.equals("\"@\"")) {
                sharedMatrixCursor = queryLocalContent();
            } else if (selection.equals("\"*\"")) {
                sharedMatrixCursor = queryNetworkContent();
            } else {
                //send request to the 3 responsible nodes and wait till you hear back from at least 2
                try {
                    Node coordinator = findCoordinator(selection);
                    Log.d(DEBUG, "From Node " + avdPort + " sending query request for " + selection + " to nodes "
                            + coordinator.value.port + ", " + coordinator.next.value.port + ", "
                            + coordinator.next.next.value.port);

                    notifyRequiredQuery = true;
                    queryReplyCounter = 0;
                    queryMonitor = new Object();

                    new Thread(new ClientTask(coordinator.value.port,
                                              TYPE_QUERY + SEPARATOR + avdPort + SEPARATOR + selection))
                            .start();
                    new Thread(new ClientTask(coordinator.next.value.port,
                                              TYPE_QUERY + SEPARATOR + avdPort + SEPARATOR + selection))
                            .start();
                    new Thread(new ClientTask(coordinator.next.next.value.port,
                                              TYPE_QUERY + SEPARATOR + avdPort + SEPARATOR + selection))
                            .start();


                    synchronized (queryMonitor) {
                        if (notifyRequiredQuery) {
                            queryMonitor.wait();
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Log.d(DEBUG, "Wait is over. Starting query code again for " + selection);
                sharedMatrixCursor.moveToFirst();
                Log.d(DEBUG, "Cursor count = " + sharedMatrixCursor.getCount());
                Log.d(DEBUG,
                      "Cursor values = " + sharedMatrixCursor.getString(0) + ":" + sharedMatrixCursor.getString(1));
            }
        } catch (Exception e) {
            //TODO - remove this after testing
            Log.e(DEBUG, e.getMessage(), e);
        }
        return sharedMatrixCursor;
    }

    private MatrixCursor queryLocalContent() {
        String[] columns = new String[]{"key", "value"};
        MatrixCursor matrixCursor = new MatrixCursor(columns);

        Log.d(DEBUG, "Begin local search");

        for (File currentLocalFile : getContext().getFilesDir().listFiles()) {
            Log.d(DEBUG, "Retrieving content for local file = " + currentLocalFile.getName());
            try (
                    InputStreamReader inputStreamReader = new InputStreamReader(new
                                                                                        FileInputStream(
                            currentLocalFile));
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            ) {

                StringBuilder stringBuilder = new StringBuilder();
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    stringBuilder.append(line);
                }
                matrixCursor.addRow(new Object[]{currentLocalFile.getName(),
                                                 stringBuilder.toString()});
            } catch (FileNotFoundException e) {
                Log.e(DEBUG, e.getMessage(), e);
            } catch (IOException e) {
                Log.e(DEBUG, e.getMessage(), e);
            }

        }
        return matrixCursor;
    }

    private MatrixCursor queryNetworkContent() {
        Log.d(DEBUG, "Network search initiated");
        try {
            sharedMatrixCursor = null;
            networkQueryReplyCounter = 0;
            networkQueryMonitor = new Object();
            synchronized (networkQueryMonitor) {
                String message = TYPE_NW_QUERY + SEPARATOR + avdPort;
                Log.d(DEBUG, "Sending a network query request to successor(" + successor + ") Message = " + message);

                new Thread(new ClientTask(successor.toString(), message)).start();

                Log.d(DEBUG, "Network query routed to the next node. Waiting this thread");
                networkQueryMonitor.wait();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //Make a matrix cursor here
        return sharedMatrixCursor;
    }

    private MatrixCursor querySpecificContent(String selection) {
        Log.d(DEBUG, "Querying locally for the key = " + selection);
        MatrixCursor matrixCursor = null;

        try (
                InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream
                                                                                    (getContext().getFilesDir()
                                                                                                 .getPath() + File
                                                                                            .separatorChar +
                                                                                             selection));
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        ) {
            matrixCursor = new MatrixCursor(columns);
            StringBuilder stringBuilder = new StringBuilder();
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
            }

            Log.d(DEBUG, "Adding the key-value pair = (" + selection + "-" + stringBuilder
                    .toString() + ")" + " to the cursor");
            matrixCursor.addRow(new Object[]{selection, stringBuilder.toString()});
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return matrixCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    class ServerTask implements Runnable {

        private static final int SERVER_RUNNING_PORT = 10000;

        @Override
        public void run() {
            try {
                Log.d(DEBUG, "Server thread created");
                try (ServerSocket serverSocket = new ServerSocket(SERVER_RUNNING_PORT)) {
                    Socket client = null;
                    while ((client = serverSocket.accept()) != null) {

                        String line = new BufferedReader(new InputStreamReader(client
                                                                                       .getInputStream())).readLine();
                        String[] split = line.split(SEPARATOR);
                        Log.d(DEBUG, "ServerTask message received = " + line);

                        if (split[0].equals(TYPE_INSERT)) {
                            //Insert request from coordinator
                            //"1" + SEPARATOR + key + SEPARATOR + value + SEPARATOR + avdPort
                            String key = split[1];
                            String value = split[2];
                            String requestingNode = split[3];
                            writeToLocalFile(key, value);

                            Log.d(DEBUG,
                                  "Completed insert req from coordinator for key - " + key + " value -" +
                                          value);

                            new Thread(new ClientTask(requestingNode,
                                                      TYPE_INSERT_REPLY + SEPARATOR + avdPort + SEPARATOR + key
                                                              + SEPARATOR + value))
                                    .start();

                        } else if (split[0].equals(TYPE_INSERT_REPLY)) {
                            //Insert successful message from a node in preference list
                            Log.d(DEBUG,
                                  "Received insert completion message from Node - " + split[1] + " for key-value=" +
                                          split[2] + "-" + split[3]);

                            insertReplyCounter++;

                            Log.d(DEBUG,
                                  "incremented the insert reply counter. insertReplyCounter = " + insertReplyCounter);

                            if (insertReplyCounter == 3) {
                                synchronized (insertReplyMonitor) {
                                    notifyRequiredInsert = false;
                                    Log.d(DEBUG,
                                          "notifying insert method "+split[2]);
                                    insertReplyMonitor.notifyAll();
                                }
                            }

                        } else if (split[0].equals(TYPE_QUERY)) {
                            //Query request from a Node
                            //TYPE_QUERY + SEPARATOR + avdPort + SEPARATOR + selection
                            Log.d(DEBUG, "TYPE_QUERY - line = " + line);

                            String requestingPort = split[1];
                            String selection = split[2];

                            MatrixCursor matrixCursor = querySpecificContent(selection);
                            matrixCursor.moveToFirst();
                            StringBuilder stringBuilder =
                                    new StringBuilder(TYPE_QUERY_REPLY + SEPARATOR + avdPort + SEPARATOR);
                            while (!matrixCursor.isAfterLast()) {
                                stringBuilder.append(matrixCursor.getString(0)).append(SEPARATOR)
                                             .append(matrixCursor.getString(1));
                                matrixCursor.moveToNext();
                            }
                            new Thread(new ClientTask(requestingPort, stringBuilder.toString())).start();

                        } else if (split[0].equals(TYPE_QUERY_REPLY)) {
                            //query and reply to this node
                            //TYPE_QUERY_REPLY + SEPARATOR + avdPort + SEPARATOR

                            Log.d(DEBUG, "TYPE_QUERY_REPLY - " + line);
                            String queryingPort = split[1];
                            queryReplyCounter++;
                            if (sharedMatrixCursor == null) {
                                sharedMatrixCursor = new MatrixCursor(columns);
                            }

                            for (int i = 2; i < split.length; i += 2) {
                                sharedMatrixCursor.addRow(new Object[]{split[i], split[i + 1]});
                            }

                            Log.d(DEBUG, "queryReplyCounter = " + queryReplyCounter);
                            if (queryReplyCounter == 3) {
                                synchronized (queryMonitor) {
                                    notifyRequiredQuery = false;
                                    Log.d(DEBUG,
                                          "notifying query method "+split[2]);
                                    queryMonitor.notifyAll();
                                }
                            }

                        } else if (split[0].equals(TYPE_NW_QUERY)) {
                            //network search
                            String requestingPort = split[1];

                            MatrixCursor localSearchCursor = queryLocalContent();
                            localSearchCursor.moveToFirst();
                            StringBuilder stringBuilder =
                                    new StringBuilder(TYPE_NW_QUERY_REPLY + SEPARATOR + avdPort + SEPARATOR);
                            while (!localSearchCursor.isAfterLast()) {
                                stringBuilder.append(localSearchCursor.getString(0) + SEPARATOR +
                                                             localSearchCursor.getString(1) + SEPARATOR);
                                localSearchCursor.moveToNext();
                            }
                            new Thread(new ClientTask(requestingPort, stringBuilder.toString())).start();

                            if (!requestingPort.equals(avdPort)) {
                                //Don't forward if this is the requesting port.
                                new Thread(new ClientTask(successor.value.port, line)).start();
                            }

                        } else if (split[0].equals(TYPE_NW_QUERY_REPLY)) {
                            //network search
                            //collate the data and finish MatrixCursor. notify the lock once done.
                            String[] data = line.split(SEPARATOR);
                            String replyingPort = data[1];
                            networkQueryReplyCounter++;
                            if (sharedMatrixCursor == null) {
                                sharedMatrixCursor = new MatrixCursor(columns);
                            }

                            for (int i = 2; i < data.length; i += 2) {
                                sharedMatrixCursor.addRow(new Object[]{data[i], data[i + 1]});
                            }

                            if (networkQueryReplyCounter == 5) {
                                synchronized (networkQueryMonitor) {
                                    networkQueryMonitor.notifyAll();
                                }
                            }
                        } else if (split[0].equals(TYPE_DELETE)) {
                            //TYPE_DELETE + SEPARATOR + avdPort + SEPARATOR + selection
                            Log.d(DEBUG, "TYPE_DELETE - " + line);
                            String requestingPort = split[1];
                            String selection = split[2];

                            if (selection.equals("\"@\"")) {
                                deleteLocalContent();
                            } else {
                                deleteSpecificFile(selection);
                            }

                            new Thread(new ClientTask(requestingPort, TYPE_DELETE_REPLY + SEPARATOR + avdPort)).start();

                        } else if (split[0].equals(TYPE_DELETE_REPLY)) {
                            //TYPE_DELETE_REPLY + SEPARATOR + avdPort
                            String requestingPort = split[1];
                            Log.d(DEBUG, "Current deletion counter - " + deleteReplyCounter);
                            deleteReplyCounter++;
                            if (deleteReplyCounter == 3) {
                                synchronized (deleteMonitor) {
                                    notifyRequiredDelete = false;
                                    deleteMonitor.notifyAll();
                                }
                            }
                        } else if (split[0].equals(TYPE_NW_DELETE)) {
                            String[] data = line.split(SEPARATOR);
                            String deleteInitiatorPort = data[1];
                            Log.d(DEBUG, "Deleting local files");
                            deleteLocalContent();
                            if (!deleteInitiatorPort.equals(successor.toString())) {
                                //forward to the next node
                                Log.d(DEBUG, "Local files deleted. Forwarding the request to the next node");
                                new Thread(new ClientTask(successor.toString(), line));
                            } else {
                                Log.d(DEBUG, "Loop complete. Done deleting");
                            }
                        }
                        client.close();
                    }
                } catch (IOException e) {
                    Log.e(DEBUG, e.getMessage(), e);
                }
            } catch (Exception e) {
                //TODO - remove this after testing
                Log.e(DEBUG, e.getMessage(), e);
            }
        }


    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    class ClientTask implements Runnable {

        String portToWrite;
        String messageToServer;

        ClientTask(String portToWrite, String messageToServer) {
            this.portToWrite = portToWrite;
            this.messageToServer = messageToServer;
        }

        @Override
        public void run() {
            try {
                try (
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                   Integer.parseInt(portToWrite) * 2);
                        OutputStream os = socket.getOutputStream();
                ) {

//                    Log.d(DEBUG, "ClientThread - Sending the message - " + messageToServer + " to - " + portToWrite);
                    os.write(messageToServer.getBytes());
                    os.flush();
//                    Log.d(DEBUG, "ClientThread - Message " + messageToServer + " flushed from client to - " +
// portToWrite);

                    socket.close();
                } catch (IOException e) {
                    Log.e(DEBUG, e.getMessage(), e);
                }
            } catch (Exception e) {
                //TODO - remove this after testing
                Log.e(DEBUG, e.getMessage(), e);
            }

        }
    }
}
