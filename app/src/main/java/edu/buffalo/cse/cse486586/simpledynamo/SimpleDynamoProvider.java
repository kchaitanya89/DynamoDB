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
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class SimpleDynamoProvider extends ContentProvider {

    private static final String DEBUG     = "DEBUG";
    private static final String SEPARATOR = ":";

    private String port;
    private String avdPort;

    private static final String TYPE_INSERT         = "INSERT";
    private static final String TYPE_INSERT_REPLY   = "INSERT_REPLY";
    private static final String TYPE_QUERY          = "QUERY";
    private static final String TYPE_QUERY_REPLY    = "QUERY_REPLY";
    private static final String TYPE_REJOIN_QUERY   = "REJOIN_QUERY";
    private static final String TYPE_REJOIN_REPLY   = "REJOIN_REPLY";
    private static final String TYPE_NW_QUERY       = "NW_QUERY";
    private static final String TYPE_NW_QUERY_REPLY = "NW_QUERY_REPLY";
    private static final String TYPE_DELETE         = "DELETE";
    private static final String TYPE_DELETE_REPLY   = "DELETE_REPLY";
    private static final String TYPE_NW_DELETE      = "NW_DELETE";

    CircularLinkedList chord;
    Node               successor;
    Node               firstPredecessor;

    Object            serverMonitor;
    boolean           notifyRequiredServer;
    Object            queryMonitor;
    int               queryReplyCounter;
    boolean           notifyRequiredQuery;
    Object            insertReplyMonitor;
    ArrayList<String> insertReplyingNodes;
    boolean           notifyRequiredInsert;
    Object            networkQueryMonitor;
    Object            deleteMonitor;
    int               deleteReplyCounter;
    int               networkQueryReplyCounter;
    boolean           notifyRequiredDelete;
    Object            rejoinMonitor;
    int               rejoinReplyCounter;
    boolean           notifyRequiredRejoin;

    MatrixCursor sharedMatrixCursor = null;
    String[]     sharedColumns      = new String[]{"key", "value"};

    HashMap<String, Boolean> queryMap;

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public String getType(Uri uri) {
        return null;
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

    private Node findCoordinator(String msg) {
        Node coordinator = null;
        try {
            String hashedMsg = genHash(msg);

            Node current = chord.root;
            do {
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
            Log.e(DEBUG, e.getMessage(), e);
        }
        return coordinator;
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
                if (current.next.next.value.port.equals(avdPort)) {
                    firstPredecessor = current;
                }
                if (current.value.port.equals(avdPort)) {
                    successor = current.next;
                }
                current = current.next;
            } while (current != chord.root);

            Log.d(DEBUG,
                  "Preference list for the current node - " + avdPort + ", " + successor.value.port + ", " +
                          "" + successor.next.value.port);
            Log.d(DEBUG, "=================================");

            serverMonitor = new Object();
            notifyRequiredServer = true;

            Thread serverThread = new Thread(new ServerTask());
            serverThread.start();

            synchronized (serverMonitor) {
                if (notifyRequiredServer) {
                    serverMonitor.wait();
                }
            }

            rejoinMonitor = new Object();
            rejoinReplyCounter = 0;
            notifyRequiredRejoin = true;

            Log.d(DEBUG,
                  "Sending rejoin requests from " + avdPort + " to " + firstPredecessor + ", " +
                          "" + firstPredecessor.next + ", " + successor);

            //Query other nodes for data
            new Thread(new ClientTask(firstPredecessor.toString(), TYPE_REJOIN_QUERY + SEPARATOR + avdPort))
                    .start();
            new Thread(new ClientTask(firstPredecessor.next.toString(), TYPE_REJOIN_QUERY + SEPARATOR + avdPort))
                    .start();
            new Thread(new ClientTask(successor.toString(), TYPE_REJOIN_QUERY + SEPARATOR + avdPort))
                    .start();

            queryMap = new HashMap<>();
        } catch (Exception e) {
            //TODO - remove this after testing
            Log.e(DEBUG, e.getMessage(), e);
        }
        return false;
    }

    @Override
    synchronized public Uri insert(Uri uri, ContentValues values) {
        try {
            Log.d(DEBUG, "Inserting - " + values.toString());

            String key = values.get("key").toString();
            String value = values.get("value").toString();

            Node coordinator = findCoordinator(key);

            Log.d(DEBUG, "From Node " + avdPort + " sending insert request for " + key + "-" + value + " to nodes "
                    + coordinator.value.port + ", " + coordinator.next.value.port + ", "
                    + coordinator.next.next.value.port);

            insertReplyMonitor = new Object();
            insertReplyingNodes = new ArrayList<>();
            notifyRequiredInsert = true;

            ArrayList<String> currentPreferenceList = new ArrayList<>();
            currentPreferenceList.add(coordinator.value.port);
            currentPreferenceList.add(coordinator.next.value.port);
            currentPreferenceList.add(coordinator.next.next.value.port);

            new Thread(new ClientTask(coordinator.value.port,
                                      TYPE_INSERT + SEPARATOR + key + SEPARATOR + value + SEPARATOR + avdPort))
                    .start();
            new Thread(new ClientTask(coordinator.next.value.port,
                                      TYPE_INSERT + SEPARATOR + key + SEPARATOR + value + SEPARATOR + avdPort))
                    .start();
            new Thread(new ClientTask(coordinator.next.next.value.port,
                                      TYPE_INSERT + SEPARATOR + key + SEPARATOR + value + SEPARATOR + avdPort))
                    .start();

            try {
                synchronized (insertReplyMonitor) {
                    if (notifyRequiredInsert) {
                        insertReplyMonitor.wait();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
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
            queryMap.put(selection, false);

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

        queryMap.put(selection, true);
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
            matrixCursor = new MatrixCursor(sharedColumns);
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

    private MatrixCursor queryLocalContent() {
        MatrixCursor matrixCursor = new MatrixCursor(sharedColumns);

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

                String val = stringBuilder.toString();
//                int len = val.length();
                matrixCursor.addRow(new Object[]{currentLocalFile.getName(), val});

//                if (val.charAt(len - 1) == ORIGINAL_INDICATOR) {
//                    matrixCursor.addRow(new Object[]{currentLocalFile.getName(), val.substring(0, len - 1)});
//                } else {
//                    matrixCursor.addRow(new Object[]{currentLocalFile.getName(), val});
//                }

            } catch (FileNotFoundException e) {
                Log.e(DEBUG, e.getMessage(), e);
            } catch (IOException e) {
                Log.e(DEBUG, e.getMessage(), e);
            }

        }


        return matrixCursor;
    }

    private String queryLocalContentInternal() {
        Log.d(DEBUG, "Begin local search internal");

        StringBuilder stringBuilder = new StringBuilder();

        for (File currentLocalFile : getContext().getFilesDir().listFiles()) {
            Log.v(DEBUG, "Retrieving content for local file = " + currentLocalFile.getName());
            try (
                    InputStreamReader inputStreamReader = new InputStreamReader(new
                                                                                        FileInputStream(
                            currentLocalFile));
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            ) {

                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    stringBuilder.append(currentLocalFile.getName()).append(SEPARATOR).append(line).append(SEPARATOR);
                }

            } catch (FileNotFoundException e) {
                Log.e(DEBUG, e.getMessage(), e);
            } catch (IOException e) {
                Log.e(DEBUG, e.getMessage(), e);
            }

        }


        return stringBuilder.toString();
    }

    private MatrixCursor queryNetworkContent() {
        Log.d(DEBUG, "Network search initiated");
        try {
            sharedMatrixCursor = null;
            networkQueryReplyCounter = 0;
            networkQueryMonitor = new Object();
            synchronized (networkQueryMonitor) {
                String message = TYPE_NW_QUERY + SEPARATOR + avdPort;

                Node current = chord.root;
                do {
                    Log.d(DEBUG, "Sending a network query request to " + current);
                    new Thread(new ClientTask(current.toString(), message)).start();
                    current = current.next;
                } while (current != chord.root);

                ScheduledExecutorService scheduledThreadPool = Executors
                        .newScheduledThreadPool(1);
                ScheduledFuture<String> future = null;
                future = scheduledThreadPool.schedule(new Callable<String>() {
                                                          @Override
                                                          public String call() throws Exception {
                                                              String reply = "ALL NODES REPLIED";
                                                              if (networkQueryReplyCounter < 5) {
                                                                  reply = "ONE NODE FAILED TO REPLY";
                                                                  new Thread(new ClientTask(avdPort,
                                                                                            TYPE_NW_QUERY_REPLY +
                                                                                                    SEPARATOR +
                                                                                                    avdPort))
                                                                          .start();
                                                              }
                                                              return reply;
                                                          }
                                                      }, 5,
                                                      TimeUnit.SECONDS);
                try {
                    Log.d(DEBUG, future.get());
                } catch (InterruptedException | ExecutionException e) {
                    Log.e(DEBUG, e.getMessage(), e);
                }

                networkQueryMonitor.wait();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return sharedMatrixCursor;
    }

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

    private void deleteNetworkContent() {
        new Thread(new ClientTask(successor.toString(), TYPE_NW_DELETE + SEPARATOR + avdPort));
    }

    class ServerTask implements Runnable {

        private static final int SERVER_RUNNING_PORT = 10000;

        @Override
        synchronized public void run() {
            try {
                Log.d(DEBUG, "Server thread created");

                synchronized (serverMonitor) {
                    notifyRequiredServer = false;
                    serverMonitor.notifyAll();
                }
                try (ServerSocket serverSocket = new ServerSocket(SERVER_RUNNING_PORT)) {
                    Socket client = null;
                    while ((client = serverSocket.accept()) != null) {

                        String line = new BufferedReader(new InputStreamReader(client
                                                                                       .getInputStream())).readLine();
                        String[] split = line.split(SEPARATOR);
                        if (split[0].equals(TYPE_INSERT)) {
                            //Insert request from coordinator
                            //"1" + SEPARATOR + key + SEPARATOR + value + SEPARATOR + avdPort
                            Log.d(DEBUG, "TYPE_INSERT = " + line);
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

                            Log.d(DEBUG, "TYPE_INSERT completed");
                        } else if (split[0].equals(TYPE_INSERT_REPLY)) {
                            //Insert successful message from a node in preference list
                            //TYPE_INSERT_REPLY + SEPARATOR + avdPort + SEPARATOR + key + SEPARATOR + value
                            Log.d(DEBUG, "TYPE_INSERT_REPLY = " + line);
                            Log.d(DEBUG,
                                  "Received insert completion message from Node - " + split[1] + " for key-value=" +
                                          split[2] + "-" + split[3]);
                            insertReplyingNodes.add(split[1]);

                            Log.d(DEBUG,
                                  "incremented the insert reply counter. insertReplyCounter = " + insertReplyingNodes
                                          .size());

                            if (insertReplyingNodes.size() == 2) {
                                synchronized (insertReplyMonitor) {
                                    notifyRequiredInsert = false;
                                    Log.d(DEBUG,
                                          "notifying insert method " + split[2]);
                                    insertReplyMonitor.notifyAll();
                                }
                            }

                            Log.d(DEBUG, "TYPE_INSERT_REPLY completed");

                        } else if (split[0].equals(TYPE_QUERY)) {
                            //Query request from a Node
                            //TYPE_QUERY + SEPARATOR + avdPort + SEPARATOR + selection
                            Log.d(DEBUG, "TYPE_QUERY = " + line);

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

                            Log.d(DEBUG, "TYPE_QUERY completed");

                        } else if (split[0].equals(TYPE_QUERY_REPLY)) {
                            //query and reply to this node
                            //TYPE_QUERY_REPLY + SEPARATOR + avdPort + SEPARATOR

                            Log.d(DEBUG, "TYPE_QUERY_REPLY = " + line);
                            String queryingPort = split[1];
                            if (!queryMap.get(split[2])) {
                                queryReplyCounter++;
                            }

                            if (sharedMatrixCursor == null) {
                                sharedMatrixCursor = new MatrixCursor(sharedColumns);
                            }

                            for (int i = 2; i < split.length; i += 2) {
                                String currentKey = split[i];
                                String currentValue = split[i + 1];
                                sharedMatrixCursor.addRow(new Object[]{currentKey, currentValue});
                            }

                            Log.d(DEBUG, "queryReplyCounter = " + queryReplyCounter);
                            if (queryReplyCounter == 2) {
                                synchronized (queryMonitor) {
                                    notifyRequiredQuery = false;
                                    Log.d(DEBUG,
                                          "notifying query method " + split[2]);
                                    queryMonitor.notifyAll();
                                }
                            }

                            Log.d(DEBUG, "TYPE_QUERY_REPLY completed");

                        } else if (split[0].equals(TYPE_NW_QUERY)) {
                            //network search
                            Log.d(DEBUG, "TYPE_NW_QUERY = " + line);
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

//                            if (!requestingPort.equals(avdPort)) {
//                                //Don't forward if this is the requesting port.
//                                new Thread(new ClientTask(successor.value.port, line)).start();
//                            }

                            Log.d(DEBUG, "TYPE_NW_QUERY completed");

                        } else if (split[0].equals(TYPE_NW_QUERY_REPLY)) {
                            //network search
                            //collate the data and finish MatrixCursor. notify the lock once done.
                            Log.d(DEBUG, "TYPE_NW_QUERY_REPLY = " + line);

                            String[] data = line.split(SEPARATOR);
                            String replyingPort = data[1];
                            networkQueryReplyCounter++;
                            if (sharedMatrixCursor == null) {
                                sharedMatrixCursor = new MatrixCursor(sharedColumns);
                            }

                            for (int i = 2; i < data.length; i += 2) {
                                String currentKey = data[i];
                                String currentValue = data[i + 1];
                                sharedMatrixCursor.addRow(new Object[]{currentKey, currentValue});

//                                if (currentValue.charAt(currentValue.length() - 1) == ORIGINAL_INDICATOR) {
//                                    sharedMatrixCursor.addRow(new Object[]{currentKey, currentValue
//                                            .substring(0, currentValue.length() - 1)});
//                                } else {
//                                    sharedMatrixCursor.addRow(new Object[]{currentKey, currentValue});
//                                }
                            }

                            if (networkQueryReplyCounter == 5) {
                                synchronized (networkQueryMonitor) {
                                    networkQueryMonitor.notifyAll();
                                }
                            }

                            Log.d(DEBUG, "TYPE_NW_QUERY_REPLY completed");

                        } else if (split[0].equals(TYPE_DELETE)) {
                            //TYPE_DELETE + SEPARATOR + avdPort + SEPARATOR + selection
                            Log.d(DEBUG, "TYPE_DELETE = " + line);

                            String requestingPort = split[1];
                            String selection = split[2];

                            if (selection.equals("\"@\"")) {
                                deleteLocalContent();
                            } else {
                                deleteSpecificFile(selection);
                            }

                            new Thread(new ClientTask(requestingPort, TYPE_DELETE_REPLY + SEPARATOR + avdPort)).start();

                            Log.d(DEBUG, "TYPE_DELETE completed");

                        } else if (split[0].equals(TYPE_DELETE_REPLY)) {
                            //TYPE_DELETE_REPLY + SEPARATOR + avdPort
                            Log.d(DEBUG, "TYPE_DELETE_REPLY = " + line);

                            String requestingPort = split[1];
                            Log.d(DEBUG, "Current deletion counter - " + deleteReplyCounter);
                            deleteReplyCounter++;
                            if (deleteReplyCounter == 2) {
                                synchronized (deleteMonitor) {
                                    notifyRequiredDelete = false;
                                    deleteMonitor.notifyAll();
                                }
                            }

                            Log.d(DEBUG, "TYPE_DELETE_REPLY completed");

                        } else if (split[0].equals(TYPE_NW_DELETE)) {
                            Log.d(DEBUG, "TYPE_NW_DELETE = " + line);

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

                            Log.d(DEBUG, "TYPE_NW_DELETE completed");
                        } else if (split[0].equals(TYPE_REJOIN_QUERY)) {
                            //Query request from a Node
                            //TYPE_REJOIN_QUERY + SEPARATOR + avdPort
                            Log.d(DEBUG, "TYPE_REJOIN_QUERY = " + line);

                            String requestingPort = split[1];

                            StringBuilder stringBuilder =
                                    new StringBuilder(
                                            TYPE_REJOIN_REPLY + SEPARATOR + avdPort + SEPARATOR +
                                                    queryLocalContentInternal());
                            String rejoinReplyMsg = stringBuilder.toString();
                            int len = rejoinReplyMsg.length();
                            if (rejoinReplyMsg.charAt(len - 1) == SEPARATOR.charAt(0)) {
                                rejoinReplyMsg = rejoinReplyMsg.substring(0, len - 1);
                            }
                            new Thread(new ClientTask(requestingPort, rejoinReplyMsg)).start();

                            Log.d(DEBUG, "TYPE_REJOIN_QUERY completed");
                        } else if (split[0].equals(TYPE_REJOIN_REPLY)) {
                            //network search
                            //collate the data and finish MatrixCursor. notify the lock once done.
                            Log.d(DEBUG, "TYPE_REJOIN_REPLY = " + line);

                            String[] data = line.split(SEPARATOR);
                            String replyingPort = data[1];

                            for (int i = 2; i < data.length; i += 2) {
                                String currentKey = data[i];
                                String currentValue = data[i + 1];
                                // sharedMatrixCursor.addRow(new Object[]{data[i], data[i + 1]});
                                Node coordinator = findCoordinator(currentKey);
                                if (coordinator.equals(firstPredecessor) || coordinator
                                        .equals(firstPredecessor.next) || coordinator.toString().equals(avdPort)) {
                                    writeToLocalFile(currentKey, currentValue);
                                }

//                                if (firstPredecessor.toString().equals(replyingPort) || firstPredecessor.next
// .toString()
//
// .equals(replyingPort)) {
//                                    if (currentValue.charAt(currentValue.length() - 1) == ORIGINAL_INDICATOR) {
//                                        currentValue = currentValue.substring(0, currentValue.length() - 1);
//                                        writeToLocalFile(currentKey, currentValue);
//                                        Log.d(DEBUG,
//                                              "Wrote key(" + currentKey + ")-(" + currentValue + ") to the server");
//                                    }
//                                } else {
//                                    if (currentValue.charAt(currentValue.length() - 1) != ORIGINAL_INDICATOR) {
//                                        currentValue = currentValue + ORIGINAL_INDICATOR;
//                                        writeToLocalFile(currentKey, currentValue);
//                                        Log.d(DEBUG,
//                                              "Wrote key(" + currentKey + ")-(" + currentValue + ") to the server");
//                                    }
//                                }
                            }

                            Log.d(DEBUG, "TYPE_REJOIN_REPLY completed");

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

                    Log.v(DEBUG, "ClientTask - Sending the message - " + messageToServer + " to - " + portToWrite);
                    os.write(messageToServer.getBytes());
                    os.flush();
                    Log.v(DEBUG, "ClientTask - Message " + messageToServer + " flushed from client to - " +
                            portToWrite);

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
