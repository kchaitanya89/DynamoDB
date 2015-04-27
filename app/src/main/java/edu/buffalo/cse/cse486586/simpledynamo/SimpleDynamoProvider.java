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
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class SimpleDynamoProvider extends ContentProvider {

    private String port;
    private String avdPort;

    private static final String DEBUG               = "DEBUG";
    private static final String SEPARATOR           = ":";
    private static final String TIMESTAMP_DELIMITER = "-";

    private static final String TYPE_INSERT         = "INSERT_FROM";
    private static final String TYPE_INSERT_REPLY   = "INSERT_REPLY_FROM";
    private static final String TYPE_QUERY          = "QUERY_FROM";
    private static final String TYPE_QUERY_REPLY    = "QUERY_REPLY_FROM";
    private static final String TYPE_REJOIN_QUERY   = "REJOIN_QUERY_FROM";
    private static final String TYPE_REJOIN_REPLY   = "REJOIN_REPLY_FROM";
    private static final String TYPE_NW_QUERY       = "NW_QUERY_FROM";
    private static final String TYPE_NW_QUERY_REPLY = "NW_QUERY_REPLY_FROM";
    private static final String TYPE_DELETE         = "DELETE_FROM";
    private static final String TYPE_DELETE_REPLY   = "DELETE_REPLY_FROM";
    private static final String TYPE_NW_DELETE      = "NW_DELETE_FROM";

    CircularLinkedList chord;
    Node               successor;
    Node               firstPredecessor;

    Object  serverMonitor;
    boolean notifyRequiredServer;
    Object  queryMonitor;
    int     queryReplyCounter;
    boolean notifyRequiredQuery;
    Object  insertReplyMonitor;
    int     insertReplyCounter;
    boolean notifyRequiredInsert;
    Object  networkQueryMonitor;
    Object  deleteMonitor;
    int     deleteReplyCounter;
    int     networkQueryReplyCounter;
    boolean notifyRequiredDelete;
    Object  rejoinMonitor;
    int     rejoinReplyCounter;
    boolean notifyRequiredRejoin;

    MatrixCursor            sharedMatrixCursor = null;
    HashMap<String, String> cursorMap          = null;
    String[]                sharedColumns      = new String[]{"key", "value"};

    HashMap<String, Boolean> queryMap;
    HashMap<String, Boolean> insertMap;

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
    synchronized public boolean onCreate() {
        try {
            Log.d(DEBUG, "OnCreate started");

            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context
                                                                                            .TELEPHONY_SERVICE);
            avdPort = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            port = String.valueOf((Integer.parseInt(avdPort) * 2));

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
                  "Preference list - " + avdPort + ", " + successor.value.port + ", " + "" + successor.next.value.port);
            Log.d(DEBUG, "=================================");

            //Server thread should start before anything else. Hence the monitor
            serverMonitor = new Object();
            notifyRequiredServer = true;

            Thread serverThread = new Thread(new ServerTask());
            serverThread.start();

            synchronized (serverMonitor) {
                if (notifyRequiredServer) {
                    serverMonitor.wait();
                }
            }

            Log.d(DEBUG,
                  "Sending rejoin requests from " + avdPort + " to " + firstPredecessor + ", " +
                          "" + firstPredecessor.next + ", " + successor);

            rejoinReplyCounter = 0;
            notifyRequiredRejoin = true;
            rejoinMonitor = new Object();

            //Query other nodes for data
            new Thread(new ClientTask(firstPredecessor.toString(), TYPE_REJOIN_QUERY + SEPARATOR + avdPort))
                    .start();
            new Thread(new ClientTask(firstPredecessor.next.toString(), TYPE_REJOIN_QUERY + SEPARATOR + avdPort))
                    .start();
            new Thread(new ClientTask(successor.toString(), TYPE_REJOIN_QUERY + SEPARATOR + avdPort))
                    .start();
            new Thread(new ClientTask(successor.next.toString(), TYPE_REJOIN_QUERY + SEPARATOR + avdPort))
                    .start();

            ScheduledExecutorService scheduledThreadPool = Executors
                    .newScheduledThreadPool(1);
            ScheduledFuture<String> future = null;
            future = scheduledThreadPool.schedule(new Callable<String>() {
                                                      @Override
                                                      public String call() throws Exception {
                                                          String reply = "ALL NODES REPLIED";
                                                          synchronized (rejoinMonitor) {
                                                              notifyRequiredRejoin = false;
                                                              rejoinMonitor.notifyAll();
                                                          }
                                                          return reply;
                                                      }
                                                  }, 4,
                                                  TimeUnit.SECONDS);
            synchronized (rejoinMonitor) {
                if (!notifyRequiredRejoin) {
                    Log.d(DEBUG, "Begin wait on create");
                    rejoinMonitor.wait();
                    Log.d(DEBUG, "wait over on create");
                }
            }
            try {
                Log.d(DEBUG, future.get());
            } catch (InterruptedException | ExecutionException e) {
                Log.e(DEBUG, e.getMessage(), e);
            }

            queryMap = new HashMap<>();
            insertMap = new HashMap<>();
        } catch (Exception e) {
            //TODO - remove this after testing
            Log.e(DEBUG, e.getMessage(), e);
        }
        return false;
    }

    @Override
    synchronized public Uri insert(Uri uri, ContentValues values) {

        String key = values.get("key").toString();
        String value = values.get("value").toString();

        try {
            Log.d(DEBUG, "Inserting - " + values.toString());
            Node coordinator = findCoordinator(key);

            Log.d(DEBUG, "From Node " + avdPort + " sending insert request for " + key + "-" + value + " to nodes "
                    + coordinator.value.port + ", " + coordinator.next.value.port + ", "
                    + coordinator.next.next.value.port);

            insertReplyMonitor = new Object();
            insertReplyCounter = 0;
            notifyRequiredInsert = true;
            insertMap.put(key, false);

            long time = System.currentTimeMillis();

            Log.d(DEBUG,
                  "Sending insert req -> " + TYPE_INSERT + SEPARATOR + avdPort + SEPARATOR + key + SEPARATOR + value +
                          TIMESTAMP_DELIMITER + time);
            new Thread(new ClientTask(coordinator.value.port,
                                      TYPE_INSERT + SEPARATOR + avdPort + SEPARATOR + key + SEPARATOR + value +
                                              TIMESTAMP_DELIMITER + time))
                    .start();
            new Thread(new ClientTask(coordinator.next.value.port,
                                      TYPE_INSERT + SEPARATOR + avdPort + SEPARATOR + key + SEPARATOR + value +
                                              TIMESTAMP_DELIMITER + time))
                    .start();
            new Thread(new ClientTask(coordinator.next.next.value.port,
                                      TYPE_INSERT + SEPARATOR + avdPort + SEPARATOR + key + SEPARATOR + value +
                                              TIMESTAMP_DELIMITER + time))
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
            insertMap.put(key, true);
            for (String name : insertMap.keySet()) {

                String currKey = name.toString();
                String currVal = insertMap.get(name).toString();
                //System.out.println(currKey + " " + currVal);
            }
        } catch (Exception e) {
            //TODO - remove this after testing
            Log.e(DEBUG, e.getMessage(), e);
        }
        return null;
    }

    private File writeToLocalFile(String key, String value) {
        Log.d(DEBUG, "Disk IO for - " + key + " with val -" + value);
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
            cursorMap = null;
            queryMap.put(selection, false);

            if (selection.equals("\"@\"")) {
                sharedMatrixCursor = queryLocalContent(false);
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
                            Log.d(DEBUG, "Begin wait on query - " + selection);
                            queryMonitor.wait();
                            queryMap.put(selection, true);
                            Log.d(DEBUG, "Wait complete on query - " + selection);
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Log.d(DEBUG,
                      "Cursor values = " + selection + ":" + cursorMap.get(selection));
                sharedMatrixCursor = new MatrixCursor(sharedColumns);
                sharedMatrixCursor.addRow(new Object[]{selection, cursorMap.get(selection)});
            }
        } catch (Exception e) {
            //TODO - remove this after testing
            Log.e(DEBUG, e.getMessage(), e);
        }
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

    private MatrixCursor queryLocalContent(boolean forNw) {
        MatrixCursor matrixCursor = new MatrixCursor(sharedColumns);

        Log.d(DEBUG, "Begin local search");

        for (File currentLocalFile : getContext().getFilesDir().listFiles()) {
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
                int len = val.length();
                // matrixCursor.addRow(new Object[]{currentLocalFile.getName(), val});
//                Log.d(DEBUG, "Contents for file -> " + currentLocalFile.getName() + " is " + val);
//                Log.d(DEBUG, "Cursor insert -> " + currentLocalFile.getName() + " is " + val
//                        .substring(0, val.lastIndexOf(TIMESTAMP_DELIMITER)));

                if (forNw) {
                    matrixCursor.addRow(new Object[]{currentLocalFile.getName(),
                                                     val});
                } else {
                    matrixCursor.addRow(new Object[]{currentLocalFile.getName(),
                                                     val.substring(0, val.lastIndexOf(TIMESTAMP_DELIMITER))});
                }

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
//            Log.v(DEBUG, "Retrieving content for local file = " + currentLocalFile.getName());
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

        //populate the cursor from the HashMap

        sharedMatrixCursor = new MatrixCursor(sharedColumns);

        for (String key : cursorMap.keySet()) {
            sharedMatrixCursor.addRow(new Object[]{key, cursorMap.get(key)});
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
                Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
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
                            //TYPE_INSERT + SEPARATOR + avdPort + SEPARATOR + key + TIMESTAMP_DELIMITER + time
                            // + SEPARATOR + value
                            requestInsert(line, split);
                        } else if (split[0].equals(TYPE_INSERT_REPLY)) {
                            //Insert successful message from a node in preference list
                            //TYPE_INSERT_REPLY + SEPARATOR + avdPort + SEPARATOR + key + SEPARATOR + value
                            replyInsert(line, split);
                        } else if (split[0].equals(TYPE_QUERY)) {
                            //Query request from a Node
                            //TYPE_QUERY + SEPARATOR + avdPort + SEPARATOR + selection

                            requestQuery(line, split);

                        } else if (split[0].equals(TYPE_QUERY_REPLY)) {
                            //query and reply to this node
                            //TYPE_QUERY_REPLY + SEPARATOR + avdPort + SEPARATOR

                            replyQuery(line, split);
                        } else if (split[0].equals(TYPE_NW_QUERY)) {
                            //network search
                            requestNwQuery(line, split[1]);

                        } else if (split[0].equals(TYPE_NW_QUERY_REPLY)) {
                            //network search
                            //collate the data and finish MatrixCursor. notify the lock once done.
                            replyNwQuery(line);
                        } else if (split[0].equals(TYPE_DELETE)) {
                            //TYPE_DELETE + SEPARATOR + avdPort + SEPARATOR + selection
                            requestDelete(line, split);

                        } else if (split[0].equals(TYPE_DELETE_REPLY)) {
                            //TYPE_DELETE_REPLY + SEPARATOR + avdPort
                            replyDelete(line, split[1]);


                        } else if (split[0].equals(TYPE_NW_DELETE)) {
                            requestNwDelete(line);
                        } else if (split[0].equals(TYPE_REJOIN_QUERY)) {
                            //Query request from a Node
                            //TYPE_REJOIN_QUERY + SEPARATOR + avdPort
                            requestRejoin(line, split[1]);
                        } else if (split[0].equals(TYPE_REJOIN_REPLY)) {
                            //network search
                            //collate the data and finish MatrixCursor. notify the lock once done.
                            replyRejoin(line);
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

        private void replyRejoin(String line) throws IOException {
            Log.d(DEBUG, "TYPE_REJOIN_REPLY = " + line);

            rejoinReplyCounter++;

            String[] data = line.split(SEPARATOR);
            String replyingPort = data[1];

            for (int i = 2; i < data.length; i += 2) {
                String currentKey = data[i];
                String currentValue = data[i + 1];
                // sharedMatrixCursor.addRow(new Object[]{data[i], data[i + 1]});
                Node coordinator = findCoordinator(currentKey);

                if (coordinator.equals(firstPredecessor) || coordinator
                        .equals(firstPredecessor.next) || coordinator.toString().equals(avdPort)) {
                    File file = new File(getContext().getFilesDir() + File.separator + currentKey);
                    if (file.exists()) {
                        checkTimeAndWrite(currentKey, currentValue, file);
                    } else {
                        Log.d(DEBUG,
                              "File " + getContext()
                                      .getFilesDir() + File.separator + currentKey + " does not " +
                                      "exists");
                        writeToLocalFile(currentKey, currentValue);
                    }
                }
            }
            Log.d(DEBUG, "TYPE_REJOIN_REPLY completed");
            if (rejoinReplyCounter == 4) {
                synchronized (rejoinMonitor) {
                    notifyRequiredRejoin = false;
                    rejoinMonitor.notifyAll();
                }
            }
        }

        private void requestRejoin(String line, String s) {
            Log.d(DEBUG, "TYPE_REJOIN_QUERY = " + line);

            String requestingPort = s;

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
        }

        private void requestNwDelete(String line) {
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
        }

        private void replyDelete(String line, String s) {
            Log.d(DEBUG, "TYPE_DELETE_REPLY = " + line);

            String requestingPort = s;
            Log.d(DEBUG, "Current deletion counter - " + deleteReplyCounter);
            deleteReplyCounter++;

            Log.d(DEBUG, "TYPE_DELETE_REPLY completed");

            if (deleteReplyCounter == 2) {
                synchronized (deleteMonitor) {
                    notifyRequiredDelete = false;
                    deleteMonitor.notifyAll();
                }
            }
        }

        private void requestDelete(String line, String[] split) {
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
        }

        private void replyNwQuery(String line) {
            Log.d(DEBUG, "TYPE_NW_QUERY_REPLY = " + line);

            String[] data = line.split(SEPARATOR);
            String replyingPort = data[1];
            networkQueryReplyCounter++;
            if (cursorMap == null) {
                cursorMap = new HashMap<>();
            }

            for (int i = 2; i < data.length; i += 2) {
                String currentKey = data[i];
                String currentValue = data[i + 1];
                if (cursorMap.get(currentKey) != null) {
                    if (cursorMap.get(currentKey)
                                 .compareTo(currentValue) < 0) {
                        cursorMap.put(currentKey, currentValue
                                .substring(0, currentValue.lastIndexOf(TIMESTAMP_DELIMITER)));
                    }
                } else {
                    cursorMap.put(currentKey, currentValue
                            .substring(0, currentValue.lastIndexOf(TIMESTAMP_DELIMITER)));
                }
            }
            Log.d(DEBUG, "TYPE_NW_QUERY_REPLY completed");

            if (networkQueryReplyCounter == 5) {
                synchronized (networkQueryMonitor) {
                    networkQueryMonitor.notifyAll();
                }
            }
        }

        private void requestNwQuery(String line, String s) {
            Log.d(DEBUG, "TYPE_NW_QUERY = " + line);
            String requestingPort = s;

            MatrixCursor localSearchCursor = queryLocalContent(true);
            localSearchCursor.moveToFirst();
            StringBuilder stringBuilder =
                    new StringBuilder(TYPE_NW_QUERY_REPLY + SEPARATOR + avdPort + SEPARATOR);
            while (!localSearchCursor.isAfterLast()) {
                stringBuilder.append(localSearchCursor.getString(0) + SEPARATOR +
                                             localSearchCursor.getString(1) + SEPARATOR);
                localSearchCursor.moveToNext();
            }
            new Thread(new ClientTask(requestingPort, stringBuilder.toString())).start();

            Log.d(DEBUG, "TYPE_NW_QUERY completed");
        }

        private void replyQuery(String line, String[] split) {
            Log.d(DEBUG, "TYPE_QUERY_REPLY = " + line);
            String queryingPort = split[1];
            if (!queryMap.get(split[2])) {
                queryReplyCounter++;

                if (cursorMap == null) {
                    cursorMap = new HashMap<>();
                }

                for (int i = 2; i < split.length; i += 2) {
                    String currentKey = split[i];
                    String currentValue = split[i + 1];
                    Log.d(DEBUG, "TYPE_QUERY_REPLY-" + currentKey);
                    Log.d(DEBUG, "TYPE_QUERY_REPLY-" + currentValue);

                    if (cursorMap.get(currentKey) != null) {
                        if (cursorMap.get(currentKey)
                                     .compareTo(currentValue) < 0) {
                            cursorMap.put(currentKey, currentValue
                                    .substring(0, currentValue.lastIndexOf(TIMESTAMP_DELIMITER)));
                        }
                    } else {
                        cursorMap.put(currentKey, currentValue
                                .substring(0, currentValue.lastIndexOf(TIMESTAMP_DELIMITER)));
                    }
                }

                Log.d(DEBUG, "queryReplyCounter = " + queryReplyCounter);

                Log.d(DEBUG, "TYPE_QUERY_REPLY completed");

                if (queryReplyCounter == 2) {
                    synchronized (queryMonitor) {
                        notifyRequiredQuery = false;
                        Log.d(DEBUG,
                              "notifying query method " + split[2]);
                        queryMonitor.notifyAll();
                    }
                }
            }
        }

        private void requestQuery(String line, String[] split) {
            Log.d(DEBUG, "TYPE_QUERY = " + line);

            String sendingPort = split[1];
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
            Log.d(DEBUG, "When querying for " + selection + " - found - " + stringBuilder.toString());
            new Thread(new ClientTask(sendingPort, stringBuilder.toString())).start();

            Log.d(DEBUG, "TYPE_QUERY completed");
        }

        private void replyInsert(String line, String[] split) {
            Log.d(DEBUG, "TYPE_INSERT_REPLY = " + line);

            String sendingPort = split[1];
            String key = split[2];
            String val = split[3];

            Log.d(DEBUG,
                  "Received insert completion message from Node - " + sendingPort + " for key-value=" +
                          key + "-" + val);

            if (!insertMap.get(key)) {
                insertReplyCounter++;


                Log.d(DEBUG, "InsertReplyCounter = " + insertReplyCounter);
                Log.d(DEBUG, "TYPE_INSERT_REPLY completed");

                if (insertReplyCounter == 2) {
                    synchronized (insertReplyMonitor) {
                        notifyRequiredInsert = false;
                        Log.d(DEBUG,
                              "notifying insert method " + key);
                        insertReplyMonitor.notifyAll();
                    }
                }
            }
        }

        private void requestInsert(String line, String[] split) throws IOException {
            Log.d(DEBUG, "TYPE_INSERT = " + line);
            String sendingPort = split[1];
            String key = split[2];
            String val = split[3];

            File file = new File(getContext().getFilesDir() + File.separator + key);
            if (file.exists()) {
                checkTimeAndWrite(key, val, file);
            } else {
                Log.d(DEBUG,
                      "File " + getContext().getFilesDir() + File.separator + key + " does not exists");
                writeToLocalFile(key, val);
            }
            Log.d(DEBUG,
                  "Completed insert req from coordinator" + sendingPort + " for key - " + key + " " +
                          "value -" + val);

            new Thread(new ClientTask(sendingPort,
                                      TYPE_INSERT_REPLY + SEPARATOR + avdPort + SEPARATOR + key
                                              + SEPARATOR + val))
                    .start();

            Log.d(DEBUG, "TYPE_INSERT completed");
        }

        private void checkTimeAndWrite(String currentKey, String currentValue, File file) throws IOException {
            Log.d(DEBUG,
                  "File " + getContext().getFilesDir() + File.separator + currentKey + " already exists");
            BufferedReader fileReader = new BufferedReader(new FileReader(file));
            String fileContent = fileReader.readLine();

            long timeInFile = Long.parseLong(fileContent.substring(fileContent.indexOf(TIMESTAMP_DELIMITER) + 1));
            long timeInMsg = Long.parseLong(currentValue.substring(currentValue.indexOf(TIMESTAMP_DELIMITER) + 1));
            Log.d(DEBUG, "Timestamp in file - " + timeInFile);
            Log.d(DEBUG, "Contents of file : " + fileContent);
            Log.d(DEBUG, "Timestamp in msg - " + timeInMsg);
            Log.d(DEBUG, "Contents of msg : " + currentValue);

            if (timeInFile < timeInMsg) {
                Log.d(DEBUG, "Time in file is less than time in msg. Overwriting  with " +
                        "new msg");
                writeToLocalFile(currentKey, currentValue);
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

//                    Log.v(DEBUG, "ClientTask - Sending the message - " + messageToServer + " to - " + portToWrite);
                    os.write(messageToServer.getBytes());
                    os.flush();
//                    Log.v(DEBUG, "ClientTask - Message " + messageToServer + " flushed from client to - " +
//                            portToWrite);
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
