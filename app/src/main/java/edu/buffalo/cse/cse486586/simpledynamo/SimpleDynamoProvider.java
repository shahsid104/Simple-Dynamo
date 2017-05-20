package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.preference.PreferenceManager;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
    static final int SERVER_PORT = 10000;
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    String myPort, myHash;
    int myIndex;
    Boolean signal = false;
    HashMap<String,Integer> portMapping = new HashMap<>();
    HashMap<Integer,String> reversePortMapping = new HashMap<>();
    ArrayList<String> allPortOrderedHash = new ArrayList<>();
    final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private void makeThreadSleep()
    {
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
    private void fillHashMap()
    {
        portMapping.put(REMOTE_PORT0,2);
        reversePortMapping.put(2,REMOTE_PORT0);

        portMapping.put(REMOTE_PORT1,1);
        reversePortMapping.put(1,REMOTE_PORT1);

        portMapping.put(REMOTE_PORT2,3);
        reversePortMapping.put(3,REMOTE_PORT2);

        portMapping.put(REMOTE_PORT3,4);
        reversePortMapping.put(4,REMOTE_PORT3);

        portMapping.put(REMOTE_PORT4,0);
        reversePortMapping.put(0,REMOTE_PORT4);

        try {

            allPortOrderedHash.add(0, genHash("5562"));
            allPortOrderedHash.add(1, genHash("5556"));
            allPortOrderedHash.add(2, genHash("5554"));
            allPortOrderedHash.add(3, genHash("5558"));
            allPortOrderedHash.add(4, genHash("5560"));

        }catch(Exception ex) {

        }

    }
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        if (selection.equals("@")) {
            PreferenceManager.getDefaultSharedPreferences(getContext()).edit().clear().commit();
            return 0;
        }

        String keys,value;
        int query;
        if(selection.equals("*")) {
            PreferenceManager.getDefaultSharedPreferences(getContext()).edit().clear().commit();
            keys = "DUMMY";
            value = "DUMMY";
            query = 2;
        }

        else {
            PreferenceManager.getDefaultSharedPreferences(getContext()).edit().remove(selection).commit();
            keys = selection;
            value = "DUMMY";
            query = 1;
        }
            try {
            if (!myPort.equals(REMOTE_PORT0)) {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(REMOTE_PORT0));
                Message msgToSend = new Message(keys,value,query);
                ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                objStream.writeObject(msgToSend);
            }

            if (!myPort.equals(REMOTE_PORT1)) {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(REMOTE_PORT1));
                Message msgToSend = new Message(keys,value,query);
                ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                objStream.writeObject(msgToSend);
            }

            if (!myPort.equals(REMOTE_PORT2)) {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(REMOTE_PORT2));
                Message msgToSend = new Message(keys,value,query);
                ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                objStream.writeObject(msgToSend);
            }

            if (!myPort.equals(REMOTE_PORT3)) {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(REMOTE_PORT3));
                Message msgToSend = new Message(keys,value,query);
                ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                objStream.writeObject(msgToSend);
            }

            if (!myPort.equals(REMOTE_PORT4)) {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(REMOTE_PORT4));
                Message msgToSend = new Message(keys,value,query);
                ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                objStream.writeObject(msgToSend);
            }

        }catch (Exception e){

        }

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
            while (signal)
                continue;
            Set<Map.Entry<String,Object>> set = values.valueSet();
            Iterator<Map.Entry<String,Object>> it = set.iterator();
            String value = it.next().getValue().toString();
            String keys = it.next().getValue().toString();
            Log.d("Keys",keys);
            Log.d("value",value);

            SharedPreferences sp = PreferenceManager.getDefaultSharedPreferences(getContext());
            SharedPreferences.Editor edit = sp.edit();
            edit.putString(keys, value);
            edit.commit();
            Log.d("insert called in", myPort);

            String size = sp.getString("size","0");
            edit.putString("size",String.valueOf(Integer.parseInt(size) + 1));
            edit.commit();
            Log.d("Size of dynamo",String.valueOf(Integer.parseInt(size) + 1));
            Message msgToSend = new Message(keys,value,0);
            /*new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return null;*/

            //Adding clientTask here itself
        try {
            try {
                if (!myPort.equals(REMOTE_PORT0)) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT0));
                    socket.setSoTimeout(500);
                    ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                    objStream.writeObject(msgToSend);
                    Log.d("Sent to port", REMOTE_PORT0);
                }
            }catch(IOException ex)
            {
                Log.d("Socket is dead","0");
            }

            try {
                if (!myPort.equals(REMOTE_PORT1)) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT1));
                    socket.setSoTimeout(500);
                    ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                    objStream.writeObject(msgToSend);
                    Log.d("Sent to port", REMOTE_PORT1);
                }
            }catch (IOException ex){
                Log.d("Socket is dead","1");
            }

            try {
                if (!myPort.equals(REMOTE_PORT2)) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT2));
                    socket.setSoTimeout(500);
                    ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                    objStream.writeObject(msgToSend);
                    Log.d("Sent to port", REMOTE_PORT2);
                }
            }catch(IOException ex){
                Log.d("Socket is dead","2");
            }

            try {
                if (!myPort.equals(REMOTE_PORT3)) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT3));
                    socket.setSoTimeout(500);
                    ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                    objStream.writeObject(msgToSend);
                    Log.d("Sent to port", REMOTE_PORT3);
                }
            }catch(IOException ex){
                Log.d("Socket is dead","3");
            }

            try {
                if (!myPort.equals(REMOTE_PORT4)) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT4));
                    socket.setSoTimeout(500);
                    ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                    objStream.writeObject(msgToSend);
                    Log.d("Sent to port", REMOTE_PORT4);
                }
            }catch (IOException ex){
                Log.d("Socket is dead","4");
            }

            Thread.sleep(5000);

            Log.d("Reached","at the end");
        }catch (Exception e){

        }
        return null;
	}

	@Override
	public boolean onCreate() {
        try {
            fillHashMap();
            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            myHash = genHash(portStr);
            myIndex = portMapping.get(myPort);
            Log.d("Port String",portStr);
            Log.d("Port",myPort);
            Log.d("My Index",String.valueOf(myIndex));
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }catch(Exception ex){
            ex.printStackTrace();
        }
        SharedPreferences sp = PreferenceManager.getDefaultSharedPreferences(getContext());
        String value = sp.getString("created","xxx");
        Log.d("QueryValue",value);
        if(value.equals("xxx"))
        {
            SharedPreferences.Editor edit = sp.edit();
            edit.putString("created","yes");
            edit.putString("size",String.valueOf(0));
            edit.commit();
            Log.d("created once","wohooo");
        }

        else if(value.equals("yes"))
        {
            signal = true;
            //PreferenceManager.getDefaultSharedPreferences(getContext()).edit().clear().commit();
           /* try {
                boolean v = new RecoverTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR).get();
            }catch (Exception ex){

            }*/
            new RecoverTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
            SharedPreferences.Editor edit = sp.edit();
            edit.putString("created","yes");
            edit.commit();
            Log.d("created once again","wohooo");


        }

		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
        Log.d("QUERYING FOR",selection);
		if(selection.equals("*"))
        {
            Map<String,String> allKeys = (Map<String,String>) PreferenceManager.getDefaultSharedPreferences(getContext()).getAll();
            MatrixCursor cursor = new MatrixCursor(new String[]{"key","value"});
            for(Map.Entry<String,String> entry:allKeys.entrySet()){
                Log.d("Shared Preference value",entry.getKey()+ ":" + entry.getValue());
               /* if(entry.getKey().equals("created"))
                    continue;;*/
                MatrixCursor.RowBuilder build;
                build =  cursor.newRow();
                build.add("key",entry.getKey());
                build.add("value",entry.getValue());
            }
            cursor.moveToFirst();
            return cursor;
        }

        /*while(signal)
            continue;*/

        if(selection.equals("@"))
        {

            if(signal) {
                while (signal)
                    continue;
            }

            else {
                Log.d("Making thread", "sleep");
                makeThreadSleep();
            }

            MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
            int pre1_index, pre2_index, pre3_index;
            if(myIndex == 0)
            {
                pre1_index = 4;
                pre2_index = 3;
                pre3_index = 2;
            }

            else if(myIndex == 1)
            {
                pre1_index = 0;
                pre2_index = 4;
                pre3_index = 3;
            }

            else if(myIndex == 2)
            {
                pre1_index = 1;
                pre2_index = 0;
                pre3_index = 4;
            }
            else
            {
                pre1_index = myIndex - 1;
                pre2_index = myIndex - 2;
                pre3_index = myIndex - 3;
            }
            try {
                Map<String, String> allKeys = (Map<String, String>) PreferenceManager.getDefaultSharedPreferences(getContext()).getAll();
                for (Map.Entry<String, String> entry : allKeys.entrySet()) {
                    Log.d("All values",entry.getKey()+ ":" + entry.getValue());
                    if(entry.getKey().equals("created") || entry.getKey().equals("size"))
                        continue;
                    String keyHash = genHash(entry.getKey());
                    if(myIndex != 0) {
                        if (keyHash.compareTo(myHash) <= 0 && keyHash.compareTo(allPortOrderedHash.get(pre1_index)) > 0) {
                            MatrixCursor.RowBuilder build;
                            build = cursor.newRow();
                            build.add("key", entry.getKey());
                            build.add("value", entry.getValue());
                        }
                    }

                    else {

                        if (keyHash.compareTo(myHash) <= 0 || keyHash.compareTo(allPortOrderedHash.get(pre1_index)) > 0) {
                            Log.d("Shared Preference value",entry.getKey()+ ":" + entry.getValue());
                            MatrixCursor.RowBuilder build;
                            build = cursor.newRow();
                            build.add("key", entry.getKey());
                            build.add("value", entry.getValue());
                        }
                    }

                    if(pre1_index != 0) {
                        if (keyHash.compareTo(allPortOrderedHash.get(pre1_index)) <= 0 && keyHash.compareTo(allPortOrderedHash.get(pre2_index)) > 0) {
                            Log.d("Shared Preference value",entry.getKey()+ ":" + entry.getValue());
                            MatrixCursor.RowBuilder build;
                            build = cursor.newRow();
                            build.add("key", entry.getKey());
                            build.add("value", entry.getValue());
                        }
                    }

                    else {

                        if (keyHash.compareTo(allPortOrderedHash.get(pre1_index)) <= 0 || keyHash.compareTo(allPortOrderedHash.get(pre2_index)) > 0) {
                            Log.d("Shared Preference value",entry.getKey()+ ":" + entry.getValue());
                            MatrixCursor.RowBuilder build;
                            build = cursor.newRow();
                            build.add("key", entry.getKey());
                            build.add("value", entry.getValue());
                        }
                    }

                    if(pre2_index != 0) {

                        if (keyHash.compareTo(allPortOrderedHash.get(pre2_index)) <= 0 && keyHash.compareTo(allPortOrderedHash.get(pre3_index)) > 0) {
                            Log.d("Shared Preference value",entry.getKey()+ ":" + entry.getValue());
                            MatrixCursor.RowBuilder build;
                            build = cursor.newRow();
                            build.add("key", entry.getKey());
                            build.add("value", entry.getValue());
                        }
                    }

                    else {

                        if (keyHash.compareTo(allPortOrderedHash.get(pre2_index)) <= 0 || keyHash.compareTo(allPortOrderedHash.get(pre3_index)) > 0) {
                            Log.d("Shared Preference value",entry.getKey()+ ":" + entry.getValue());
                            MatrixCursor.RowBuilder build;
                            build = cursor.newRow();
                            build.add("key", entry.getKey());
                            build.add("value", entry.getValue());
                        }
                    }
                }
            }catch (Exception ex){

            }
            Log.d("@","returned");
            cursor.moveToFirst();
            return cursor;
        }

        /*try{
            Thread.sleep(2000);
        }catch(Exception ex){

        }*/
        SharedPreferences sp = PreferenceManager.getDefaultSharedPreferences(getContext());
        while(sp.getString(selection,"xyz").equals("xyz"))
            continue;

        String value = sp.getString(selection,"xyz");
        Log.d("QueryKey",selection);
        Log.d("QueryValue",value);
        if(value.equals("xyz"))
        {
            try {
                try {

                    if(!myPort.equals(REMOTE_PORT0)) {
                        Log.d("Sending message",REMOTE_PORT0);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(REMOTE_PORT0));
                        socket.setSoTimeout(2000);
                        Message recoverMessage = new Message(selection, "Dummy", 4);
                        ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                        objStream.writeObject(recoverMessage);

                        ObjectInputStream msgToReceieve = new ObjectInputStream(socket.getInputStream());
                        value = (String) msgToReceieve.readObject();
                        Log.d("Value received", value);
                        socket.close();
                        if (!value.equals("xyz")) {
                            sp = PreferenceManager.getDefaultSharedPreferences(getContext());
                            SharedPreferences.Editor edit = sp.edit();
                            edit.putString(selection, value);
                            edit.commit();
                            MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                            MatrixCursor.RowBuilder build;
                            build = cursor.newRow();
                            build.add("key", selection);
                            build.add("value", value);
                            cursor.moveToFirst();
                            Log.d("cursor", "returned");
                            return cursor;
                        }
                    }
                } catch (IOException ex) {

                }

                try{
                    if(!myPort.equals(REMOTE_PORT1)) {
                        Log.d("Sending message",REMOTE_PORT1);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(REMOTE_PORT1));
                        socket.setSoTimeout(2000);
                        Message recoverMessage = new Message(selection, "Dummy", 4);
                        ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                        objStream.writeObject(recoverMessage);

                        ObjectInputStream msgToReceieve = new ObjectInputStream(socket.getInputStream());
                        value = (String) msgToReceieve.readObject();
                        Log.d("Value received", value);
                        socket.close();
                        if (!value.equals("xyz")) {
                            sp = PreferenceManager.getDefaultSharedPreferences(getContext());
                            SharedPreferences.Editor edit = sp.edit();
                            edit.putString(selection, value);
                            edit.commit();
                            MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                            MatrixCursor.RowBuilder build;
                            build = cursor.newRow();
                            build.add("key", selection);
                            build.add("value", value);
                            cursor.moveToFirst();
                            Log.d("cursor", "returned");
                            return cursor;
                        }
                    }
                } catch (IOException ex) {

                }

                try{
                    if(!myPort.equals(REMOTE_PORT2)) {
                        Log.d("Sending message",REMOTE_PORT2);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(REMOTE_PORT2));
                        socket.setSoTimeout(2000);
                        Message recoverMessage = new Message(selection, "Dummy", 4);
                        ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                        objStream.writeObject(recoverMessage);

                        ObjectInputStream msgToReceieve = new ObjectInputStream(socket.getInputStream());
                        value = (String) msgToReceieve.readObject();
                        Log.d("Value received", value);
                        socket.close();
                        if (!value.equals("xyz")) {
                            sp = PreferenceManager.getDefaultSharedPreferences(getContext());
                            SharedPreferences.Editor edit = sp.edit();
                            edit.putString(selection, value);
                            edit.commit();
                            MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                            MatrixCursor.RowBuilder build;
                            build = cursor.newRow();
                            build.add("key", selection);
                            build.add("value", value);
                            cursor.moveToFirst();
                            Log.d("cursor", "returned");
                            return cursor;
                        }
                    }
                } catch (IOException ex) {

                }

                try{
                    if(!myPort.equals(REMOTE_PORT3)) {
                        Log.d("Sending message",REMOTE_PORT3);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(REMOTE_PORT3));
                        socket.setSoTimeout(2000);
                        Message recoverMessage = new Message(selection, "Dummy", 4);
                        ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                        objStream.writeObject(recoverMessage);

                        ObjectInputStream msgToReceieve = new ObjectInputStream(socket.getInputStream());
                        value = (String) msgToReceieve.readObject();
                        Log.d("Value received", value);
                        socket.close();
                        if (!value.equals("xyz")) {
                            sp = PreferenceManager.getDefaultSharedPreferences(getContext());
                            SharedPreferences.Editor edit = sp.edit();
                            edit.putString(selection, value);
                            edit.commit();
                            MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                            MatrixCursor.RowBuilder build;
                            build = cursor.newRow();
                            build.add("key", selection);
                            build.add("value", value);
                            cursor.moveToFirst();
                            Log.d("cursor", "returned");
                            return cursor;
                        }
                    }
                } catch (IOException ex) {

                }

                try{
                    if(!myPort.equals(REMOTE_PORT4)) {
                        Log.d("Sending message",REMOTE_PORT4);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(REMOTE_PORT4));
                        socket.setSoTimeout(2000);
                        Message recoverMessage = new Message(selection, "Dummy", 4);
                        ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                        objStream.writeObject(recoverMessage);

                        ObjectInputStream msgToReceieve = new ObjectInputStream(socket.getInputStream());
                        value = (String) msgToReceieve.readObject();
                        Log.d("Value received", value);
                        socket.close();
                        if (!value.equals("xyz")) {
                            sp = PreferenceManager.getDefaultSharedPreferences(getContext());
                            SharedPreferences.Editor edit = sp.edit();
                            edit.putString(selection, value);
                            edit.commit();
                            MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
                            MatrixCursor.RowBuilder build;
                            build = cursor.newRow();
                            build.add("key", selection);
                            build.add("value", value);
                            cursor.moveToFirst();
                            Log.d("cursor", "returned");
                            return cursor;
                        }
                    }
                } catch (IOException ex) {

                }

            }catch (Exception e)
            {

            }
        }

        MatrixCursor cursor = new MatrixCursor(new String[]{"key","value"});
        MatrixCursor.RowBuilder build;
        build =  cursor.newRow();
        build.add("key",selection);
        build.add("value",value);
        cursor.moveToFirst();
        Log.d("cursor","returned");
        return cursor;

	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
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

    private class ServerTask extends AsyncTask<ServerSocket,Message,Void>
    {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try{
                while (true){
                    Socket clientSocket = serverSocket.accept();
                    ObjectInputStream msgReceived = new ObjectInputStream(clientSocket.getInputStream());
                    Message receivedObject = (Message) msgReceived.readObject();
                    Log.d("Message received",String.valueOf(receivedObject.query));
                    Log.d("Signal received",String.valueOf(signal));
                    while(signal)
                        continue;

                    if(receivedObject.query == 0)
                    {
                        SharedPreferences sp = PreferenceManager.getDefaultSharedPreferences(getContext());
                        SharedPreferences.Editor edit = sp.edit();
                        edit.putString(receivedObject.key, receivedObject.value);
                        edit.commit();

                        String size = sp.getString("size","0");
                        edit.putString("size",String.valueOf(Integer.parseInt(size) + 1));
                        edit.commit();
                        Log.d("Size of dynamo",String.valueOf(Integer.parseInt(size) + 1));
                        Log.d("Key",receivedObject.key);
                        Log.d("Value",receivedObject.value);
                        Log.d("inserted in", myPort);
                        //publishProgress(receivedObject);
                    }

                    if(receivedObject.query == 2)
                    {
                        PreferenceManager.getDefaultSharedPreferences(getContext()).edit().clear().commit();
                        Log.d("Cleared Everything","WOHOOO");
                    }

                    if(receivedObject.query == 1)
                    {
                        PreferenceManager.getDefaultSharedPreferences(getContext()).edit().remove(receivedObject.key).commit();
                    }

                    if(receivedObject.query == 3)
                    {
                        Log.d("Query",String.valueOf(receivedObject.query));
                        Cursor q = query(mUri, null,"*", null, null);
                        HashMap<String,String> allValues = new HashMap<>();
                        Log.d("Count",String.valueOf(q.getCount()));
                        if(q.getCount() != 0) {
                            do {
                                int keyIndex = q.getColumnIndex("key");
                                int valueIndex = q.getColumnIndex("value");
                                allValues.put(q.getString(keyIndex), q.getString(valueIndex));
                                Log.d(q.getString(keyIndex), q.getString(valueIndex));
                            } while (q.moveToNext());
                        }
                        ObjectOutputStream msgToSend = new ObjectOutputStream(clientSocket.getOutputStream());
                        msgToSend.writeObject(allValues);
                    }

                    if(receivedObject.query == 4)
                    {
                        Log.d("Query",receivedObject.key);
                        SharedPreferences sp = PreferenceManager.getDefaultSharedPreferences(getContext());
                        String value = sp.getString(receivedObject.key,"xyz");
                        ObjectOutputStream msgToSend = new ObjectOutputStream(clientSocket.getOutputStream());
                        msgToSend.writeObject(value);
                        Log.d("Sent value",value);
                    }

                }

            }catch(Exception ex){

            }

            return null;
        }

        @Override
        protected void onProgressUpdate(Message... values) {
            super.onProgressUpdate(values);
            SharedPreferences sp = PreferenceManager.getDefaultSharedPreferences(getContext());
            SharedPreferences.Editor edit = sp.edit();
            edit.putString(values[0].key, values[0].value);
            edit.commit();

            String size = sp.getString("size","0");
            edit.putString("size",String.valueOf(Integer.parseInt(size) + 1));
            edit.commit();
            Log.d("Size of dynamo",String.valueOf(Integer.parseInt(size) + 1));
            Log.d("Key",values[0].key);
            Log.d("Value",values[0].value);
            Log.d("inserted in", myPort);
        }
    }

    private class ClientTask extends AsyncTask<Message,Void,Void>
    {

        @Override
        protected Void doInBackground(Message... params) {
            try {
            if (!myPort.equals(REMOTE_PORT0)) {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(REMOTE_PORT0));
                ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                objStream.writeObject(params[0]);
                Log.d("Sent to port",REMOTE_PORT0);
            }

            if (!myPort.equals(REMOTE_PORT1)) {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(REMOTE_PORT1));
                ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                objStream.writeObject(params[0]);
                Log.d("Sent to port",REMOTE_PORT1);
            }

            if (!myPort.equals(REMOTE_PORT2)) {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(REMOTE_PORT2));
                ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                objStream.writeObject(params[0]);
                Log.d("Sent to port",REMOTE_PORT2);
            }

            if (!myPort.equals(REMOTE_PORT3)) {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(REMOTE_PORT3));
                ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                objStream.writeObject(params[0]);
                Log.d("Sent to port",REMOTE_PORT3);
            }

            if (!myPort.equals(REMOTE_PORT4)) {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(REMOTE_PORT4));
                ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                objStream.writeObject(params[0]);
                Log.d("Sent to port",REMOTE_PORT4);
            }
            Log.d("Reached","at the end");
        }catch (Exception e){

        }
            return null;
        }
    }

    private class RecoverTask extends AsyncTask<Void,HashMap<String,String>,Boolean>
    {

        @Override
        protected Boolean doInBackground(Void... params) {
          //  PreferenceManager.getDefaultSharedPreferences(getContext()).edit().clear().commit();
            int requestIndex;
            if(myIndex == 2)
                requestIndex = 4;

            else
                requestIndex = 2;

            try {
                Log.d("Requesting port",reversePortMapping.get(requestIndex));
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(reversePortMapping.get(requestIndex)));
                Message recoverMessage = new Message("Dummy","Dummy",3);
                ObjectOutputStream objStream = new ObjectOutputStream(socket.getOutputStream());
                objStream.writeObject(recoverMessage);

                ObjectInputStream msgToReceieve = new ObjectInputStream(socket.getInputStream());
                HashMap<String,String> recoveryData = (HashMap<String,String>)msgToReceieve.readObject();
                socket.close();
                for(Map.Entry<String,String> entry:recoveryData.entrySet()){
                    SharedPreferences sp = PreferenceManager.getDefaultSharedPreferences(getContext());
                   /* String value = sp.getString(entry.getKey(),"xyz");
                    if(!value.equals("xyz"))
                        continue;*/
                    SharedPreferences.Editor edit = sp.edit();
                    edit.putString(entry.getKey(),entry.getValue());
                    edit.commit();
                    Log.d(entry.getKey(),entry.getValue());
                }
                Log.d("DONE","DONE");
                signal = false;
                return true;
                //publishProgress(recoveryData);
                //return true;

            }catch (Exception ex){

            }

            return null;
        }

        @Override
        protected void onProgressUpdate(HashMap<String, String>... values) {
            super.onProgressUpdate(values);
            for(Map.Entry<String,String> entry:values[0].entrySet()){
                SharedPreferences sp = PreferenceManager.getDefaultSharedPreferences(getContext());
                String value = sp.getString(entry.getKey(),"xyz");
                if(!value.equals("xyz"))
                    continue;
                SharedPreferences.Editor edit = sp.edit();
                edit.putString(entry.getKey(),entry.getValue());
                edit.commit();
            }
            Log.d("DONE","DONE");


        }
    }
}
