package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	String TAG="SimpleDynamo";
	String node_id;
	String Port;
	String myPort;
	LinkedList<String> multicast=new LinkedList<String>();
	LinkedList<String> activePorts=new LinkedList<String>();
	LinkedList<String> activeNodes=new LinkedList<String>();
	String[] arrActivePorts={"5554","5556","5558","5560","5562"};
	String[] arrActiveNodes=new String[arrActivePorts.length];
	Boolean flag;
	Boolean flag2=true;
	//Boolean flag3=true;
	static final ReentrantLock lock = new ReentrantLock(true);
	static final int SERVER_PORT = 10000;
	String suc;
	String sucsuc;
	String pre;
	HashMap<String, Integer> waitsinglequery=new HashMap<String, Integer>();
	HashMap<String, MatrixCursor> allcursors=new HashMap<String, MatrixCursor>();
	HashMap<String, String> backup5554=new HashMap<String, String>();
	HashMap<String, String> backup5556=new HashMap<String, String>();
	HashMap<String, String> backup5558=new HashMap<String, String>();
	HashMap<String, String> backup5560=new HashMap<String, String>();
	HashMap<String, String> backup5562=new HashMap<String, String>();

	MatrixCursor matcur;
	int count=0;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		getContext().deleteFile(selection);
		if(flag2) {
			Message msg = new Message();
			msg.tag = "delete";

			String[] remotePort = {"11108", "11112", "11116", "11120", "11124"};
			for (String s : remotePort) {
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(s));
					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//
					out.writeObject(msg);

					socket.close();

				} catch (Exception e) {
					Log.e(TAG, "query");
				}
			}
		}
		flag2=false;


		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String key = (String)values.get("key");
		String value= (String)values.get("value");
		FileOutputStream outputStream;
		String send="";
		String sendsuc="";
		String sendsucsuc="";

		try{
			String hashkey = genHash(key);

			if(hashkey.compareTo(arrActiveNodes[0])<=0 || hashkey.compareTo(arrActiveNodes[arrActiveNodes.length-1])>0){
				Log.v("find1","find1");
				send=arrActivePorts[0];
				sendsuc=arrActivePorts[1];
				sendsucsuc=arrActivePorts[2];
			}
			else{
				for(int i=1;i<arrActiveNodes.length;i++){
					if(hashkey.compareTo(arrActiveNodes[i])<=0){
						if(i<=2) {
							Log.v("find2","find2");
							send = arrActivePorts[i];
							sendsuc = arrActivePorts[i + 1];
							sendsucsuc = arrActivePorts[i + 2];
							break;
						}else if(i==3){
							Log.v("find3","find3");
							send = arrActivePorts[i];
							sendsuc = arrActivePorts[i + 1];
							sendsucsuc = arrActivePorts[0];
							break;
						}else if(i==4){
							Log.v("find4","find4");
							send = arrActivePorts[i];
							sendsuc = arrActivePorts[0];
							sendsucsuc = arrActivePorts[1];
							break;
						}
					}
				}
			}

//			if(hashkey.compareTo(activeNodes.getFirst())<=0 || hashkey.compareTo(activeNodes.getLast())>0){
//				send=activePorts.getFirst();
//			}
//			else{
//				Iterator it = activeNodes.iterator();
//				Iterator itt= activePorts.iterator();
//
//				while(it.hasNext()){
//					if(hashkey.compareTo((String)it.next())<=0){
//						send=(String)itt.next();
//						break;
//					}
//					else{
//						itt.next();
//
//					}
//				}
//			}

			Log.v("send to",send);
			Log.v("keyhere",key);
			if(send.equals(myPort)){
				Log.v("keyhere1","keyhere1");
				outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
				outputStream.write(value.getBytes());
				outputStream.close();
				create_backup(send,sendsuc,sendsucsuc,key, value);
				Message msg=new Message();
				msg.key=key;
				msg.value=value;
				msg.tag="insert";


				// sending to second
				int temp1= Integer.parseInt(sendsuc);
				temp1=temp1*2;
				sendsuc=Integer.toString(temp1);
				try {
					Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(sendsuc));

					ObjectOutputStream out1 = new ObjectOutputStream(socket1.getOutputStream());
					out1.writeObject(msg);

					socket1.close();
				}catch(Exception e){
					Log.v("insert ex at 2",key);
				}
				Log.v("sent to  ", sendsuc);

				// sending to third
				int temp2= Integer.parseInt(sendsucsuc);
				temp2=temp2*2;
				sendsucsuc=Integer.toString(temp2);

				try {
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(sendsucsuc));

					ObjectOutputStream out2 = new ObjectOutputStream(socket2.getOutputStream());
					out2.writeObject(msg);

					socket2.close();
					Log.v("sent to  ", sendsucsuc);

				}catch(Exception e){
					Log.v("insert ex at 2",key);
				}

			}
			Log.v("keyhere2","keyhere2");
			Log.v("keyhere3",send);
			Log.v("keyhere4",sendsuc);
			Log.v("keyhere5",sendsucsuc);
			Log.v("keyhere6","keyhere2");
			Log.v("keyhere7","keyhere2");
//			else{
			//create_backup(send,sendsuc,sendsucsuc,key, value);

				int temp= Integer.parseInt(send);
				temp=temp*2;
				send=Integer.toString(temp);

				Message msg=new Message();
				msg.key=key;
				msg.value=value;
				msg.tag="insert";
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(send));

					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(msg);

					socket.close();
					Log.v("sent to  ", send);
				}catch(Exception e){
					Log.v("insert ex at 1",key);
				}

			// sending to second
			int temp1= Integer.parseInt(sendsuc);
			temp1=temp1*2;
			sendsuc=Integer.toString(temp1);
			try {
				Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(sendsuc));

				ObjectOutputStream out1 = new ObjectOutputStream(socket1.getOutputStream());
				out1.writeObject(msg);

				socket1.close();
				Log.v("sent to  ", sendsuc);
			}catch(Exception e){
				Log.v("insert ex at 2",key);
			}

			// sending to third
			int temp2= Integer.parseInt(sendsucsuc);
			temp2=temp2*2;
			sendsucsuc=Integer.toString(temp2);

			try {
				Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(sendsucsuc));

				ObjectOutputStream out2 = new ObjectOutputStream(socket2.getOutputStream());
				out2.writeObject(msg);

				socket2.close();
				Log.v("sent to  ", sendsucsuc);
			}catch(Exception e){
				Log.v("insert ex at 3",key);
			}
//			}
			Log.v("keyhere8","keyhere2");

		}catch(NoSuchAlgorithmException e){
			Log.v("genHash","exception");
		}
		catch(Exception e){
			Log.v("keyhere9","keyhere2");
			Log.v("keyhere10",key);
//			int temp= Integer.parseInt(sendsuc);
//			temp=temp*2;
//			sendsuc=Integer.toString(temp);
//
//			Log.v("here","is the exception");
//			Message msg=new Message();
//			msg.key=key;
//			msg.value=value;
//			msg.tag="insert2";
//			try {
//				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//						Integer.parseInt(sendsuc));
//
//				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//				out.writeObject(msg);
//
//				socket.close();
//				Log.v("sent to  ", send);
//			}catch(Exception ex){
//				Log.v("Exception k andar","exception");
//			}
		}

		return null;
	}

	////////////#### hash map wala function

	void create_backup(String send,String sendsuc,String sendsucsuc, String key, String value){
		Log.v("back1", "back1");
		if(send.equals("5554") || sendsuc.equals("5554") || sendsucsuc.equals("5554")){
			backup5554.put(key,value);
		}
		if(send.equals("5556") || sendsuc.equals("5556") || sendsucsuc.equals("5556")){
			backup5556.put(key,value);
		}
		if(send.equals("5558") || sendsuc.equals("5558") || sendsucsuc.equals("5558")){
			backup5558.put(key,value);
		}
		if(send.equals("5560") || sendsuc.equals("5560") || sendsucsuc.equals("5560")){
			backup5560.put(key,value);
		}
		if(send.equals("5562") || sendsuc.equals("5562") || sendsucsuc.equals("5562")){
			backup5562.put(key,value);
		}
		Log.v("back2", "back2");
	}


	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		Log.v("on", "create");
		try {
			for (int i = 0; i < arrActivePorts.length; i++) {
				arrActiveNodes[i] = genHash(arrActivePorts[i]);
			}

			for (int n = 0; n < arrActiveNodes.length; n++) {
				for (int m = n + 1; m < arrActiveNodes.length; m++) {
					Log.v("" + m, "" + n);
					if ((arrActiveNodes[m].compareTo(arrActiveNodes[n])) < 0) {
						String swap = arrActiveNodes[m];
						arrActiveNodes[m] = arrActiveNodes[n];
						arrActiveNodes[n] = swap;
						swap = arrActivePorts[m];
						arrActivePorts[m] = arrActivePorts[n];
						arrActivePorts[n] = swap;
					}
				}
			}

		}catch(NoSuchAlgorithmException e){
			Log.v("exception","here");
		}catch(Exception e){
			Log.v("main","exception");
		}

		Log.v("reached","here");

		for (int j=0; j<arrActivePorts.length;j++){
			Log.v("Active ports", arrActivePorts[j]);
			Log.v("active nodes", arrActiveNodes[j]);
		}
		try {
			for (int j = 0; j < arrActivePorts.length; j++) {
				activePorts.add(arrActivePorts[j]);
				activeNodes.add(arrActiveNodes[j]);
				Log.v("Active ports", arrActivePorts[j]);
			}
		}catch(Exception e){
			Log.v("caught","here");
		}
		try {
			TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);

			String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			Port = String.valueOf((Integer.parseInt(portStr) * 2));
			myPort=String.valueOf((Integer.parseInt(portStr)));
			Log.v("pri", myPort);
			Log.v("pri", Port);
			node_id=genHash(myPort);


		}catch(NoSuchAlgorithmException ex){
			Log.e("abc","abc");
		}
		catch(Exception e){
			Log.e("excptn","message");
		}

		try {
			int loc = 0;

			for (int i = 0; i < arrActiveNodes.length; i++) {
				if (arrActiveNodes[i].equals(genHash(myPort))) {
					loc = i;
					break;
				}
			}

			Log.e(myPort, "" + loc);


			if (loc == 0) {
				Log.v("suc_print0ya","0");
				suc = arrActivePorts[1];
				sucsuc=arrActivePorts[2];
				pre = arrActivePorts[arrActivePorts.length - 1];
			} else if (loc == arrActiveNodes.length - 1) {
				Log.v("suc_print1ya","1");
				suc = arrActivePorts[0];
				sucsuc = arrActivePorts[1];
				pre = arrActivePorts[loc - 1];
			} else if(loc == arrActiveNodes.length - 2){
				Log.v("suc_print2ya","2");
				suc = arrActivePorts[4];
				sucsuc = arrActivePorts[0];
				pre = arrActivePorts[loc - 1];
			}
			else {
				Log.v("suc_print3ya","3");
				suc = arrActivePorts[loc + 1];
				sucsuc = arrActivePorts[loc + 2];
				pre = arrActivePorts[loc - 1];
			}
			Log.v("suc_print2",pre);
			Log.v("suc_print",suc);

			Log.v("suc_print3",sucsuc);

		}catch(NoSuchAlgorithmException e){
			Log.v("exception","suc");
		}
		catch(Exception e){
			Log.v("Exception","on create");
		}

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");

		}catch(Exception e){
			Log.v("Exception", "on create1111");
		}



		String tag="alive";
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, tag);

		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

//		while(flag3){
//			continue;
//		}

		String symbol = selection;
		String[] columnNames= {"key","value"};
		MatrixCursor MC= new MatrixCursor(columnNames);

		if(symbol.equals("@")){
			try{
				Log.v("query2", "query2");
				File file = new File( getContext().getFilesDir()+"/");
				File[] filenames=file.listFiles();
				for(File temp:filenames) {
					Log.v("query3", "query3");
					FileInputStream fis = new FileInputStream(temp);
					BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
					String value = null;
					try {
						value = reader.readLine();
					} catch (IOException e) {
						Log.e("", "IO Exception!!!!!!");
					}
					Log.v("temp", temp.toString().split("/")[5]);

					Log.v("value", value);
					String[] values = {temp.toString().split("/")[5], value};
					MC.addRow(values);

				}
			}catch(Exception e){
				Log.e(TAG, e.getMessage());
			}

			return MC;
		}


		else if(symbol.equals("*")){
			//multicast for all data

			Message msg=new Message();
			msg.tag="*";
			msg.origPort=myPort;
			count=activeNodes.size();
			String[] columns= {"key","value"};
			matcur= new MatrixCursor(columns);
			flag=true;


				String[] remotePort = {"11108","11112","11116","11120","11124"};
				for (String s : remotePort) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(s));
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//
						out.writeObject(msg);

						socket.close();

					} catch (Exception e) {
						Log.e(TAG, "query");
						count--;
					}
				}
			while(flag){
				continue;
			}
			return matcur;
		}



		else{

			Log.v("if not * or @","comes here");
			String filename = selection;
			String send="";
			String sendsuc="";
			String sendsucsuc="";

			Log.v("query",selection);


			try {
				String hashkey = genHash(filename);

				Log.v("checking for","node partition");

				if(hashkey.compareTo(arrActiveNodes[0])<=0 || hashkey.compareTo(arrActiveNodes[arrActiveNodes.length-1])>0){
					send=arrActivePorts[0];
					sendsuc=arrActivePorts[1];
					sendsucsuc=arrActivePorts[2];
				}
				else{
					for(int i=1;i<arrActiveNodes.length;i++){
						if(hashkey.compareTo(arrActiveNodes[i])<=0){
							if(i<=2) {
								send = arrActivePorts[i];
								sendsuc = arrActivePorts[i + 1];
								sendsucsuc = arrActivePorts[i + 2];
								break;
							}else if(i==3){
								send = arrActivePorts[i];
								sendsuc = arrActivePorts[i + 1];
								sendsucsuc = arrActivePorts[0];
								break;
							}else if(i==4){
								send = arrActivePorts[i];
								sendsuc = arrActivePorts[0];
								sendsucsuc = arrActivePorts[1];
								break;
							}
						}
					}
				}


				File file = new File( getContext().getFilesDir()+"/");
				File[] filenames=file.listFiles();

				LinkedList<String> myfiles=new LinkedList<String>();

				for(File temp:filenames){
					myfiles.add(temp.toString().split("/")[5]);
					Log.v("file",temp.toString().split("/")[5]);
				}

				Log.v("linked list ","created");

				if(myfiles.contains(filename)) {
					waitsinglequery.put(selection, 0);

					Log.v("inside","if");

					File thisfile = new File( getContext().getFilesDir() + "/" + filename );

					try {
						FileInputStream fis = new FileInputStream(thisfile);
						BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
						String value = null;
						try {
							value = reader.readLine();
						} catch (IOException e) {
							Log.e("", "IO Exception!!!!!!");
						}
						String[] values = {selection, value};
						MC.addRow(values);

						Log.v("returning","cursor");
						return MC;
					} catch (FileNotFoundException e) {
						Log.e("", "file not found!!!!!!");
					}
				}











//
//				//Log.v("pre",pre);
//				Log.v("myport",myPort);
//				Log.v("send",send);
//				Log.v("wrong","wrong");
//
//
//				if(send.equals(myPort)){// || sendsuc.equals(myPort) || sendsucsuc.equals(myPort)){// || send.equals(pre)){
//					File thisfile = new File( getContext().getFilesDir() + "/" + filename );
//					Log.v("found_at","oye1");
//
//				try {
//					Log.v("found_at","oye2");
//					FileInputStream fis = new FileInputStream(thisfile);
//					Log.v("found_at","oye3");
//					BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
//					Log.v("found_at","oye4");
//					String value = null;
//					try {
//						value = reader.readLine();
//					} catch (IOException e) {
//						Log.e("", "IO Exception!!!!!!");
////					}catch(Exception e){
////						Log.v("found_at","oye5");
//					}
//					Log.v("found_at","oye6");
//					String[] values = {selection, value};
//					MC.addRow(values);
//					Log.v("found_at","oye7");
//					Log.v("returning","cursor");
//					return MC;
//				} catch (FileNotFoundException e) {
//					Log.e("", "file not found!!!!!!");
//				}catch(Exception e){
//					Log.v("found_at","oye8");
//				}
//				}

				else {
					waitsinglequery.put(selection, 1);
					Log.v("want_to_send",selection+" "+send);
					Log.v("found_at","me3");
					Log.v("asking others", "for file");
					Log.v("asking", send);
					Message msg = new Message();
					msg.tag = "single";
					msg.origPort = myPort;
					msg.filename = selection;
					count = activeNodes.size();
//					String[] columns = {"key", "value"};
//					matcur = new MatrixCursor(columns);
					//flag = true;



					try {
						Log.v("found_at","me4");
						int temp = Integer.parseInt(send);
						temp = temp * 2;
						String tempSend = String.valueOf(temp);
						Log.v("found_at", "me5");

						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(tempSend));
						Log.v("found_at", "me6");
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						Log.v("found_at", "me7");
						out.writeObject(msg);
						Log.v("found_at", "me8");
						socket.close();
						Log.v("found_at", "me9");
					}

					catch (Exception e) {
						Log.v("found_at", "me10");
						//Log.e(TAG, e.getMessage());

					}


					try {
						Log.v("found_at","me11");
						int temp = Integer.parseInt(sendsuc);
						temp = temp * 2;
						String tempSend = String.valueOf(temp);
						Log.v("found_at", "me12");

						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(tempSend));
						Log.v("found_at", "me13");
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						Log.v("found_at", "me14");
						msg.tag="single2";
						out.writeObject(msg);
						Log.v("found_at", "me15");
						socket.close();
						Log.v("found_at", "me16");
					}catch(Exception ex){
						Log.v("found at","me17");
					}




					while (waitsinglequery.get(selection)==1) {
						//continue;
					}
					waitsinglequery.remove(selection);
					Log.v("file returned from ", selection);

					//String[] columns = {"key", "value"};
//					matcur = new MatrixCursor(columns);

					MatrixCursor querycursor=allcursors.get(selection);

					allcursors.remove(selection);

					return querycursor;

				}
			}catch(NoSuchAlgorithmException e){
				Log.v("exception","not @ or *");
			}
//

			Log.v("query2", selection);
			return null;
		}

		//return null;
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


	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs){
			if(msgs[0].equals("alive")){
				try {
					String[] senders = {"11108","11112","11116","11120","11124"};
					ArrayList<String> to= new ArrayList<String>(Arrays.asList(senders));
					to.remove(Port);
					for(String s:to) {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(s));


						Message msg = new Message();

						msg.tag = msgs[0];
						msg.origPort = myPort;
						//msg.mulPort = Port;

						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						out.writeObject(msg);

						socket.close();
						Log.v("client each ", "port client");
					}
				}catch(UnknownHostException e){
					Log.e(TAG, e.getMessage());
				}catch(IOException e){
					//Log.e(TAG, e.getMessage());
					Log.e(TAG, "do nothing");
				}catch(Exception e){
					Log.e(TAG, e.getMessage());
				}
			}

			return null;
		}
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		ContentValues cv ;
		ContentResolver cr = getContext().getContentResolver();
		int counter = 0;
		//String KEY_FIELD = "key";
		//String VALUE_FIELD = "value";


		Uri buildUri(String scheme, String authority) {
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.authority(authority);
			uriBuilder.scheme(scheme);
			return uriBuilder.build();
		}

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

			while(true){
				try{

					Log.v("inside","server task");
					Socket clientSocket = serverSocket.accept();
					//lock.lock();
					ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream());
					Message msg = (Message) inputStream.readObject();


					if(msg.tag.equals("alive")){
						Log.v("alive",msg.origPort);
						HashMap<String, String> hmtosend=new HashMap<String, String>();
						if(msg.origPort.equals("5554")){
							hmtosend=backup5554;
						}
						if(msg.origPort.equals("5556")){
							hmtosend=backup5556;
						}
						if(msg.origPort.equals("5558")){
							hmtosend=backup5558;
						}
						if(msg.origPort.equals("5560")){
							hmtosend=backup5560;
						}
						if(msg.origPort.equals("5562")){
							hmtosend=backup5562;
						}

						Message msg2=new Message();

						msg2.tag="backup";
						msg2.backup=hmtosend;
						msg2.mulPort=myPort;

						int temp = Integer.parseInt(msg.origPort);
						temp = temp * 2;
						String tempSend = String.valueOf(temp);

						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(tempSend));
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						out.writeObject(msg2);
						socket.close();

					}

					if(msg.tag.equals("backup")){
						for(String s: msg.backup.keySet()){

							String key=s;
							Log.v("backupkey",key);
							Log.v("backupkeyorg",msg.mulPort);
							String value=msg.backup.get(key);
							FileOutputStream outputStream;
							outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
							outputStream.write(value.getBytes());
							outputStream.close();

							String hashkey=genHash(key);
							String send="";
							String sendsuc="";
							String sendsucsuc= "";

							if(hashkey.compareTo(arrActiveNodes[0])<=0 || hashkey.compareTo(arrActiveNodes[arrActiveNodes.length-1])>0){
								send=arrActivePorts[0];
								sendsuc=arrActivePorts[1];
								sendsucsuc=arrActivePorts[2];
							}
							else{
								for(int i=1;i<arrActiveNodes.length;i++){
									if(hashkey.compareTo(arrActiveNodes[i])<=0){
										if(i<=2) {
											send = arrActivePorts[i];
											sendsuc = arrActivePorts[i + 1];
											sendsucsuc = arrActivePorts[i + 2];
											break;
										}else if(i==3){
											send = arrActivePorts[i];
											sendsuc = arrActivePorts[i + 1];
											sendsucsuc = arrActivePorts[0];
											break;
										}else if(i==4){
											send = arrActivePorts[i];
											sendsuc = arrActivePorts[0];
											sendsucsuc = arrActivePorts[1];
											break;
										}
									}
								}
							}
							Log.v("patatochale1",key);
							Log.v("patatochale2",send);
							Log.v("patatochale3",sendsuc);
							Log.v("patatochale4", sendsucsuc);
							create_backup(send, sendsuc, sendsucsuc, key, value);
							//flag3=false;
						}
					}

					if(msg.tag.equals("insert")){

						Log.v("inside", "insert");
						FileOutputStream outputStream;

						outputStream = getContext().openFileOutput(msg.key, Context.MODE_PRIVATE);
						outputStream.write(msg.value.getBytes());
						outputStream.close();


						String hashkey=genHash(msg.key);
						String send="";
						String sendsuc="";
						String sendsucsuc="";


						if(hashkey.compareTo(arrActiveNodes[0])<=0 || hashkey.compareTo(arrActiveNodes[arrActiveNodes.length-1])>0){
							send=arrActivePorts[0];
							sendsuc=arrActivePorts[1];
							sendsucsuc=arrActivePorts[2];
						}
						else{
							for(int i=1;i<arrActiveNodes.length;i++){
								if(hashkey.compareTo(arrActiveNodes[i])<=0){
									if(i<=2) {
										send = arrActivePorts[i];
										sendsuc = arrActivePorts[i + 1];
										sendsucsuc = arrActivePorts[i + 2];
										break;
									}else if(i==3){
										send = arrActivePorts[i];
										sendsuc = arrActivePorts[i + 1];
										sendsucsuc = arrActivePorts[0];
										break;
									}else if(i==4){
										send = arrActivePorts[i];
										sendsuc = arrActivePorts[0];
										sendsucsuc = arrActivePorts[1];
										break;
									}
								}
							}
						}



						Log.v("keylog1", msg.key);
						Log.v("keylog-1", myPort);
						Log.v("keylog-2", suc);
						Log.v("keylog-3", sucsuc);
						//create_backup(myPort, suc, sucsuc, msg.key, msg.value);
						Log.v("keylog-4", "return");


						Log.v("wrote", "in memory");
						//Log.v("now sending to", suc);

						create_backup(send, sendsuc, sendsucsuc, msg.key, msg.value);

//						String temp_suc="";
//
//						int temp= Integer.parseInt(suc);
//						temp=temp*2;
//						temp_suc=Integer.toString(temp);
//
//						Message msg2=new Message();
//						msg2.key=msg.key;
//
//						msg2.value=msg.value;
//						msg2.tag="insert2";
//						try {
//							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//									Integer.parseInt(temp_suc));
//
//							ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//							out.writeObject(msg2);
//
//							socket.close();
//							Log.v("insert sent to  ", temp_suc);
//							Log.v("insert_sent to",msg.key);
//						}catch(Exception ex){
//							int tempsuc= Integer.parseInt(sucsuc);
//							tempsuc=tempsuc*2;
//							temp_suc=Integer.toString(tempsuc);
//							Message msg3=new Message();
//							msg3.key=msg.key;
//							msg3.value=msg.value;
//							Log.v("insert_Exc_key",msg.key);
//							msg3.tag="insert3";
//							try {
//								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//										Integer.parseInt(temp_suc));
//
//								ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//								out.writeObject(msg3);
//
//								socket.close();
//								Log.v("insert sent to Exc ", temp_suc);
//								Log.v("insert sent to Exc2 ", msg3.key);
//							}catch(Exception exx){
//								Log.v("insert","exception");
//							}
//						}

//						cv=new ContentValues();
//
////						Set<String> keySet=msg.values.keySet();
////
////						Object[] keys=keySet.toArray();
////
////						String k=keys[0].toString();
////
////						String v=msg.values.get(keys[0].toString());
//
//						cv.put("key", msg.key);
//						cv.put("value", msg.value);
//
//
//						cr.insert(mUri, cv);
					}
					if(msg.tag.equals("insert2")) {

						Log.v("inside","insert2");

						FileOutputStream outputStream;
						outputStream = getContext().openFileOutput(msg.key, Context.MODE_PRIVATE);
						outputStream.write(msg.value.getBytes());
						outputStream.close();
						Log.v("keylog2",msg.key );
						String send="";
						String sendsuc="";
						String sendsucsuc="";
						String hashkey=genHash(msg.key);

						if(hashkey.compareTo(arrActiveNodes[0])<=0 || hashkey.compareTo(arrActiveNodes[arrActiveNodes.length-1])>0){
							send=arrActivePorts[0];
							sendsuc=arrActivePorts[1];
							sendsucsuc=arrActivePorts[2];
						}
						else{
							for(int i=1;i<arrActiveNodes.length;i++){
								if(hashkey.compareTo(arrActiveNodes[i])<=0){
									if(i<=2) {
										send = arrActivePorts[i];
										sendsuc = arrActivePorts[i + 1];
										sendsucsuc = arrActivePorts[i + 2];
										break;
									}else if(i==3){
										send = arrActivePorts[i];
										sendsuc = arrActivePorts[i + 1];
										sendsucsuc = arrActivePorts[0];
										break;
									}else if(i==4){
										send = arrActivePorts[i];
										sendsuc = arrActivePorts[0];
										sendsucsuc = arrActivePorts[1];
										break;
									}
								}
							}
						}
						create_backup(send,sendsuc,sendsucsuc,msg.key,msg.value);

						Log.v("wrote", "in memory");
						Log.v("now sending to", suc);

						String temp_suc="";

						int temp = Integer.parseInt(suc);
						temp = temp * 2;
						temp_suc = Integer.toString(temp);

						Message msg2 = new Message();
						msg2.key = msg.key;
						msg2.value = msg.value;
						msg2.tag = "insert3";

						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(temp_suc));

						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						out.writeObject(msg2);

						socket.close();
						Log.v("insert sent to  ", temp_suc);
					}
					if(msg.tag.equals("insert3")) {

						Log.v("inside","insert3");
						FileOutputStream outputStream;
						outputStream = getContext().openFileOutput(msg.key, Context.MODE_PRIVATE);
						outputStream.write(msg.value.getBytes());
						outputStream.close();
						Log.v("keylog3", msg.key);
						String send="";
						String sendsuc="";
						String sendsucsuc="";
						String hashkey=genHash(msg.key);

						if(hashkey.compareTo(arrActiveNodes[0])<=0 || hashkey.compareTo(arrActiveNodes[arrActiveNodes.length-1])>0){
							send=arrActivePorts[0];
							sendsuc=arrActivePorts[1];
							sendsucsuc=arrActivePorts[2];
						}
						else{
							for(int i=1;i<arrActiveNodes.length;i++){
								if(hashkey.compareTo(arrActiveNodes[i])<=0){
									if(i<=2) {
										send = arrActivePorts[i];
										sendsuc = arrActivePorts[i + 1];
										sendsucsuc = arrActivePorts[i + 2];
										break;
									}else if(i==3){
										send = arrActivePorts[i];
										sendsuc = arrActivePorts[i + 1];
										sendsucsuc = arrActivePorts[0];
										break;
									}else if(i==4){
										send = arrActivePorts[i];
										sendsuc = arrActivePorts[0];
										sendsucsuc = arrActivePorts[1];
										break;
									}
								}
							}
						}
						create_backup(send, sendsuc, sendsucsuc, msg.key, msg.value);

						Log.v("insert","final");
					}

					else if(msg.tag.equals("*")){
						Log.v("entering","* query");
						Cursor cursor=cr.query(mUri,null,"@",null,null);

						String k;
						String v;
						while(cursor.moveToNext()) {
							int x = cursor.getColumnIndex("key");
							int y = cursor.getColumnIndex("value");
							k = cursor.getString(x);
							v = cursor.getString(y);
							Log.v("cursor values", k + " " + v);

							msg.values.put(k, v);

						}
						cursor.close();

						int temp = Integer.parseInt(msg.origPort);
						temp = temp * 2;
						String tempSuc = String.valueOf(temp);
						Log.v("tempSuc", tempSuc);

						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(tempSuc));
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

						msg.tag="query";

						out.writeObject(msg);

						socket.close();
					}
					else if(msg.tag.equals("query")){
						Log.v("values return","from others");
						count--;

						Set<String> keySet=msg.values.keySet();

						Object[] keys=keySet.toArray();

						for(Object obkey:keys){
							String key=obkey.toString();
							String value=msg.values.get(key);
							String[] values={key,value};
							matcur.addRow(values);
						}

						if(count==0) {
							flag = false;
						}
					}

					else if(msg.tag.equals("single")) {
						Log.v("entering", "single query");
						String filename = msg.filename;

						Log.v("incoming query", filename);


						Log.v("if", "found");

						msg.tag = "found";


						Cursor cursor = cr.query(mUri, null, filename, null, null);
						Log.v("yo1","yo1");
						String k;
						String v;
						if (cursor.moveToFirst()) {
							int x = cursor.getColumnIndex("key");
							int y = cursor.getColumnIndex("value");
							k = cursor.getString(x);
							v = cursor.getString(y);

						} else {
							k = "empty";
							v = "empty";
						}
						cursor.close();

						msg.key=k;
						Log.v("cursor values", k + " " + v);

						msg.values.put(k, v);


						int temp = Integer.parseInt(msg.origPort);
						temp = temp * 2;
						String tempSuc = String.valueOf(temp);
						Log.v("tempSuc", tempSuc);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(tempSuc));
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//
						out.writeObject(msg);

						socket.close();

					}else if(msg.tag.equals("single2"))
					{
						Log.v("entering2", "single query");
						String filename = msg.filename;

						Log.v("incoming query", filename);


						Log.v("if", "found");

						msg.tag = "found";


						//Cursor cursor = cr.query(mUri, null, filename, null, null);
						Log.v("yo1","yo1");

						File thisfile = new File( getContext().getFilesDir() + "/" + filename );
						Log.v("found_at","oye1");


							Log.v("found_at","oye2");
							FileInputStream fis = new FileInputStream(thisfile);
							Log.v("found_at","oye3");
							BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
							Log.v("found_at","oye4");
							String value = null;
							try {
								value = reader.readLine();
							} catch (IOException e) {
								Log.e("", "IO Exception!!!!!!");
//					}catch(Exception e){
//						Log.v("found_at","oye5");
							}


						msg.values.put(filename, value);


						int temp = Integer.parseInt(msg.origPort);
						temp = temp * 2;
						String tempSuc = String.valueOf(temp);
						Log.v("tempSuc", tempSuc);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(tempSuc));
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//
						out.writeObject(msg);

						socket.close();

					}

					else if(msg.tag.equals("found")){
						Log.v("found","the key");

						Set<String> keySet=msg.values.keySet();

						Object[] keys=keySet.toArray();

						String k=keys[0].toString();

						String v=msg.values.get(keys[0].toString());

						String[] values={k,v};
						String[] columns= {"key","value"};
						MatrixCursor local=new MatrixCursor(columns);
						Log.v("adding to cursor",k);
						Log.v("adding to cursor",v);
						local.addRow(values);
						allcursors.put(k,local);
						//flag=false;
						waitsinglequery.put(k,0);
					}

					if(msg.tag.equals("delete")){
						backup5554=new HashMap<String, String>();
						backup5556=new HashMap<String, String>();
						backup5558=new HashMap<String, String>();
						backup5560=new HashMap<String, String>();
						backup5562=new HashMap<String, String>();
					}

				}catch(Exception e){
					Log.v("Exception", "server task" );
				}
			}

			//return null;
		}
	}


}
