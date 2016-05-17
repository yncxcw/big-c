package org.apahce.hadoop.mapreduce.v2.app.ganglia;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.Socket;

import java.net.UnknownHostException;


public class RemoteReader {

	private String url;
	
	private int port;

	public RemoteReader(int port, String url) {

		this.url = url;

		this.port = port;

	}

	public RemoteReader(String url) {

		this.url = url;

		this.port = 8651;
	}

	public RemoteReader() {

		this.url = "127.0.0.1";

		this.port = 8651;

	}
	

	public void setUrl(String url){
		
		this.url = url;
		
	}
	
	public void setPort(int port){
		
		this.port = port;
	}
	
	public String getUrl() {
		return url;
	}

	public int getPort() {
		return port;
	}


	public String SendRequest() {

		StringBuffer stringBuffer = new StringBuffer();
		
		Socket client = null;
		Reader reader = null;

		try {
			
			 client = new Socket(this.url,this.port);
			
			 reader = new InputStreamReader(client.getInputStream());
			
			 char chars[]= new char[8192];
			
			 int len;
			
			 while((len=reader.read(chars))!= -1){
				
				stringBuffer.append(new String(chars,0,len));
			 }
						
		} catch (Exception e) {

			System.out.println("execetion found during send request" + e);
			e.printStackTrace();

		} finally {
            
			try {
				client.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

		return stringBuffer.toString();

	}

	

	public static void main(String[] args) throws UnknownHostException, IOException {

		RemoteReader httpAccess = new RemoteReader();

        System.out.println(httpAccess.SendRequest());

	}

}
