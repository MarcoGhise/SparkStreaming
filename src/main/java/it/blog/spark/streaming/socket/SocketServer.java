package it.blog.spark.streaming.socket;

import java.net.*;
import java.util.Random;
import java.io.*;

public class SocketServer {

	private ServerSocket serverSocket;
	private Socket clientSocket;
	private PrintWriter out;

	private static final double left = 45.798679;
	private static final double top = 8.418881;
	
	private static final double bottom = 10.060654;
	private static final double right = 45.142256;
	
	private static int generator(int min, int max)
	{
		return new Random().nextInt(max + 1)  + min;
	}
	
	public void start(int port) throws IOException, InterruptedException {

		serverSocket = new ServerSocket(port);
		clientSocket = serverSocket.accept();
		out = new PrintWriter(clientSocket.getOutputStream(), true);
		
		int carNumber=0;
		String position = "";
		
		while(1!=0)
		{
			carNumber = new Random().nextInt(10+1);
			//carNumber|long|lat
			
			position = carNumber + "|45." + generator(142256, 798679) + "|" + generator(8,10) + "." + generator(060654, 418881);
			System.out.println(position);
			out.println(position);
			
			Thread.sleep(generator(100,2000));
		}
	}

	public void stop() throws IOException {

		out.close();
		clientSocket.close();
		serverSocket.close();

	}

	public static void main(String[] args) throws IOException, InterruptedException {
		SocketServer server = new SocketServer();
		server.start(6789);
	}

}
