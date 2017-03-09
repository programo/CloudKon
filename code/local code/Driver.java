package pa3_local;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Driver {
	public static Queue<String> queue;
	public static Queue<String> resqueue;
	
	
	//The client submits the jobs to the queue
	public static void Run_Client(String file){
		String filename = file;
		String line = null;
		try{
			//Reading the tasks from the file
			FileReader fileReader = new FileReader(filename);
			BufferedReader bufferReader = new BufferedReader(fileReader);
			while ((line = bufferReader.readLine()) != null ){
				 queue.add(line);
				}
		}
		catch(FileNotFoundException ex){
			System.out.println("Could not find the file"+filename);
		}
		catch(IOException ex){
			System.out.println("Cound not read the file"+filename);
		}
	}
	
	public static void main(String[] args) {
		//Creating queues
		queue = new ConcurrentLinkedQueue<String>();
		resqueue = new ConcurrentLinkedQueue<String>();
		
		//Calling the Run Client 
	    Driver.Run_Client(args[6]);
	    //Timer started and the threads are created and run method of the Local class is called
	    long starttime = System.currentTimeMillis();
	    ExecutorService executor = Executors.newFixedThreadPool(Integer.parseInt(args[4]));
	    int i=Integer.parseInt(args[4]);
	    while (i>0){
	    	Runnable worker = new Local();
	    	executor.execute(worker);
	    	i--;
	    	}
	    executor.shutdown();

	    while (!executor.isTerminated()){
	    	
	    }
	    //Timer stopped
	    long endtime = System.currentTimeMillis();
	    //Elapsed time calculated
	    long time_elapsed = endtime - starttime;
	    System.out.println("Time elapsed in ms to run on "+args[4]+" threads : " + time_elapsed);
	    
	}

}
