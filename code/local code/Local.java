package pa3_local;

public class Local implements Runnable{
	
	//Implementing the run method of the Runnable class
	public void run(){
		try {
			//Calling the Run_Worker
			Run_Worker();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	//The worker pulls the jobs from the queue and executes the sleep jobs
	public static void Run_Worker() throws InterruptedException{
		String line = null;
		String[] parts = null;
		for (line = (String)Driver.queue.poll();line != null ; line = (String)Driver.queue.poll()){
				
				parts = line.split(" ");
				try{
				//Sleep jobs run
				Thread.sleep(Integer.parseInt(parts[1]));
			    //The executed sleep jobs are put in the resqueue
				Driver.resqueue.add(parts[1]);
				}
				catch (NumberFormatException ex){
					System.out.println("Could not parse the String parts[]");
				}
		}
	}

}
